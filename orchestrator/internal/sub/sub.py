import asyncio
import signal
import json
import logging
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError
import aiohttp
from nats.js.api import ConsumerConfig, AckPolicy
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def worker_task(worker_id, js, stop_event):
    try:
        sub = await js.pull_subscribe("task.EXECUTE", durable="concurrent_worker_group",stream="TASKS")
    except Exception as e:
        logger.error(f"Worker {worker_id} - Error creating consumer: {e}")
        return

    logger.info(f"Worker {worker_id} started.")

    # Reuse one session for the worker's lifetime instead of creating one per request
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            try:
                msgs = await sub.fetch(1, timeout=0.5)

                for msg in msgs:
                    try:
                        if random.random() < 0.3:
                            print("Chaos Monkey: Simulating sudden worker crash!")
                            # Do NOT ack. Just drop the message and continue the loop.
                            continue
                        d = json.loads(msg.data)
                        workflow_id = d.get("Workflow_id")
                        task_id = d.get("Task_id")
                        task_type = d.get("Task_type")
                        # --- HTTP idempotency check before doing the work ---

                        payload = {
                            "Workflow_id": workflow_id,
                            "Task_id": task_id,
                            "Task_type": task_type,
                        }

                        proceed = False
                        try:
                            async with session.request(
                                "PATCH",
                                f"http://localhost:8080/api/v1/tasks/{task_id}",
                                json=payload,
                                timeout=aiohttp.ClientTimeout(total=2),
                            ) as resp:
                                if resp.status == 200:
                                    proceed = True
                                else:
                                    body = await resp.text()
                                    logger.warning(
                                        f"Worker {worker_id} - Task {task_id} not eligible "
                                        f"(status {resp.status}): {body}"
                                    )
                        except asyncio.TimeoutError:
                            logger.error(f"Worker {worker_id} - HTTP timeout checking Task {task_id}")
                        except aiohttp.ClientError as e:
                            logger.error(f"Worker {worker_id} - HTTP error checking Task {task_id}: {e}")

                        if not proceed:
                            # Idempotency check failed / task already running / HTTP error
                            # -> don't process it, just ack so it's not redelivered
                            await msg.ack()
                            continue

                        
                        # --- Simulate task execution ---
                        rand_int = random.randint(2, 5)
                        logger.info(f"Worker {worker_id} processing Task: {task_id} (Type: {task_type}) (excution time: {rand_int})")
                        await asyncio.sleep(rand_int)

                        # Prepare completed payload
                        completed_payload = {
                            "Workflow_id": workflow_id,
                            "Task_id": task_id,
                            "Task_type": task_type,
                        }

                        await js.publish(
                            "task.COMPLETED",
                            json.dumps(completed_payload).encode("utf-8")
                        )

                        await msg.ack()
                        logger.info(f"Worker {worker_id} completed and acked Task: {task_id}")

                    except json.JSONDecodeError:
                        logger.error(f"Worker {worker_id} - Invalid JSON. Terminating message.")
                        await msg.term()

                    except Exception as e:
                        logger.error(f"Worker {worker_id} - Processing failed: {e}")
                        await msg.nak()

            except TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not stop_event.is_set():
                    logger.error(f"Worker {worker_id} unexpected error: {e}")
                    await asyncio.sleep(1)

    logger.info(f"Worker {worker_id} shutting down.")


async def main():
    nc = NATS()
    try:
        await nc.connect("localhost:4222", connect_timeout=30)
    except Exception as e:
        logger.error(f"Error connecting to NATS: {e}")
        return

    js = nc.jetstream()
    await js.add_consumer(
    "TASKS", # The stream that holds "task.EXECUTE"
    config=ConsumerConfig(
        durable_name="concurrent_worker_group",
        ack_policy=AckPolicy.EXPLICIT,
        max_deliver=5,                     # Limit retries
        ack_wait=10.0,                     # Give workers 10s to ack
        filter_subject="task.EXECUTE"      # Only consume these subjects
    )
)
    stop_event = asyncio.Event()

    def signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass 

    # --- Start Concurrent Workers ---
    NUM_WORKERS = 5
    workers = []
    
    for i in range(NUM_WORKERS):
        task = asyncio.create_task(worker_task(i + 1, js, stop_event))  #this will schedule corountines in the background but it won't run it 
        workers.append(task)
   
    try:
        while not stop_event.is_set():
            await asyncio.sleep(0.5)  #this juggle between main func and corountines 
    except KeyboardInterrupt:
        stop_event.set()

    # --- Graceful Shutdown ---
    logger.info("Initiating graceful shutdown...")
    
    # Wait for all workers to finish their current loop iteration
    await asyncio.gather(*workers, return_exceptions=True)
    await nc.close()
    logger.info("Shutdown complete.")

if __name__ == '__main__':
    asyncio.run(main())