import asyncio
import aiohttp
import time
import logging
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

URL = "http://localhost:8080/api/v1/tasks"
CANCEL_URL = "http://localhost:8080/api/v1/tasks/{}"
TOTAL_REQUESTS = 1000
CONCURRENCY_LIMIT = 100 # How many requests to send at the exact same millisecond

active_workflows = []

async def fire_request(session, req_id):
    try:
        async with session.post(URL, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status in [200, 201, 202]:
                data = await response.json()
                active_workflows.append(data["workflow_id"])
                return True
            else:
                logger.warning(f"Request {req_id} failed with status: {response.status}")
                return False
    except Exception as e:
        logger.error(f"Request {req_id} exception: {e}")
        return False

async def cancel_monkey(session, queue):
    """Randomly cancels workflows mid-flight to test the broadcast kill switch."""
    logger.info("Cancel Monkey started...")
    cancelled_count = 0
    while True:
        # Wait a bit before starting the chaos
        
        
        # If the queue is empty, the test is wrapping up
        
        await asyncio.sleep(random.uniform(0.5, 2.0))
        logger.info(f"Cancel Monkey checking queue size: {queue.qsize()}")
        if queue.empty():
            logger.info("Cancel Monkey stopping as the queue is empty.")
            break
            
        if active_workflows:
            # Pick a random workflow to assassinate
            target_id = random.choice(active_workflows)
            try:
                queue.get_nowait()  # Decrement the cancel queue
                logger.info(f"Cancel Monkey firing DELETE for {target_id}")
                async with session.delete(CANCEL_URL.format(target_id), timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        cancelled_count += 1  # Decrement the cancel queue
                        
                        active_workflows.remove(target_id)
                        logger.info(f"Cancel Monkey finished. Successfully sent {cancelled_count} kill signals.")
                        
            except Exception:
                logger.error(f"Cancel Monkey failed to send DELETE for {target_id}")
                pass
    return 0    
                
    
async def worker(queue, session):
    success_count = 0
    while True:
        req_id = await queue.get()
        if req_id is None:
            break
        success = await fire_request(session, req_id)
        if success:
            success_count += 1
        queue.task_done()
    return success_count

async def main():
    logger.info(f"Starting Load Test: Firing {TOTAL_REQUESTS} workflows into the Orchestrator...")
    start_time = time.time()
    
    queue = asyncio.Queue()
    for i in range(TOTAL_REQUESTS):
        queue.put_nowait(i)
    
    cancel_queue=asyncio.Queue()
    for i in range(TOTAL_REQUESTS // 10):
        cancel_queue.put_nowait(1)
        
    # We use a custom TCP connector to allow high concurrency
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Spin up bounded workers to blast the API concurrently
        tasks = []
        for _ in range(CONCURRENCY_LIMIT):
            task = asyncio.create_task(worker(queue, session))
            tasks.append(task)
            await asyncio.sleep(0.1)

        await asyncio.sleep(8)     # Slight delay to let some requests start before chaos begins
        # Start the Chaos Cancel Monkey
        for _ in range(CONCURRENCY_LIMIT // 10):  # Start a few cancel monkeys
            monkey_task = asyncio.create_task(cancel_monkey(session, cancel_queue))
            tasks.append(monkey_task)
            
        # Add stop signals
        for _ in range(CONCURRENCY_LIMIT):
            queue.put_nowait(None)
            
        # Wait for all requests to finish
        results = await asyncio.gather(*tasks)
        
    total_success = sum(results)
    duration = time.time() - start_time
    
    logger.info("=========================================")
    logger.info(f"Load Test Complete in {duration:.2f} seconds")
    logger.info(f"Workflows Created: {total_success} / {TOTAL_REQUESTS}")
    logger.info(f"Throughput: {TOTAL_REQUESTS / duration:.2f} workflows/second")
    logger.info(f"Total Tasks Generated: {total_success * 10} (Assuming 10 nodes per DAG)")
    logger.info("=========================================")
    logger.info("Now monitor your Go Orchestrator logs and NATS pending message count!")
    logger.info("Run `nats stream info TASKS` to see the backlog.")

if __name__ == "__main__":
    asyncio.run(main())