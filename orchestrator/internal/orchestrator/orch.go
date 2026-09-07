package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/JoYBoy7214/swarm-orchestrator/internal/jet_stream"
	"github.com/JoYBoy7214/swarm-orchestrator/internal/storage/postgresDb"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Orchestrator struct {
	DbDriver      *postgresDb.PostgresDriver
	StreamHandler *jet_stream.JetStreamHandler
}

type tempschema struct {
	WorkFlow_id uuid.UUID `json:"Workflow_id"`
	Task_id     uuid.UUID `json:"Task_id"`
	Task_type   string    `json:"Task_type"`
}

type MaxDeliverAdvisory struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Timestamp  string `json:"timestamp"`
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries int    `json:"deliveries"`
}

func CreateOrchestrator(ctx context.Context, DbString string, natsUrl string) (*Orchestrator, error) {
	DbDriver, err := postgresDb.CreatePostgresDriver(ctx, DbString)
	if err != nil {
		return nil, fmt.Errorf("Error in creating Db Driver, Error: %w", err)
	}
	StreamHandler, err := jet_stream.CreateJetStreamHandler(ctx, natsUrl)
	if err != nil {
		return nil, fmt.Errorf("Error in creating Stream hnadler, Error: %w", err)
	}
	return &Orchestrator{
		DbDriver:      DbDriver,
		StreamHandler: StreamHandler,
	}, nil
}

func (orch *Orchestrator) StartOrchestrating(ctx context.Context) {
	var wg sync.WaitGroup
	consumer, err := orch.StreamHandler.Jstream.CreateOrUpdateConsumer(ctx, "TASKS", jetstream.ConsumerConfig{
		Durable:       "ORCH_CONS",
		FilterSubject: "task.COMPLETED",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatalf("Error in creating complete update consumer %w", err)
	}

	if err != nil {
		log.Fatalf("Error in creating fail check consumer %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		orch.pullConsumer(ctx, consumer)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		orch.failCheckPullConsumer(ctx)
	}()
	<-ctx.Done()
	wg.Wait()

}
func (orch *Orchestrator) pullConsumer(ctx context.Context, consumer jetstream.Consumer) {
	//we will evalute the DAG in batch so we can reduce the DB Calls why this works(according to me) let say we have
	// a graph of 500 nodes evaluating every time a task completed will inefficient and number of the ready task also will be less
	//so evaluting after 100 task completed or 1 sec could lead to more ready tasks
	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			log.Println("waiting for goroutines to end")
			wg.Wait()
			log.Println("goroutines ended")
			return
		default:
			//fmt.Println("I am running 1")
			bulk, err := consumer.Fetch(100, jetstream.FetchMaxWait(1*time.Second))
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) { // in case the queue is not full by one second
					log.Println("Deadline reached before filling the queue %w", err)
				} else {
					log.Fatal("Error in fetching message from stream %w", err)
					return
				}
			}
			for m := range bulk.Messages() {

				wg.Add(1)
				go func(m jetstream.Msg) {
					//don't tie root context to goroutines since if the root context dies the all child ctx also dies
					//so here DB calls and publisher will throw context canceled error even we provide time to shutdown gracfully
					processContext, processcontextCancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer processcontextCancel()
					defer wg.Done()
					var msg tempschema
					err := json.Unmarshal(m.Data(), &msg)
					if err != nil {
						log.Println("Error in converting jetstream.Msg to tempschema %w", err)
						return
					}
					workflow_id := msg.WorkFlow_id //can be further optimized by evaluating one workflow only one time
					task_id := msg.Task_id
					err = orch.DbDriver.UpdateTask(processContext, task_id, workflow_id, "COMPLETED")
					if err != nil {
						log.Println("Error in updating task_status %w:", err)
						return
					}

					ready_task, err := orch.DbDriver.Evaluate(processContext, workflow_id)
					if err != nil {
						log.Println("Error in Evaluating DAG %w", err)
						return
					}

					err = orch.testTempLogger(processContext, workflow_id)
					if err != nil {
						log.Println("Error in logging the states %w", err)
					}

					err = orch.bulkPublisher(processContext, ready_task, workflow_id, msg.Task_type)
					if err != nil {
						log.Println("Error in publishing the tasks %w", err)
						return
					} else {
						err = m.Ack()
						if err != nil {
							log.Println("Error in ack the message %w", err)
							return
						}
					}
					//time.Sleep(10 * time.Second)
				}(m)

			}
			//wg.Wait()
		}

	}

}

func (orch *Orchestrator) failCheckPullConsumer(ctx context.Context) {
	//to handle the failed tasks
	advisorySubject := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"

	// 1. Worker queue to avoid unbounded goroutine creation
	const maxWorkers = 10
	taskCh := make(chan MaxDeliverAdvisory, 1000)
	var workerWg sync.WaitGroup

	// Start bounded workers
	for i := 0; i < maxWorkers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for adv := range taskCh {
				orch.processMaxDeliveryAdvisory(adv)
			}
		}()
	}

	// 2. Subscribe to advisories
	sub, err := orch.StreamHandler.Nats.Subscribe(advisorySubject, func(msg *nats.Msg) {
		var adv MaxDeliverAdvisory
		if err := json.Unmarshal(msg.Data, &adv); err != nil {
			log.Printf("Failed to unmarshal advisory: %v", err)
			return
		}

		// Push to worker queue (non-blocking fallback or block until queue clears)
		select {
		case taskCh <- adv:
		default:
			log.Printf("[Warning] Advisory queue full. Backpressure dropping/waiting on seq: %d", adv.StreamSeq)
			taskCh <- adv // Or block if dropping is unacceptable
		}
	})
	if err != nil {
		log.Printf("Error in creating fail check nats subscriber: %v", err)
		close(taskCh)
		workerWg.Wait()
		return
	}

	// 3. Block until context cancellation
	<-ctx.Done()
	log.Println("Shutting down fail-check advisory listener...")

	// 4. Drain the subscription first: stops incoming messages and processes pending
	if err := sub.Drain(); err != nil {
		log.Printf("Error draining advisory subscription: %v", err)
	}

	// 5. Safely close task channel and wait for workers to exit cleanly
	close(taskCh)
	workerWg.Wait()
	log.Println("All fail-check worker tasks finished.")
}

func (orch *Orchestrator) processMaxDeliveryAdvisory(adv MaxDeliverAdvisory) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := orch.StreamHandler.Jstream.Stream(ctx, adv.Stream)
	if err != nil {
		log.Printf("Failed to get stream %s: %v", adv.Stream, err)
		return
	}

	rawMsg, err := stream.GetMsg(ctx, adv.StreamSeq)
	if err != nil {
		log.Printf("Failed to get message seq %d: %v", adv.StreamSeq, err)
		return
	}

	var temp tempschema
	if err := json.Unmarshal(rawMsg.Data, &temp); err != nil {
		log.Printf("Failed to unmarshal raw data: %v", err)
		return
	}
	log.Printf("Failed to do the task: %s", temp.Task_type)
	if err := orch.DbDriver.UpdateTask(ctx, temp.Task_id, temp.WorkFlow_id, "FAILED"); err != nil {
		log.Printf("Failed to update task status: %v", err)
		return
	}

	if err := orch.DbDriver.UpdateWorkflowStatus(ctx, temp.WorkFlow_id, "FAILED"); err != nil {
		log.Printf("Failed to update workflow status: %v", err)
		return
	}
}

func (orch *Orchestrator) bulkPublisher(ctx context.Context, tasks []uuid.UUID, workflow_id uuid.UUID, task_type string) error {
	for _, task := range tasks {
		msg := tempschema{
			WorkFlow_id: workflow_id,
			Task_id:     task,
			Task_type:   task_type,
		}
		msgByte, err := json.Marshal(msg)
		err = orch.StreamHandler.Publish(ctx, msgByte, "task.EXECUTE")
		if err != nil {
			return err
		}
	}
	return nil
}

func (orch *Orchestrator) RootNodeUpdater(ctx context.Context, workflow_id uuid.UUID) error {
	//This fucntion is to update the status of the root nodes of DAG since func StartOrchestrating() only relies on task.Completed it won't able to identify and
	//trigger the Root ndoes so every time when we create a DAG we have to run this func

	//Important point to note this can cause single point of failure since if the evaulate of publishing fails parent nodes have no way to start so a background worker
	// is needed to clean up this kind of things
	driverContext, driverContextCancel := context.WithTimeout(ctx, time.Second)
	defer driverContextCancel()
	ready_task, err := orch.DbDriver.Evaluate(driverContext, workflow_id)
	if err != nil {
		return fmt.Errorf("Error in Evaluating Root DAG error: %w", err)
	}

	err = orch.testTempLogger(ctx, workflow_id)
	if err != nil {
		log.Println("Error in logging the states %w", err)
	}

	publisherContext, publisherContextCancel := context.WithTimeout(ctx, time.Second)
	defer publisherContextCancel()
	err = orch.bulkPublisher(publisherContext, ready_task, workflow_id, "ParentNode") //need to change this part
	if err != nil {
		return fmt.Errorf("Error in publishing the tasks %w", err)

	}

	return nil

}

func (orch *Orchestrator) BackgroundSweeper(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Println("Background Sweeper Running")
			processContext, processcontextCancel := context.WithTimeout(context.Background(), 1*time.Second)
			result, err := orch.DbDriver.GetAllReadyLongLivedTasks(processContext)
			processcontextCancel()
			log.Printf("Background Sweeper Running %d", len(result))
			if err != nil {
				log.Println("Error in background sweeper while getting the long lived task, ERROR: %w", err)
				continue
			}
			for _, r := range result {
				msgByte, err := json.Marshal(r)
				err = orch.StreamHandler.Publish(ctx, msgByte, "task.EXECUTE")
				if err != nil {
					log.Println("Error in background sweeper while publishing, ERROR: %w", err)
				}
			}

		case <-ctx.Done():
			return
		}

	}
}

func (orch *Orchestrator) testTempLogger(ctx context.Context, workflowID uuid.UUID) error {
	tasksInfo, err := orch.DbDriver.TesttempGettingInfo(ctx, workflowID)
	if err != nil {
		log.Println("iteration: failed to get task info: %v", err)
		return err
	}

	for _, info := range tasksInfo {
		log.Printf("Iteration - Type: %s, Status: %s", info.Task_type, info.Task_status)
	}

	log.Printf("end of iteration")
	return nil
}
