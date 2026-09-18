package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/JoYBoy7214/swarm-orchestrator/internal/orchestrator"
	"github.com/JoYBoy7214/swarm-orchestrator/internal/storage"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type workflowResponse struct {
	WorkflowID uuid.UUID `json:"workflow_id"`
}

func deleteWorkflowHander(w http.ResponseWriter, r *http.Request, orch *orchestrator.Orchestrator) {
	w.Header().Set("content-type", "application/json")
	workflow_id := r.URL.Path[len("/api/v1/tasks/"):]
	if workflow_id == "" {
		log.Println("Error in deleting workflow, workflow_id is empty")
		http.Error(w, "Error in deleting workflow, workflow_id is empty", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	uuidWorkflowId, err := uuid.Parse(workflow_id)
	if err != nil {
		log.Println("Error in parsing workflow_id %w", err)
		http.Error(w, "Error in parsing workflow_id", http.StatusBadRequest)
		return
	}
	err = orch.CancelWorkflowHandler(ctx, uuidWorkflowId)
	if err != nil {
		log.Println("Error in canceling workflow %w", err)
		http.Error(w, "Error in canceling workflow", http.StatusInternalServerError)
		return
	}
	err = orch.TestTempLogger(ctx, uuidWorkflowId)
	if err != nil {
		log.Println("Error in logging the states %w", err)
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	orch, err := orchestrator.CreateOrchestrator(ctx, "postgres://postgres:postgres123@localhost:5432/postgres?sslmode=disable", nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		orch.StartOrchestrating(ctx)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		rctx, rcancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer rcancel()
		workflow_id, err := orch.DbDriver.CreateWorkflow(rctx)
		if err != nil {
			log.Println("Error in creating workflow %w", err)
			http.Error(w, "Error in creating workflow", http.StatusInternalServerError)
			return
		}

		err = orch.RootNodeUpdater(rctx, workflow_id)
		if err != nil {
			log.Println("Error in updating workflow %w", err)
			http.Error(w, "Error in creating workflow", http.StatusInternalServerError)
			return
		}
		var response workflowResponse
		response.WorkflowID = workflow_id
		json.NewEncoder(w).Encode(response)

	})

	mux.HandleFunc("PATCH /api/v1/tasks/{task_id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		var req storage.Tempschema
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			log.Println("Error in decoding request %w", err)
			http.Error(w, "Error in decoding request", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()
		flag, err := orch.DbDriver.StatusCheckForIdempotency(ctx, req.Task_id)
		if err != nil {
			log.Println("Error in updating task %w", err)
			http.Error(w, "Error in updating task", http.StatusInternalServerError)
			return
		}
		if !flag {
			log.Println("Error Task is already RUNNING ")
			http.Error(w, "Error task is already running", http.StatusConflict)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("DELETE /api/v1/tasks/{workflow_id}", func(w http.ResponseWriter, r *http.Request) {
		deleteWorkflowHander(w, r, orch)

	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Server started at :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		orch.BackgroundSweeper(ctx)
	}(ctx)

	<-ctx.Done()
	log.Println("shutdowning grace fully")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	wg.Wait()
	stop() //this will not wait it will just trigger the signal so we have to do waiting manualy
	log.Println("Application stopped cleanly.")
}
