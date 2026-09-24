package postgresDb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	Graph "github.com/JoYBoy7214/DAG-Orchestrator/internal"
	storage "github.com/JoYBoy7214/DAG-Orchestrator/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDriver struct {
	pool *pgxpool.Pool
}

func CreatePostgresDriver(ctx context.Context, dbstring string) (*PostgresDriver, error) {
	pool, err := pgxpool.New(ctx, dbstring)
	if err != nil {
		return nil, fmt.Errorf("Error in creating a pgx pool %w", err)
	}
	pd := &PostgresDriver{
		pool: pool,
	}
	err = pd.createDbs(ctx)
	if err != nil {
		return nil, err
	}
	return pd, nil
}

func (pd *PostgresDriver) createDbs(ctx context.Context) error {
	_, err := pd.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS workflow (
    workflow_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status      VARCHAR(20) DEFAULT 'PENDING' NOT NULL,
    updated_at  TIMESTAMP,
    input_data  JSONb ,
    output_data JSONb
 	);`)
	if err != nil {
		return fmt.Errorf("Error in creating workflow table %w", err)
	}

	_, err = pd.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS tasks (
    workflow_id UUID NOT NULL,
    task_id     UUID PRIMARY KEY,
    task_type   VARCHAR(100),
    status      VARCHAR(20) DEFAULT 'PENDING' NOT NULL,
    updated_at  TIMESTAMP,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    input_data  JSONb,
    output_data JSONb,

    CONSTRAINT fk_workflow FOREIGN KEY (workflow_id) REFERENCES workflow(workflow_id) ON DELETE CASCADE  
    );`)
	if err != nil {
		return fmt.Errorf("Error in creating tasks table %w", err)
	}

	_, err = pd.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS edges(
    parent_id  UUID NOT NULL,
    child_id   UUID NOT NULL,

    PRIMARY kEY (parent_id,child_id),
    CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES tasks(task_id) on DELETE CASCADE,
    CONSTRAINT fk_child FOREIGN KEY (child_id) REFERENCES tasks(task_id) on DELETE CASCADE
    );`)
	if err != nil {
		return fmt.Errorf("Error in creating edges table %w", err)
	}
	_, err = pd.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_edges_child_id on edges(child_id);`) //postgres creates index on only one primary key auto matically so we have to manually index it
	if err != nil {
		return fmt.Errorf("Error in creating index on child_id on edges table %w", err)
	}
	_, err = pd.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS compound_idx_task_status_workflow_id on tasks(status,workflow_id);`) //creating index on status and workflow_id so DAG evaluate can be optimized
	if err != nil {
		return fmt.Errorf("Error in creating index on (status,workflow_id) on tasks table %w", err)
	}

	return err
}

func (pd *PostgresDriver) CreateWorkflow(ctx context.Context) (uuid.UUID, error) {
	DAG := Graph.CreateGraph()
	workflow_id := uuid.New()
	current_time := time.Now().UTC()
	tx, err := pd.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("Error in creating Transaction connection %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO workflow (workflow_id,created_at,status,updated_at) VALUES ($1,$2,'PENDING',$3)`, workflow_id, current_time, current_time)
	if err != nil {
		return uuid.Nil, fmt.Errorf("Error in inserting into workflow %w", err)
	}

	Q_string_task_insert := `INSERT INTO tasks (workflow_id,task_id,task_type,status,updated_at,created_at)
							VALUES($1,$2,$3,'PENDING',$4,$5)`
	for key := range DAG.Nodes {
		task_id := uuid.New()
		_, err = tx.Exec(ctx, Q_string_task_insert, workflow_id, task_id, key, current_time, current_time)
		if err != nil {
			return uuid.Nil, fmt.Errorf("Error in inserting into task table task failed is %s, Error: %w", key, err)
		}
		DAG.Nodes[key] = &Graph.Node{ //could cause a problem need to look into it
			Name: key,
			ID:   task_id,
		}
	}

	DAG.HardCodeIt()
	Q_string_edge_insert := `INSERT INTO edges(parent_id,child_id) Values ($1,$2)`
	for key, val := range DAG.Edges {
		for _, id := range val {
			_, err = tx.Exec(ctx, Q_string_edge_insert, key, id)
			if err != nil {
				return uuid.Nil, fmt.Errorf("Error in inserting edge in edges table %w", err)
			}
		}

	}
	err = tx.Commit(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("error in commiting transactions %w", err)
	}

	return workflow_id, nil
}
func (pd *PostgresDriver) Evaluate(ctx context.Context, workflow_id uuid.UUID) ([]uuid.UUID, error) {
	cur_time := time.Now().UTC() //could cause inconsistency
	tx, err := pd.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error in creating Transaction connection %w", err)
	}
	defer tx.Rollback(ctx)

	//for every pending task with workflow_id= workflow_id(of tasks table) we will check every row in edges which has child_id=task_id(in the task table)
	//and check if it parent_status(note: we have mode join on subquery which give edge table about the info of parent status)
	//we switched from count to subquery since not exists works better than count when we don't need the exact count
	evaluate_and_update_string := `
	UPDATE tasks
	SET status = 'READY',updated_at=$2
	WHERE status = 'PENDING' and workflow_id =$1
    AND NOT EXISTS ( 
		SELECT 1
		FROM edges e
		LEFT JOIN tasks p ON e.parent_id = p.task_id
		WHERE e.child_id = tasks.task_id
		AND (p.status IS NULL OR p.status != 'COMPLETED')
    )RETURNING tasks.task_id`
	rows, err := tx.Query(ctx, evaluate_and_update_string, workflow_id, cur_time)
	if err != nil {
		return nil, fmt.Errorf("error in  evaluating DAG %w", err)
	}
	defer rows.Close()
	var ready_list []uuid.UUID
	for rows.Next() {
		var temp uuid.UUID
		if err := rows.Scan(&temp); err != nil {
			return nil, fmt.Errorf("Error in scanning the rows %w", err)
		}
		ready_list = append(ready_list, temp)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("Error in iterating the rows %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error in commiting transactions while evaluating DAG %w", err)
	}
	return ready_list, nil
}

func (pd *PostgresDriver) UpdateTask(ctx context.Context, task_id uuid.UUID, workflow_id uuid.UUID, status string) error {
	cur_time := time.Now().UTC()
	query_string := `
	update tasks
	set status =$1,updated_at =$4
	where workflow_id=$2 AND task_id=$3
	`
	_, err := pd.pool.Exec(ctx, query_string, status, workflow_id, task_id, cur_time)
	if err != nil {
		return fmt.Errorf("Error in updating the task status %w", err)
	}
	return nil
}

type Temp struct {
	Task_id     uuid.UUID
	Task_type   string
	Task_status string
}

func (pd *PostgresDriver) GetAllReadyLongLivedTasks(ctx context.Context) ([]storage.Tempschema, error) {
	var result []storage.Tempschema
	cur_time := time.Now().UTC()

	//this query will first update the long running tasks(which are in RUNNING state for more than one minute) to READY and return the those task (with updated_running_tasks)
	//the second block of query will union the previous result with ready task which are in same state for more than one minute
	//postgres will use the snapshot of the data when the query started so there will be no duplications in the second block

	query_string := `WITH updated_running_tasks AS (
    UPDATE tasks
    SET status = 'READY'
    WHERE status = 'RUNNING' 
      AND ($1 - updated_at) > INTERVAL '4 minute'
    RETURNING workflow_id, Task_id, Task_type
	)
	SELECT * FROM updated_running_tasks
	UNION ALL
	SELECT workflow_id, Task_id, Task_type 
	FROM tasks 
	WHERE status = 'READY' 
    AND ($1 - updated_at) > INTERVAL '4 minute'; `
	rows, err := pd.pool.Query(ctx, query_string, cur_time)
	if err != nil {
		return nil, fmt.Errorf("Error in getting the long lived ready rows  %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var temp storage.Tempschema
		if err = rows.Scan(&temp.WorkFlow_id, &temp.Task_id, &temp.Task_type); err != nil {
			return nil, fmt.Errorf("Error in iterating the rows %w", err)
		}
		result = append(result, temp)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("Error in iterating the rows %w", err)
	}

	return result, nil
}

func (pd PostgresDriver) StatusCheckForIdempotency(ctx context.Context, taskID uuid.UUID) (bool, error) {
	curTime := time.Now().UTC()
	query := `
        UPDATE tasks
        SET status = 'RUNNING', updated_at = $2
        WHERE task_id = $1
          AND status = 'READY'
        RETURNING status;
    `

	var status string
	err := pd.pool.QueryRow(ctx, query, taskID, curTime).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Task does not exist or was already picked up
			log.Println("Error from postgress: %w", err)
			return false, nil
		}
		return false, fmt.Errorf("failed to claim task: %w", err)
	}

	return true, nil // Successfully claimed
}

func (pd *PostgresDriver) UpdateWorkflowStatus(ctx context.Context, workflow_id uuid.UUID, status string) error {
	cur_time := time.Now().UTC()
	query_string := `
	update workflow
	set status =$1,updated_at =$3
	where workflow_id=$2 
	`
	_, err := pd.pool.Exec(ctx, query_string, status, workflow_id, cur_time)
	if err != nil {
		return fmt.Errorf("Error in updating the workflow status %w", err)
	}
	return nil
}
func (pd *PostgresDriver) UpdateWorkflowCompletionStatus(ctx context.Context, workflow_id uuid.UUID) (bool, error) {
	cur_time := time.Now().UTC()
	query_string := `
	UPDATE workflow SET status='COMPLETED', updated_at=$2
WHERE workflow_id=$1 AND status NOT IN ('CANCELLED','COMPLETED')`
	pg_tag, err := pd.pool.Exec(ctx, query_string, workflow_id, cur_time)
	if err != nil {
		return false, fmt.Errorf("Error in updating the workflow status %w", err)
	}
	return pg_tag.RowsAffected() > 0, nil
}

func (pd *PostgresDriver) DeleteWorkflow(ctx context.Context, workflow_id uuid.UUID) error {
	cur_time := time.Now().UTC()
	tx, err := pd.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error in creating Transaction connection %w", err)
	}
	defer tx.Rollback(ctx)

	workflow_query_string := `UPDATE workflow
SET status = 'CANCELLED', updated_at = $2
WHERE workflow_id = $1
  AND status NOT IN ('COMPLETED', 'CANCELLED')
RETURNING status;`

	tag, err := tx.Exec(ctx, workflow_query_string, workflow_id, cur_time)
	if err != nil {
		return fmt.Errorf("Error in updating the workflow status %w", err)
	}

	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	query_string := `
	update tasks
	set status =$1,updated_at =$3
	where workflow_id=$2 and status != 'COMPLETED'
	`
	_, err = tx.Exec(ctx, query_string, "CANCELLED", workflow_id, cur_time)
	if err != nil {
		return fmt.Errorf("Error in updating the task status %w", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("Error in committing transaction %w", err)
	}
	return nil
}

func (pd *PostgresDriver) CheckWorkflowComplete(ctx context.Context, workflow_id uuid.UUID) (bool, time.Time, error) {
	var creationTime time.Time
	query_string := `
	SELECT created_at FROM workflow WHERE workflow_id = $1 AND NOT EXISTS (
		SELECT 1 FROM tasks WHERE workflow_id = $1 AND status != 'COMPLETED'
	);`
	err := pd.pool.QueryRow(ctx, query_string, workflow_id).Scan(&creationTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, time.Time{}, nil // Workflow is not complete
		}
		return false, time.Time{}, fmt.Errorf("Error in checking workflow completion %w", err)
	}
	return true, creationTime, nil // Workflow is complete
}

func (pd *PostgresDriver) TesttempGettingInfo(ctx context.Context, workflow_id uuid.UUID) ([]Temp, error) {
	var result []Temp
	query_string := "select task_id,task_type,status from tasks where workflow_id =$1"
	rows, err := pd.pool.Query(ctx, query_string, workflow_id)
	if err != nil {
		return nil, fmt.Errorf("Error in executing command to get ready task %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var temp1 Temp
		if err = rows.Scan(&temp1.Task_id, &temp1.Task_type, &temp1.Task_status); err != nil {
			return nil, fmt.Errorf("Error in scanning the rows %w", err)
		}
		result = append(result, temp1)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("Error in iterating the rows %w", err)
	}
	return result, nil
}
