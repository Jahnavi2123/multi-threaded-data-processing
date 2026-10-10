package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	taskCount   = 20
	workerCount = 3
)

type Task struct {
	ID    int
	Value int
}

type Result struct {
	TaskID int
	Input  int
	Output int
	Worker string
}

type SharedTaskQueue struct {
	tasks chan Task
}

func newTaskQueue(capacity int) *SharedTaskQueue {
	return &SharedTaskQueue{
		tasks: make(chan Task, capacity),
	}
}

func (q *SharedTaskQueue) addTask(task Task) {
	q.tasks <- task
}

func (q *SharedTaskQueue) getTask() <-chan Task {
	return q.tasks
}

func (q *SharedTaskQueue) close() {
	close(q.tasks)
}

func worker(
	id int,
	queue *SharedTaskQueue,
	results chan<- Result,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	name := fmt.Sprintf("Worker-%d", id)
	log.Printf("%s started", name)
	defer log.Printf("%s stopped", name)

	for task := range queue.getTask() {
		time.Sleep(100 * time.Millisecond)

		output := task.Value * task.Value

		results <- Result{
			TaskID: task.ID,
			Input:  task.Value,
			Output: output,
			Worker: name,
		}

		log.Printf("%s completed Task %d -> %d",
			name, task.ID, output)
	}
}

func saveResults(filename string, results []Result) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	if err := writer.Write(
		[]string{"task_id", "input", "output", "worker"},
	); err != nil {
		return fmt.Errorf("write CSV header: %w", err)
	}

	for _, result := range results {
		record := []string{
			strconv.Itoa(result.TaskID),
			strconv.Itoa(result.Input),
			strconv.Itoa(result.Output),
			result.Worker,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf(
				"write task %d: %w", result.TaskID, err)
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush CSV: %w", err)
	}

	return nil
}

func run() error {
	queue := newTaskQueue(taskCount)
	resultsChannel := make(chan Result, taskCount)

	var wg sync.WaitGroup

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, queue, resultsChannel, &wg)
	}

	for i := 1; i <= taskCount; i++ {
		queue.addTask(Task{ID: i, Value: i})
	}

	queue.close()

	go func() {
		wg.Wait()
		close(resultsChannel)
	}()

	results := make([]Result, 0, taskCount)

	for result := range resultsChannel {
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].TaskID < results[j].TaskID
	})

	seen := make(map[int]bool)

	for _, result := range results {
		if seen[result.TaskID] {
			return fmt.Errorf(
				"duplicate task ID: %d", result.TaskID)
		}
		seen[result.TaskID] = true
	}

	if len(results) != taskCount {
		return fmt.Errorf(
			"expected %d results, received %d",
			taskCount, len(results))
	}

	if err := saveResults("results.csv", results); err != nil {
		return err
	}

	fmt.Println("\nGO PROCESSING SUMMARY")
	fmt.Printf("Tasks submitted: %d\n", taskCount)
	fmt.Printf("Results saved: %d\n", len(results))
	fmt.Printf("Unique task IDs: %d\n", len(seen))
	fmt.Println("Output file: results.csv")
	fmt.Println("Status: SUCCESS")

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Printf("Processing failed: %v", err)
		os.Exit(1)
	}
}
