package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Payload struct {
	Data string `json:"data"`
	UUID string `json:"uuid"`
}

type Job struct {
	Payload
}

// PendingJobs is a channel to send jobs todo
type PendingJobs chan Job

// CompletedJobs is a channel of jobs that are done
type CompletedJobs chan Job

// Worker represents the worker that executes the job
type Worker struct {
	ID int
	PendingJobs
	CompletedJobs
}

func NewWorker(ID int, pendingJobs PendingJobs, completedJobs CompletedJobs) Worker {
	return Worker{
		ID:            ID,
		PendingJobs:   pendingJobs,
		CompletedJobs: completedJobs,
	}
}

func (w Worker) Start() {
	// Find an available jobs to work on
	for job := range w.PendingJobs {

		// Process Job
		fmt.Printf("worker %d got the job: %s \n", w.ID, job.UUID)

		//  Error handling
		// 	Log error
		// 	Add job back to queue

		time.Sleep(time.Second * 1)

		// Send completed job
		w.CompletedJobs <- job
	}
}

func (w Worker) StartWithWithWaitGroup(wg *sync.WaitGroup) {
	defer wg.Done()

	// Find an available jobs to work on
	for job := range w.PendingJobs {

		// Process Job
		fmt.Printf("worker %d got the job: %s \n", w.ID, job.UUID)

		//  Error handling
		// 	Log error
		// 	Add job back to queue

		time.Sleep(time.Second * 1)

		// Send completed job
		// w.CompletedJobs <- job
		fmt.Printf("job %s  ✔ \n", job.UUID)
	}
}

func StartFooConsumerWithWaitGroup(numberOfWorkers int, jobs []Job) {
	numberOfJobs := len(jobs)

	pending := make(chan Job, numberOfJobs)

	// sync.WaitGroup used to synchronize the group
	// of goroutines that are being executed
	var wg sync.WaitGroup
	wg.Add(numberOfWorkers)

	// Multiple workers
	// Pending jobs at this point are still empty
	// Each worker will work in 1 Job at a time
	for w := 1; w <= numberOfWorkers; w++ {
		worker := NewWorker(w, pending, nil)
		go worker.StartWithWithWaitGroup(&wg)
	}

	// Enqueue jobs todo in pending
	for _, job := range jobs {
		pending <- job
	}

	// Close pending jobs channel
	close(pending)

	// Wait until all group be finished
	wg.Wait()

	// Consumer finishes
	fmt.Println("foo consumer finished")
}

func StartFooConsumer(numberOfWorkers int, jobs []Job) {
	numberOfJobs := len(jobs)

	pending := make(chan Job, numberOfJobs)
	completed := make(chan Job, numberOfJobs)

	// Multiple workers
	// Pending jobs at this point are still empty
	// Each worker will work in 1 Job at a time
	for w := 1; w <= numberOfWorkers; w++ {
		worker := NewWorker(w, pending, completed)
		go worker.Start()
	}

	// Enqueue jobs todo in pending
	for _, job := range jobs {
		pending <- job
	}

	// Close pending jobs channel
	close(pending)

	// Wait for jobs all to be completed
	for a := 1; a <= numberOfJobs; a++ {
		job := <-completed
		fmt.Printf("job %s  ✔ \n", job.UUID)
	}

	// Consumer finishes
	fmt.Println("foo consumer finished")
}

func main() {
	const numberOfWorkers = 100
	const numberOfJobs = 100

	// jobs := CollectJobs(numberOfJobs)
	// StartFooConsumerWithWaitGroup(numberOfWorkers, jobs)

	//
	// StartFooConsumerWithWaitGroup(numberOfWorkers, jobs)
	//

	// // ETL Pipeline // // //
	for {
		// [1]
		// Collect
		jobs := CollectJobs(numberOfJobs)

		// [2]
		// Process
		StartFooConsumer(numberOfWorkers, jobs)

		// [3]
		// Wait new jobs
		time.Sleep(time.Second * 3)
	}

	fmt.Println("main finished")
}

func CollectJobs(amount int) []Job {
	jobs := []Job{}

	for j := 0; j <= amount; j++ {
		jobs = append(jobs, Job{
			Payload{
				Data: FakeString(20),
				UUID: FakeString(10),
			},
		})
	}

	return jobs
}

func FakeString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	rand.Seed(time.Now().UnixNano())

	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func FakeNumber(length int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(length)
}
