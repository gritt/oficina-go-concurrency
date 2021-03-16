package main

import (
	"fmt"
	"time"
)

func worker(id int, jobs <-chan int, results chan int) {
	// loop through the channel of jobs
	for j := range jobs {
		fmt.Println("worker", id, "got the job", j)

		// do something
		time.Sleep(time.Second * 1)
		// do something

		// send result
		results <- j
	}
}

func main() {
	const numberOfJobs = 10
	const numberOfWorkers = 5

	jobs := make(chan int, numberOfJobs)
	results := make(chan int, numberOfJobs)

	// 10 workers
	// jobs at this point are still empty
	// each worker will work in one job at a time
	for w := 1; w <= numberOfWorkers; w++ {
		go worker(w, jobs, results)
	}

	// add jobs todos
	for j := 1; j <= numberOfJobs; j++ {
		jobs <- j
	}
	// close jobs channel
	close(jobs)

	// wait for all results
	for a := 1; a <= numberOfJobs; a++ {
		r := <-results
		fmt.Printf("job %d ✔\n", r)
	}

	// execution finishes
	fmt.Println("execution finishes")
}
