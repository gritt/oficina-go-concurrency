package main

import (
	"fmt"
	"sync"
	"time"
)

func printer(i int, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("printing index: ", i)
	time.Sleep(2 * time.Second)
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	for i := 0; i < 2; i++ {
		go printer(i, &wg)
	}

	wg.Wait()

	fmt.Println("done after waiting!")
}
