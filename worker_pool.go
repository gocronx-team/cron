package cron

import (
	"runtime"
	"sync"
)

// WorkerPool manages concurrent job execution
type WorkerPool struct {
	semaphore chan struct{}
	wg        sync.WaitGroup
}

// NewWorkerPool creates a worker pool with specified max workers.
// If maxWorkers <= 0, a default cap of runtime.NumCPU() * 128 is used.
func NewWorkerPool(maxWorkers int) *WorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU() * 128
	}
	return &WorkerPool{
		semaphore: make(chan struct{}, maxWorkers),
	}
}

// Submit submits a job to the worker pool.
// The semaphore acquisition happens inside the goroutine so that the caller
// (the scheduler's run() loop) is never blocked by a saturated pool.
func (wp *WorkerPool) Submit(job func()) {
	wp.wg.Add(1)

	go func() {
		wp.semaphore <- struct{}{} // acquire slot inside goroutine
		defer wp.wg.Done()
		defer func() { <-wp.semaphore }()
		job()
	}()
}

// Wait waits for all jobs to complete
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}
