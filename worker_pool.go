package cron

import "sync"

// WorkerPool manages concurrent job execution
type WorkerPool struct {
	semaphore chan struct{}
	wg        sync.WaitGroup
}

// NewWorkerPool creates a worker pool with specified max workers
// If maxWorkers <= 0, no limit is applied
func NewWorkerPool(maxWorkers int) *WorkerPool {
	if maxWorkers <= 0 {
		return &WorkerPool{}
	}
	return &WorkerPool{
		semaphore: make(chan struct{}, maxWorkers),
	}
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(job func()) {
	if wp.semaphore != nil {
		wp.semaphore <- struct{}{}
	}
	wp.wg.Add(1)

	go func() {
		defer wp.wg.Done()
		if wp.semaphore != nil {
			defer func() { <-wp.semaphore }()
		}
		job()
	}()
}

// Wait waits for all jobs to complete
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}
