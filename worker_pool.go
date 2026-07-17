package cron

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// defaultQueueFactor bounds how many goroutines may be waiting for an execution
// slot, as a multiple of the concurrency limit. Without this bound a scheduler
// that fires faster than its jobs complete spawns a goroutine per fire that
// blocks on the semaphore, growing without limit until the process OOMs.
const defaultQueueFactor = 16

// WorkerPool manages concurrent job execution with a bounded number of
// in-flight goroutines.
type WorkerPool struct {
	semaphore   chan struct{} // limits concurrent execution
	maxInFlight int32         // hard cap on spawned goroutines (waiting + running)
	inFlight    int32         // atomic: goroutines currently spawned
	dropped     int64         // atomic: jobs dropped because the pool was saturated
	wg          sync.WaitGroup
}

// NewWorkerPool creates a worker pool with the specified max concurrent workers.
// If maxWorkers <= 0, a default of runtime.NumCPU() * 128 is used. The number of
// in-flight goroutines is bounded to maxWorkers * defaultQueueFactor.
func NewWorkerPool(maxWorkers int) *WorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU() * 128
	}
	return NewWorkerPoolWithQueue(maxWorkers, maxWorkers*defaultQueueFactor)
}

// NewWorkerPoolWithQueue creates a worker pool with an explicit concurrency limit
// (maxWorkers) and an explicit cap on total in-flight goroutines (maxInFlight).
// maxInFlight is raised to at least maxWorkers. When the cap is reached, further
// Submit calls drop the job (non-blocking) and increment Dropped(), instead of
// spawning yet another goroutine that would pile up without bound.
func NewWorkerPoolWithQueue(maxWorkers, maxInFlight int) *WorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU() * 128
	}
	if maxInFlight < maxWorkers {
		maxInFlight = maxWorkers
	}
	return &WorkerPool{
		semaphore:   make(chan struct{}, maxWorkers),
		maxInFlight: int32(maxInFlight),
	}
}

// Submit runs job on the pool. It never blocks the caller (the scheduler's run()
// loop): if the pool is already at its in-flight cap, the job is dropped and
// counted rather than spawning another goroutine. Concurrency of actually
// executing jobs is still limited to maxWorkers by the semaphore.
func (wp *WorkerPool) Submit(job func()) {
	// Admission control: reserve an in-flight slot up front. Exceeding the cap
	// means jobs are firing faster than they can run; drop rather than grow
	// waiting goroutines without bound.
	if atomic.AddInt32(&wp.inFlight, 1) > wp.maxInFlight {
		atomic.AddInt32(&wp.inFlight, -1)
		atomic.AddInt64(&wp.dropped, 1)
		return
	}

	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
		defer atomic.AddInt32(&wp.inFlight, -1)
		wp.semaphore <- struct{}{} // acquire execution slot
		defer func() { <-wp.semaphore }()
		job()
	}()
}

// Wait waits for all accepted jobs to complete.
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

// Running returns the number of jobs currently executing.
func (wp *WorkerPool) Running() int { return len(wp.semaphore) }

// Pending returns the number of in-flight goroutines (waiting for a slot plus
// currently executing).
func (wp *WorkerPool) Pending() int { return int(atomic.LoadInt32(&wp.inFlight)) }

// Dropped returns the total number of jobs dropped because the pool was saturated.
func (wp *WorkerPool) Dropped() int64 { return atomic.LoadInt64(&wp.dropped) }
