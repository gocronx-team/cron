package cron

import "sync/atomic"

// Metrics contains statistics about cron job execution.
type Metrics struct {
	TotalRuns   int64
	TotalPanics int64
	ActiveJobs  int32
	// DroppedJobs is the number of fires dropped because the worker pool was
	// saturated (jobs firing faster than they can run). Non-zero indicates the
	// scheduler is overloaded.
	DroppedJobs int64
}

func (m *Metrics) incRuns() {
	atomic.AddInt64(&m.TotalRuns, 1)
}

func (m *Metrics) incPanics() {
	atomic.AddInt64(&m.TotalPanics, 1)
}

func (m *Metrics) incActive() {
	atomic.AddInt32(&m.ActiveJobs, 1)
}

func (m *Metrics) decActive() {
	atomic.AddInt32(&m.ActiveJobs, -1)
}
