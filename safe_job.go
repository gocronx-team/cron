package cron

import (
	"log"
	"os"
	"runtime/debug"
	"strings"
)

// SafeJob wraps a Job with panic recovery
type SafeJob struct {
	Job
	Name    string
	metrics *Metrics
}

func (s SafeJob) Run() {
	if s.metrics != nil {
		s.metrics.incActive()
		s.metrics.incRuns()
		defer s.metrics.decActive()
	}

	defer func() {
		if r := recover(); r != nil {
			if s.metrics != nil {
				s.metrics.incPanics()
			}
			// Simplified log in test mode
			if isTestMode() {
				log.Printf("[CRON] Job '%s' panicked: %v (recovered)", s.Name, r)
			} else {
				log.Printf("Job '%s' panicked: %v\nStack trace:\n%s", s.Name, r, debug.Stack())
			}
		}
	}()
	s.Job.Run()
}

// wrapJob wraps a job with panic recovery (for backward compatibility)
func wrapJob(job Job, name string) Job {
	return SafeJob{Job: job, Name: name}
}

// wrapJobWithMetrics wraps a job with panic recovery and metrics
func wrapJobWithMetrics(job Job, name string, metrics *Metrics) Job {
	return SafeJob{Job: job, Name: name, metrics: metrics}
}

// isTestMode checks if running in test environment
func isTestMode() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}
	return false
}
