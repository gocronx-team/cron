package cron

import (
	"log"
	"os"
	"runtime/debug"
	"strings"
	"sync"
)

// SafeJob wraps a Job with panic recovery
type SafeJob struct {
	Job
	Name    string
	metrics *Metrics
}

func (s *SafeJob) Run() {
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

var (
	testModeOnce   sync.Once
	testModeCached bool
)

// isTestMode checks if running in test environment (cached)
func isTestMode() bool {
	testModeOnce.Do(func() {
		for _, arg := range os.Args {
			if strings.HasPrefix(arg, "-test.") {
				testModeCached = true
				return
			}
		}
	})
	return testModeCached
}
