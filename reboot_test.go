package cron

import (
	"sync"
	"testing"
	"time"
)

// TestRebootScheduleConcurrency tests that RebootSchedule is thread-safe
// and only executes once even when accessed concurrently.
func TestRebootScheduleConcurrency(t *testing.T) {
	schedule := &RebootSchedule{}

	// Create a base time for testing
	baseTime := time.Now()

	// Use a channel to collect results from multiple goroutines
	resultChan := make(chan time.Time, 10)
	var wg sync.WaitGroup

	// Launch multiple goroutines to call Next concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nextTime := schedule.Next(baseTime)
			resultChan <- nextTime
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(resultChan)

	// Collect all results
	var results []time.Time
	for result := range resultChan {
		results = append(results, result)
	}

	// Verify that exactly one execution happened at the expected time
	// (baseTime + RebootDelay), and all other calls returned the far future time
	expectedFirstRun := baseTime.Add(RebootDelay)
	expectedFutureTime := time.Date(2099, 1, 1, 0, 0, 0, 0, baseTime.Location())

	firstRunCount := 0
	futureRunCount := 0

	for _, result := range results {
		if result.Equal(expectedFirstRun) {
			firstRunCount++
		} else if result.Equal(expectedFutureTime) {
			futureRunCount++
		} else {
			t.Errorf("Unexpected time returned: %v, expected either %v or %v",
				result, expectedFirstRun, expectedFutureTime)
		}
	}

	// Should have exactly one first run and nine future runs
	if firstRunCount != 1 {
		t.Errorf("Expected exactly 1 first run, got %d", firstRunCount)
	}
	if futureRunCount != 9 {
		t.Errorf("Expected exactly 9 future runs, got %d", futureRunCount)
	}

	t.Logf("Successfully verified concurrency: %d first run, %d future runs",
		firstRunCount, futureRunCount)
}

// TestRebootScheduleSequential tests that RebootSchedule works correctly in sequential calls
func TestRebootScheduleSequential(t *testing.T) {
	schedule := &RebootSchedule{}
	baseTime := time.Now()

	// First call should return time after RebootDelay
	firstResult := schedule.Next(baseTime)
	expectedFirst := baseTime.Add(RebootDelay)

	if !firstResult.Equal(expectedFirst) {
		t.Errorf("First call: expected %v, got %v", expectedFirst, firstResult)
	}

	// Second call should return time in far future
	secondResult := schedule.Next(baseTime)
	expectedFuture := time.Date(2099, 1, 1, 0, 0, 0, 0, baseTime.Location())

	if !secondResult.Equal(expectedFuture) {
		t.Errorf("Second call: expected %v, got %v", expectedFuture, secondResult)
	}
}
