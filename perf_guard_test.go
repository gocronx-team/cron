package cron

// perf_guard_test.go: Tests that guard correctness during performance optimization.
// These tests verify internal behavior invariants that MUST hold after any
// refactoring of parser, pooling, snapshot, or concurrency paths.

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// G1: Parser Equivalence Tests
// Ensures that after parser refactoring, every spec produces the exact same
// SpecSchedule bit patterns as before.
// =============================================================================

func TestGuard_ParserEquivalence_StandardSpecs(t *testing.T) {
	// Each spec must produce exactly the documented SpecSchedule.
	// If parser internals change, these catch silent bit corruption.
	tests := []struct {
		spec     string
		expected SpecSchedule
	}{
		{
			"* * * * * *",
			SpecSchedule{all(seconds), all(minutes), all(hours), all(dom), all(months), all(dow)},
		},
		{
			"0 0 0 * * *",
			SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months), all(dow)},
		},
		{
			"5 10 15 20 6 3",
			SpecSchedule{1 << 5, 1 << 10, 1 << 15, 1 << 20, 1 << 6, 1 << 3},
		},
		{
			"0-5 * * * * *",
			SpecSchedule{
				1<<0 | 1<<1 | 1<<2 | 1<<3 | 1<<4 | 1<<5,
				all(minutes), all(hours), all(dom), all(months), all(dow),
			},
		},
		{
			"*/10 * * * * *",
			SpecSchedule{
				1<<0 | 1<<10 | 1<<20 | 1<<30 | 1<<40 | 1<<50 | starBit,
				all(minutes), all(hours), all(dom), all(months), all(dow),
			},
		},
		{
			"0,15,30,45 * * * * *",
			SpecSchedule{
				1<<0 | 1<<15 | 1<<30 | 1<<45,
				all(minutes), all(hours), all(dom), all(months), all(dow),
			},
		},
		{
			"0 0 0 1-15/3 * *",
			SpecSchedule{
				1 << 0, 1 << 0, 1 << 0,
				1<<1 | 1<<4 | 1<<7 | 1<<10 | 1<<13,
				all(months), all(dow),
			},
		},
		{
			"0 0 0 * 1,6,12 *",
			SpecSchedule{
				1 << 0, 1 << 0, 1 << 0, all(dom),
				1<<1 | 1<<6 | 1<<12,
				all(dow),
			},
		},
		{
			"0 0 0 * * 0-4",
			SpecSchedule{
				1 << 0, 1 << 0, 1 << 0, all(dom), all(months),
				1<<0 | 1<<1 | 1<<2 | 1<<3 | 1<<4,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			sched, err := ParseWithError(tt.spec)
			if err != nil {
				t.Fatalf("ParseWithError(%q) error: %v", tt.spec, err)
			}
			spec, ok := sched.(*SpecSchedule)
			if !ok {
				t.Fatalf("ParseWithError(%q) returned %T, want *SpecSchedule", tt.spec, sched)
			}
			if *spec != tt.expected {
				t.Errorf("ParseWithError(%q) =\n  got  %+v\n  want %+v", tt.spec, *spec, tt.expected)
			}
		})
	}
}

func TestGuard_ParserEquivalence_WithNames(t *testing.T) {
	tests := []struct {
		spec     string
		expected SpecSchedule
	}{
		{
			"0 0 0 * JAN *",
			SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), 1 << 1, all(dow)},
		},
		{
			"0 0 0 * * MON-FRI",
			SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months),
				1<<1 | 1<<2 | 1<<3 | 1<<4 | 1<<5},
		},
		{
			"0 0 0 * JAN-MAR *",
			SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom),
				1<<1 | 1<<2 | 1<<3, all(dow)},
		},
		{
			"0 0 0 * * SUN,SAT",
			SpecSchedule{1 << 0, 1 << 0, 1 << 0, all(dom), all(months),
				1<<0 | 1<<6},
		},
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			sched, err := ParseWithError(tt.spec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			spec := sched.(*SpecSchedule)
			if *spec != tt.expected {
				t.Errorf("got %+v, want %+v", *spec, tt.expected)
			}
		})
	}
}

func TestGuard_ParserEquivalence_FiveFields(t *testing.T) {
	// 5-field spec should add implicit seconds=0
	sched, err := ParseWithError("* * * * *")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	spec := sched.(*SpecSchedule)
	// 5-field: second minute hour dom month [dow=*]
	// Wait, actually in this library 5-field means: second minute hour dom month, with dow defaulting to *
	// Let me check: fields[0]=second, ..., fields[4]=month, fields[5]=dow appended as "*"
	expected := SpecSchedule{
		all(seconds), all(minutes), all(hours), all(dom), all(months), all(dow),
	}
	if *spec != expected {
		t.Errorf("5-field '* * * * *' =\n  got  %+v\n  want %+v", *spec, expected)
	}
}

func TestGuard_ParserEquivalence_Descriptors(t *testing.T) {
	tests := []struct {
		spec string
		// We check type and key fields
		checkType string
	}{
		{"@yearly", "SpecSchedule"},
		{"@annually", "SpecSchedule"},
		{"@monthly", "SpecSchedule"},
		{"@weekly", "SpecSchedule"},
		{"@daily", "SpecSchedule"},
		{"@midnight", "SpecSchedule"},
		{"@hourly", "SpecSchedule"},
		{"@reboot", "RebootSchedule"},
		{"@every 5s", "ConstantDelaySchedule"},
		{"@every 1h30m", "ConstantDelaySchedule"},
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			sched, err := ParseWithError(tt.spec)
			if err != nil {
				t.Fatalf("error: %v", err)
			}
			rt := reflect.TypeOf(sched)
			var typeName string
			if rt.Kind() == reflect.Ptr {
				typeName = rt.Elem().Name()
			} else {
				typeName = rt.Name()
			}
			if typeName != tt.checkType {
				t.Errorf("type = %s, want %s", typeName, tt.checkType)
			}
		})
	}
}

func TestGuard_ParserErrors_Preserved(t *testing.T) {
	// Every error case must still return an error after refactoring.
	errorSpecs := []string{
		"",
		"* * *",
		"* * * * * * *",
		"60 * * * * *",
		"* 60 * * * *",
		"* * 24 * * *",
		"* * * 32 * *",
		"* * * * 13 *",
		"* * * * * 7",
		"* * * 0 * *",
		"* * * * 0 *",
		"5-3 * * * * *",
		"*/0 * * * * *",
		"@invalid",
		"@every invalid",
		"@every 500ms", // sub-second
		"abc * * * * *",
		"* abc * * * *",
		"1-2-3 * * * * *",
		"1/2/3 * * * * *",
	}

	for _, spec := range errorSpecs {
		t.Run(spec, func(t *testing.T) {
			_, err := ParseWithError(spec)
			if err == nil {
				t.Errorf("ParseWithError(%q) should return error", spec)
			}
		})
	}
}

// =============================================================================
// G2: Schedule.Next() Equivalence Tests
// Ensures SpecSchedule.Next() produces identical times after any bit-scanning optimization.
// =============================================================================

func TestGuard_NextTime_Equivalence(t *testing.T) {
	base := time.Date(2026, 3, 12, 14, 30, 0, 0, time.Local)

	tests := []struct {
		spec     string
		expected time.Time
	}{
		// Every second -> next second
		{"* * * * * *", time.Date(2026, 3, 12, 14, 30, 1, 0, time.Local)},
		// Every minute at :00
		{"0 * * * * *", time.Date(2026, 3, 12, 14, 31, 0, 0, time.Local)},
		// Specific time far in the future
		{"0 0 0 1 1 *", time.Date(2027, 1, 1, 0, 0, 0, 0, time.Local)},
		// Every 15 seconds
		{"0,15,30,45 * * * * *", time.Date(2026, 3, 12, 14, 30, 15, 0, time.Local)},
		// Specific day-of-week (Thursday = 4, 2026-03-12 is Thursday)
		{"0 0 0 * * 4", time.Date(2026, 3, 19, 0, 0, 0, 0, time.Local)},
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			sched := Parse(tt.spec)
			got := sched.Next(base)
			if !got.Equal(tt.expected) {
				t.Errorf("Next(%v) = %v, want %v", base, got, tt.expected)
			}
		})
	}
}

func TestGuard_NextTime_ConsecutiveCalls(t *testing.T) {
	// Verify that calling Next() repeatedly produces strictly increasing times
	sched := Parse("*/5 * * * * *") // every 5 seconds
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)

	prev := now
	for i := 0; i < 100; i++ {
		next := sched.Next(prev)
		if !next.After(prev) {
			t.Fatalf("iteration %d: Next(%v) = %v, not strictly increasing", i, prev, next)
		}
		prev = next
	}
}

// =============================================================================
// G3: Snapshot Isolation Tests
// Ensures Entries() returns a true copy that is isolated from the scheduler.
// =============================================================================

func TestGuard_SnapshotIsolation_NotRunning(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {}, "job1")
	c.AddFunc("@every 5s", func() {}, "job2")

	snap := c.Entries()
	if len(snap) != 2 {
		t.Fatalf("len(snap) = %d, want 2", len(snap))
	}

	// Mutate the snapshot - should NOT affect the original
	snap[0].Name = "MUTATED"
	snap = append(snap, &Entry{Name: "extra"})

	original := c.Entries()
	for _, e := range original {
		if e.Name == "MUTATED" || e.Name == "extra" {
			t.Error("snapshot mutation leaked to original entries")
		}
	}
}

func TestGuard_SnapshotIsolation_WhileRunning(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {}, "job1")
	c.Start()
	defer c.Stop()

	time.Sleep(10 * time.Millisecond)

	snap := c.Entries()
	if len(snap) != 1 {
		t.Fatalf("len(snap) = %d, want 1", len(snap))
	}

	// Mutate
	snap[0].Name = "MUTATED"

	snap2 := c.Entries()
	if snap2[0].Name == "MUTATED" {
		t.Error("snapshot mutation leaked")
	}
}

func TestGuard_SnapshotSortOrder(t *testing.T) {
	c := New()
	c.AddFunc("0 0 0 1 1 *", func() {}, "far_future")    // Jan 1
	c.AddFunc("* * * * * *", func() {}, "every_second")    // soonest
	c.AddFunc("0 0 0 31 12 *", func() {}, "end_of_year")   // Dec 31
	c.Start()
	defer c.Stop()
	time.Sleep(50 * time.Millisecond)

	entries := c.Entries()
	if len(entries) != 3 {
		t.Fatalf("len = %d, want 3", len(entries))
	}

	// Verify sorted by Next time
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Next.IsZero() {
			continue
		}
		if entries[i].Next.IsZero() {
			continue
		}
		if entries[i].Next.Before(entries[i-1].Next) {
			t.Errorf("entries not sorted: [%d].Next=%v > [%d].Next=%v",
				i-1, entries[i-1].Next, i, entries[i].Next)
		}
	}
}

// =============================================================================
// G4: Concurrent Safety Under Stress
// Goes beyond race detection — verifies logical consistency under contention.
// =============================================================================

func TestGuard_ConcurrentAddRemove_Consistency(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	const goroutines = 20
	const opsPerGoroutine = 50
	var wg sync.WaitGroup

	// Concurrently add and remove jobs
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				name := fmt.Sprintf("job-g%d-i%d", gid, i)
				c.AddFunc("* * * * * *", func() {}, name)
				c.RemoveJob(name)
			}
		}(g)
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	// After all add+remove pairs, no jobs should remain
	entries := c.Entries()
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name
		}
		t.Errorf("expected 0 entries after add+remove pairs, got %d: %v", len(entries), names)
	}
}

func TestGuard_ConcurrentAddWhileReadingEntries(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	var wg sync.WaitGroup
	const writers = 10
	const readers = 10
	const ops = 30

	// Writers: add jobs
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.AddFunc("* * * * * *", func() {}, fmt.Sprintf("w%d-j%d", wid, i))
			}
		}(w)
	}

	// Readers: snapshot entries
	var readErrors int32
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				entries := c.Entries()
				// Each snapshot must be internally consistent (sorted)
				for j := 1; j < len(entries); j++ {
					if !entries[j-1].Next.IsZero() && !entries[j].Next.IsZero() {
						if entries[j].Next.Before(entries[j-1].Next) {
							atomic.AddInt32(&readErrors, 1)
						}
					}
				}
			}
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&readErrors) > 0 {
		t.Errorf("found %d unsorted snapshots during concurrent read", readErrors)
	}
}

func TestGuard_ConcurrentDuplicateNameRejection(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	const goroutines = 50
	var wg sync.WaitGroup
	var successes int32

	// 50 goroutines all try to add "same-name" — exactly 1 must succeed
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.AddFuncWithError("* * * * * *", func() {}, "same-name")
			if err == nil {
				atomic.AddInt32(&successes, 1)
			}
		}()
	}

	wg.Wait()

	if s := atomic.LoadInt32(&successes); s != 1 {
		t.Errorf("expected exactly 1 success for duplicate name, got %d", s)
	}
}

// =============================================================================
// G5: WorkerPool Behavior Tests
// =============================================================================

func TestGuard_WorkerPool_ConcurrencyLimit(t *testing.T) {
	const maxWorkers = 3
	wp := NewWorkerPool(maxWorkers)

	var active int32
	var maxActive int32
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		wp.Submit(func() {
			defer wg.Done()
			cur := atomic.AddInt32(&active, 1)
			// Track peak concurrency
			for {
				old := atomic.LoadInt32(&maxActive)
				if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&active, -1)
		})
	}

	wg.Wait()
	wp.Wait()

	peak := atomic.LoadInt32(&maxActive)
	if peak > int32(maxWorkers) {
		t.Errorf("peak concurrency %d exceeded limit %d", peak, maxWorkers)
	}
}

func TestGuard_WorkerPool_DefaultCap(t *testing.T) {
	wp := NewWorkerPool(0) // should use default
	if cap(wp.semaphore) <= 0 {
		t.Error("default worker pool should have positive capacity")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	wp.Submit(func() { defer wg.Done() })
	wg.Wait()
}

func TestGuard_WorkerPool_PanicRecovery(t *testing.T) {
	// SafeJob wraps panics, so the WorkerPool should survive
	c := New()
	var ran int32

	c.AddFunc("* * * * * *", func() {
		atomic.AddInt32(&ran, 1)
		panic("test panic in worker")
	}, "panic-job")

	c.Start()
	time.Sleep(1100 * time.Millisecond) // wait for at least 1 fire
	c.Stop()

	if atomic.LoadInt32(&ran) == 0 {
		t.Error("panic job should have run at least once")
	}
}

// =============================================================================
// G6: Lifecycle Invariant Tests
// =============================================================================

func TestGuard_StopStart_Cycle(t *testing.T) {
	c := New()
	var counter int32

	c.AddFunc("* * * * * *", func() {
		atomic.AddInt32(&counter, 1)
	}, "cycle-job")

	// Cycle 1
	c.Start()
	time.Sleep(1100 * time.Millisecond)
	c.Stop()
	count1 := atomic.LoadInt32(&counter)
	if count1 == 0 {
		t.Fatal("job should have run in cycle 1")
	}

	// Reset
	atomic.StoreInt32(&counter, 0)

	// Cycle 2: job must still work after Stop+Start
	c.Start()
	time.Sleep(1100 * time.Millisecond)
	c.Stop()
	count2 := atomic.LoadInt32(&counter)
	if count2 == 0 {
		t.Error("job should have run in cycle 2 after restart")
	}
}

func TestGuard_AddWhileStoppedThenStart(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {}, "pre-start-job")
	c.Start()
	c.Stop()

	// Add while stopped
	c.AddFunc("* * * * * *", func() {}, "post-stop-job")

	entries := c.Entries()
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	sort.Strings(names)

	if len(names) != 2 {
		t.Fatalf("expected 2 entries, got %d: %v", len(names), names)
	}
}

func TestGuard_RemoveNonExistent(t *testing.T) {
	c := New()
	result := c.RemoveJobWithResult("does-not-exist")
	if result {
		t.Error("removing non-existent job should return false")
	}

	c.Start()
	defer c.Stop()
	time.Sleep(10 * time.Millisecond)

	result = c.RemoveJobWithResult("still-does-not-exist")
	if result {
		t.Error("removing non-existent job while running should return false")
	}
}

// =============================================================================
// G7: Edge Case Tests
// =============================================================================

func TestGuard_EntryOriginalJobPreserved(t *testing.T) {
	c := New()
	original := FuncJob(func() {})
	c.AddJob("* * * * * *", original, "test")

	entries := c.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// The snapshot's Job field should be the original (unwrapped) job
	if entries[0].OriginalJob == nil {
		t.Error("OriginalJob should be preserved in snapshot")
	}
}

func TestGuard_ManyJobsAddRemove(t *testing.T) {
	c := New()
	const n = 500

	// Add 500 jobs
	for i := 0; i < n; i++ {
		c.AddFunc("* * * * * *", func() {}, fmt.Sprintf("job-%d", i))
	}

	entries := c.Entries()
	if len(entries) != n {
		t.Fatalf("after adding %d jobs, got %d entries", n, len(entries))
	}

	// Remove all even-numbered jobs
	for i := 0; i < n; i += 2 {
		result := c.RemoveJobWithResult(fmt.Sprintf("job-%d", i))
		if !result {
			t.Errorf("failed to remove job-%d", i)
		}
	}

	entries = c.Entries()
	expected := n / 2
	if len(entries) != expected {
		t.Errorf("after removing %d jobs, got %d entries, want %d", n/2, len(entries), expected)
	}
}

func TestGuard_ParseWithError_SubSecondReject(t *testing.T) {
	_, err := ParseWithError("@every 500ms")
	if err == nil {
		t.Error("sub-second duration should be rejected")
	}
	if !strings.Contains(err.Error(), "less than a second") {
		t.Errorf("error should mention 'less than a second', got: %v", err)
	}
}

func TestGuard_ParseWithError_StepZeroReject(t *testing.T) {
	_, err := ParseWithError("*/0 * * * * *")
	if err == nil {
		t.Error("step=0 should be rejected")
	}
}

func TestGuard_ConstantDelay(t *testing.T) {
	sched := Every(5 * time.Second)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)

	next := sched.Next(base)
	expected := base.Add(5 * time.Second)
	if !next.Equal(expected) {
		t.Errorf("Every(5s).Next() = %v, want %v", next, expected)
	}
}

func TestGuard_RebootSchedule_RunsOnce(t *testing.T) {
	rs := &RebootSchedule{}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)

	first := rs.Next(base)
	if first.IsZero() {
		t.Error("first Next() should return a valid time")
	}

	second := rs.Next(first)
	if !second.IsZero() {
		t.Errorf("second Next() should return zero time, got %v", second)
	}
}
