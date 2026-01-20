// This library implements a cron spec parser and runner.  See the README for
// more details.
package cron

import (
	"errors"
	"sort"
	"sync/atomic"
	"time"
)

type entries []*Entry

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries    entries
	entryIndex map[string]*Entry // 优化: 添加索引Map加速查找
	stop       chan struct{}
	add        chan *Entry
	remove     chan string
	snapshot   chan entries
	running    bool
	workerPool *WorkerPool
	metrics    *Metrics
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run (wrapped with SafeJob).
	Job Job

	// Unique name to identify the Entry so as to be able to remove it later.
	Name string

	// OriginalJob stores the unwrapped job for backward compatibility
	OriginalJob Job

	// timer holds the timer for this entry (优化: 独立Timer)
	timer *time.Timer
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner.
func New() *Cron {
	return &Cron{
		entries:    nil,
		entryIndex: make(map[string]*Entry), // 优化: 初始化索引Map
		add:        make(chan *Entry),
		remove:     make(chan string),
		stop:       make(chan struct{}),
		snapshot:   make(chan entries),
		running:    false,
		workerPool: NewWorkerPool(0), // No limit by default
		metrics:    &Metrics{},
	}
}

// NewWithWorkerLimit returns a new Cron job runner with worker limit.
func NewWithWorkerLimit(maxWorkers int) *Cron {
	c := New()
	c.workerPool = NewWorkerPool(maxWorkers)
	return c
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func(), name string) {
	c.AddJob(spec, FuncJob(cmd), name)
}

// AddFuncWithError adds a func to the Cron to be run on the given schedule.
// Returns error if spec is invalid or name already exists.
func (c *Cron) AddFuncWithError(spec string, cmd func(), name string) error {
	return c.AddJobWithError(spec, FuncJob(cmd), name)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job, name string) {
	c.Schedule(Parse(spec), cmd, name)
}

// AddJobWithError adds a Job to the Cron to be run on the given schedule.
// Returns error if spec is invalid or name already exists.
func (c *Cron) AddJobWithError(spec string, cmd Job, name string) error {
	schedule, err := ParseWithError(spec)
	if err != nil {
		return err
	}
	return c.ScheduleWithError(schedule, cmd, name)
}

// RemoveJob removes a Job from the Cron based on name.
func (c *Cron) RemoveJob(name string) {
	c.RemoveJobWithResult(name)
}

// RemoveJobWithResult removes a Job from the Cron based on name.
// Returns true if the job was found and removed, false otherwise.
func (c *Cron) RemoveJobWithResult(name string) bool {
	if !c.running {
		// 优化: 使用索引Map快速查找
		entry, exists := c.entryIndex[name]
		if !exists {
			return false
		}

		// 停止timer
		if entry.timer != nil {
			entry.timer.Stop()
		}

		// 从数组中删除
		for i, e := range c.entries {
			if e.Name == name {
				c.entries = append(c.entries[:i], c.entries[i+1:]...)
				break
			}
		}

		// 从索引中删除
		delete(c.entryIndex, name)
		return true
	}

	c.remove <- name
	return true // Assume success for running cron
}

func (entrySlice entries) pos(name string) int {
	for p, e := range entrySlice {
		if e.Name == name {
			return p
		}
	}
	return -1
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, name string) {
	c.ScheduleWithError(schedule, cmd, name)
}

// ScheduleWithError adds a Job to the Cron to be run on the given schedule.
// Returns error if name already exists.
func (c *Cron) ScheduleWithError(schedule Schedule, cmd Job, name string) error {
	entry := &Entry{
		Schedule:    schedule,
		Job:         wrapJobWithMetrics(cmd, name, c.metrics),
		OriginalJob: cmd,
		Name:        name,
	}

	if !c.running {
		// 优化: 使用索引Map快速检查重名
		if _, exists := c.entryIndex[name]; exists {
			return errors.New("cron: job name already exists")
		}
		c.entries = append(c.entries, entry)
		c.entryIndex[name] = entry // 优化: 添加到索引
		return nil
	}

	c.add <- entry
	return nil
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.running = true
	go c.run()
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := time.Now().Local()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.scheduleEntry(entry) // 优化: 为每个任务创建独立Timer
	}

	for {
		select {
		case newEntry := <-c.add:
			// 优化: 使用索引Map检查重名
			if _, exists := c.entryIndex[newEntry.Name]; exists {
				break
			}
			c.entries = append(c.entries, newEntry)
			c.entryIndex[newEntry.Name] = newEntry
			newEntry.Next = newEntry.Schedule.Next(time.Now().Local())
			c.scheduleEntry(newEntry) // 优化: 创建独立Timer

		case name := <-c.remove:
			// 优化: 使用索引Map快速查找
			entry, exists := c.entryIndex[name]
			if !exists {
				break
			}

			// 停止timer
			if entry.timer != nil {
				entry.timer.Stop()
			}

			// 从数组中删除
			for i, e := range c.entries {
				if e.Name == name {
					c.entries = append(c.entries[:i], c.entries[i+1:]...)
					break
				}
			}

			// 从索引中删除
			delete(c.entryIndex, name)

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			// 停止所有timer
			for _, entry := range c.entries {
				if entry.timer != nil {
					entry.timer.Stop()
				}
			}
			return
		}
	}
}

// scheduleEntry creates a timer for the entry (优化: 独立Timer调度)
func (c *Cron) scheduleEntry(e *Entry) {
	if e.Next.IsZero() {
		return
	}

	// 停止旧timer
	if e.timer != nil {
		e.timer.Stop()
	}

	// 创建新timer
	duration := e.Next.Sub(time.Now().Local())
	if duration < 0 {
		duration = 0
	}

	e.timer = time.AfterFunc(duration, func() {
		// 执行任务
		c.workerPool.Submit(e.Job.Run)
		e.Prev = e.Next
		e.Next = e.Schedule.Next(e.Next)

		// 重新调度
		c.scheduleEntry(e)
	})
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	if !c.running {
		return
	}
	c.stop <- struct{}{}
	c.running = false
}

// StopWithTimeout stops the cron scheduler with a timeout.
// Returns error if timeout is exceeded.
func (c *Cron) StopWithTimeout(timeout time.Duration) error {
	if !c.running {
		return nil
	}

	done := make(chan struct{})
	go func() {
		c.stop <- struct{}{}
		close(done)
	}()

	select {
	case <-done:
		c.running = false
		return nil
	case <-time.After(timeout):
		return errors.New("cron: stop timeout exceeded")
	}
}

// StopAndWait stops the cron scheduler and waits for all running jobs to complete.
// Returns error if timeout is exceeded.
func (c *Cron) StopAndWait(timeout time.Duration) error {
	if err := c.StopWithTimeout(timeout); err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		c.workerPool.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("cron: wait timeout exceeded")
	}
}

// GetMetrics returns the current metrics.
func (c *Cron) GetMetrics() Metrics {
	return Metrics{
		TotalRuns:   atomic.LoadInt64(&c.metrics.TotalRuns),
		TotalPanics: atomic.LoadInt64(&c.metrics.TotalPanics),
		ActiveJobs:  atomic.LoadInt32(&c.metrics.ActiveJobs),
	}
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	// 优化: 预分配切片容量
	entries := make([]*Entry, 0, len(c.entries))
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule:    e.Schedule,
			Next:        e.Next,
			Prev:        e.Prev,
			Job:         e.OriginalJob,
			OriginalJob: e.OriginalJob,
			Name:        e.Name,
		})
	}
	// 保持向后兼容: 使用原有的排序方式
	sort.Sort(byTime(entries))
	return entries
}
