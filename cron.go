// This library implements a cron spec parser and runner.  See the README for
// more details.
package cron

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type entries []*Entry

// addRequest is an internal message sent to run() to add an entry.
type addRequest struct {
	entry  *Entry
	result chan error
}

// removeRequest is an internal message sent to run() to remove an entry.
type removeRequest struct {
	name   string
	result chan bool
}

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries    entries
	entryIndex map[string]*Entry // 优化: 添加索引Map加速查找
	stop       chan struct{}
	done       chan struct{} // closed when run() exits
	add        chan addRequest
	remove     chan removeRequest
	snapshot   chan entries
	fire       chan *Entry // timer callbacks send fired entries here
	running    int32       // atomic: 0=stopped, 1=running
	mu         sync.Mutex  // protects entries/entryIndex when not running
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

	// cancelled is set when the entry is removed; timer callback checks this
	cancelled bool
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

// isRunning returns whether the cron scheduler is currently running.
func (c *Cron) isRunning() bool {
	return atomic.LoadInt32(&c.running) == 1
}

// New returns a new Cron job runner.
func New() *Cron {
	return &Cron{
		entries:    nil,
		entryIndex: make(map[string]*Entry), // 优化: 初始化索引Map
		add:        make(chan addRequest),
		remove:     make(chan removeRequest),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		snapshot:   make(chan entries),
		fire:       make(chan *Entry, 64),
		running:    0,
		workerPool: NewWorkerPool(0), // Default cap: NumCPU * 128
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
	if !c.isRunning() {
		c.mu.Lock()
		defer c.mu.Unlock()

		// 优化: 使用索引Map快速查找
		entry, exists := c.entryIndex[name]
		if !exists {
			return false
		}

		// 标记取消并停止timer
		entry.cancelled = true
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

	req := removeRequest{name: name, result: make(chan bool, 1)}
	select {
	case c.remove <- req:
		select {
		case r := <-req.result:
			return r
		case <-c.done:
			return false
		}
	case <-c.done:
		return false
	}
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

	if !c.isRunning() {
		c.mu.Lock()
		defer c.mu.Unlock()

		// 优化: 使用索引Map快速检查重名
		if _, exists := c.entryIndex[name]; exists {
			return errors.New("cron: job name already exists")
		}
		c.entries = append(c.entries, entry)
		c.entryIndex[name] = entry // 优化: 添加到索引
		return nil
	}

	req := addRequest{entry: entry, result: make(chan error, 1)}
	select {
	case c.add <- req:
		select {
		case err := <-req.result:
			return err
		case <-c.done:
			return errors.New("cron: scheduler stopped")
		}
	case <-c.done:
		return errors.New("cron: scheduler stopped")
	}
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.isRunning() {
		select {
		case c.snapshot <- nil:
			select {
			case x := <-c.snapshot:
				return x
			case <-c.done:
				// run() exited, fall through
			}
		case <-c.done:
			// run() exited, fall through
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entrySnapshot()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.mu.Lock()
	if c.isRunning() {
		c.mu.Unlock()
		return
	}
	c.done = make(chan struct{})
	atomic.StoreInt32(&c.running, 1)
	c.mu.Unlock()
	go c.run()
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	c.mu.Lock()
	now := time.Now().Local()
	for _, entry := range c.entries {
		entry.cancelled = false // 重置: 确保 Stop()+Start() 后任务能正常执行
		entry.Next = entry.Schedule.Next(now)
		c.scheduleEntry(entry) // 优化: 为每个任务创建独立Timer
	}
	c.mu.Unlock()

	for {
		select {
		case req := <-c.add:
			// 优化: 使用索引Map检查重名
			if _, exists := c.entryIndex[req.entry.Name]; exists {
				req.result <- errors.New("cron: job name already exists")
				break
			}
			c.entries = append(c.entries, req.entry)
			c.entryIndex[req.entry.Name] = req.entry
			req.entry.Next = req.entry.Schedule.Next(time.Now().Local())
			c.scheduleEntry(req.entry) // 优化: 创建独立Timer
			req.result <- nil

		case req := <-c.remove:
			// 优化: 使用索引Map快速查找
			entry, exists := c.entryIndex[req.name]
			if !exists {
				req.result <- false
				break
			}

			// 标记取消并停止timer
			entry.cancelled = true
			if entry.timer != nil {
				entry.timer.Stop()
			}

			// 从数组中删除
			for i, e := range c.entries {
				if e.Name == req.name {
					c.entries = append(c.entries[:i], c.entries[i+1:]...)
					break
				}
			}

			// 从索引中删除
			delete(c.entryIndex, req.name)
			req.result <- true

		case e := <-c.fire:
			// 检查 entry 是否已被取消或不在索引中
			if e.cancelled {
				break
			}
			if _, exists := c.entryIndex[e.Name]; !exists {
				break
			}
			// 所有 Entry 修改都在 run() goroutine 中完成
			c.workerPool.Submit(e.Job.Run)
			e.Prev = e.Next
			e.Next = e.Schedule.Next(e.Next)
			c.scheduleEntry(e)

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			// 停止所有timer
			for _, entry := range c.entries {
				entry.cancelled = true
				if entry.timer != nil {
					entry.timer.Stop()
				}
			}
			atomic.StoreInt32(&c.running, 0)
			close(c.done)
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
		// Timer 回调只发信号，不做任何 Entry 修改
		select {
		case c.fire <- e:
		case <-c.done:
			// run() 已退出，丢弃
		}
	})
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	if !c.isRunning() {
		return
	}
	select {
	case c.stop <- struct{}{}:
	case <-c.done:
		return
	}
	<-c.done
}

// StopWithTimeout stops the cron scheduler with a timeout.
// Returns error if timeout is exceeded.
func (c *Cron) StopWithTimeout(timeout time.Duration) error {
	if !c.isRunning() {
		return nil
	}

	// 发送 stop 信号
	select {
	case c.stop <- struct{}{}:
	case <-c.done:
		return nil
	}

	// 等待 run() 退出
	select {
	case <-c.done:
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
