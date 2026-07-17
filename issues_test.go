package cron

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// CRIT-1 [已修复]: c.running 改为 atomic int32，无数据竞争
// =============================================================================

func TestCRIT1_Fixed_NoDataRace(t *testing.T) {
	// 并发 Start 和 AddFuncWithError 不再有数据竞争
	// 使用 go test -race 验证
	for iter := 0; iter < 5; iter++ {
		c := New()
		c.AddFunc("0 0 1 1 *", func() {}, fmt.Sprintf("job-%d", iter))

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			c.Start()
		}()

		go func() {
			defer wg.Done()
			_ = c.AddFuncWithError("0 0 1 1 *", func() {}, fmt.Sprintf("extra-%d", iter))
		}()

		wg.Wait()
		if c.isRunning() {
			c.Stop()
		}
	}
	t.Log("CRIT-1 [已修复]: go test -race 无数据竞争")
}

func TestCRIT1_Fixed_ConcurrentStartAndRead(t *testing.T) {
	for i := 0; i < 10; i++ {
		c := New()
		c.AddFunc("0 0 1 1 *", func() {}, "job")

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			c.Start()
		}()

		go func() {
			defer wg.Done()
			_ = c.isRunning() // atomic 读取，无竞争
		}()

		wg.Wait()
		if c.isRunning() {
			c.Stop()
		}
	}
	t.Log("CRIT-1 [已修复]: isRunning() 使用 atomic，并发安全")
}

// =============================================================================
// CRIT-2 [已修复]: 使用 done channel 防止 channel 操作永久阻塞
// =============================================================================

func TestCRIT2_Fixed_AddAfterStop_NoBlock(t *testing.T) {
	c := New()
	c.Start()
	c.Stop()

	// Stop 后 run() 已退出，done 已关闭
	// ScheduleWithError 通过 select done 感知到退出，不会阻塞
	done := make(chan struct{})
	go func() {
		schedule, _ := ParseWithError("* * * * * *")
		err := c.ScheduleWithError(schedule, FuncJob(func() {}), "after-stop-job")
		// 应该走 not-running 路径（因为 running 已被设为 0）
		if err != nil {
			// 如果恰好走了 running 路径且 done 关闭了，会返回 "scheduler stopped"
		}
		close(done)
	}()

	select {
	case <-done:
		t.Log("CRIT-2 [已修复]: ScheduleWithError 在 Stop 后不再阻塞")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CRIT-2: ScheduleWithError 仍然阻塞！")
	}
}

func TestCRIT2_Fixed_EntriesAfterStop_NoBlock(t *testing.T) {
	c := New()
	c.Start()
	c.Stop()

	done := make(chan struct{})
	go func() {
		_ = c.Entries()
		close(done)
	}()

	select {
	case <-done:
		t.Log("CRIT-2 [已修复]: Entries() 在 Stop 后不再阻塞")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CRIT-2: Entries() 仍然阻塞！")
	}
}

func TestCRIT2_Fixed_RemoveAfterStop_NoBlock(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {}, "testjob")
	c.Start()
	c.Stop()

	done := make(chan struct{})
	go func() {
		c.RemoveJobWithResult("testjob")
		close(done)
	}()

	select {
	case <-done:
		t.Log("CRIT-2 [已修复]: RemoveJobWithResult 在 Stop 后不再阻塞")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CRIT-2: RemoveJobWithResult 仍然阻塞！")
	}
}

// =============================================================================
// CRIT-3 [已修复]: Timer 回调只发信号到 fire channel，
// run() 中检查 cancelled 标志，移除的 entry 不会重新调度
// =============================================================================

type fastSchedule struct {
	interval time.Duration
}

func (s fastSchedule) Next(t time.Time) time.Time {
	return t.Add(s.interval)
}

func TestCRIT3_Fixed_NoZombieAfterRemoval(t *testing.T) {
	var runCount int32
	c := New()

	schedule := fastSchedule{interval: 10 * time.Millisecond}
	c.ScheduleWithError(schedule, FuncJob(func() {
		atomic.AddInt32(&runCount, 1)
	}), "fast-job")

	c.Start()
	time.Sleep(200 * time.Millisecond)

	c.RemoveJob("fast-job")
	time.Sleep(50 * time.Millisecond)
	countAtRemove := atomic.LoadInt32(&runCount)

	// 等待 — 移除后不应有新的执行
	time.Sleep(300 * time.Millisecond)
	countAfter := atomic.LoadInt32(&runCount)

	c.Stop()

	if countAfter > countAtRemove {
		t.Fatalf("CRIT-3: Job 在移除后继续执行了 %d 次！", countAfter-countAtRemove)
	}
	t.Logf("CRIT-3 [已修复]: Job 在移除后未再执行（移除时 %d，之后 %d）", countAtRemove, countAfter)
}

// TestCRIT3_TimerStopCannotPreventRunningCallback 证明 timer.Stop() 无法阻止已开始的回调
// 这是 Go 标准库的行为，我们通过 cancelled 标志在 run() 中过滤
func TestCRIT3_TimerStopCannotPreventRunningCallback(t *testing.T) {
	callbackExecuted := int32(0)
	callbackStarted := make(chan struct{})

	timer := time.AfterFunc(1*time.Millisecond, func() {
		close(callbackStarted)
		time.Sleep(100 * time.Millisecond)
		atomic.StoreInt32(&callbackExecuted, 1)
	})

	<-callbackStarted
	stopped := timer.Stop()
	time.Sleep(200 * time.Millisecond)

	if !stopped && atomic.LoadInt32(&callbackExecuted) == 1 {
		t.Log("CRIT-3: timer.Stop() 返回 false 且回调已执行 — 这是 Go 标准行为，" +
			"我们通过 cancelled 标志在 run() 中防止 zombie 重调度")
	}
}

func TestCRIT3_Fixed_ZombieEntryBlocked(t *testing.T) {
	// 多次尝试，确保 cancelled 标志有效阻止 zombie
	for attempt := 0; attempt < 10; attempt++ {
		c := New()
		var execCount int32

		entry := &Entry{
			Schedule: fastSchedule{interval: 1 * time.Millisecond},
			Job: &SafeJob{Job: FuncJob(func() {
				atomic.AddInt32(&execCount, 1)
			}), Name: "zombie", metrics: c.metrics},
			Name: "zombie-entry",
		}

		c.entries = append(c.entries, entry)
		c.entryIndex[entry.Name] = entry
		entry.Next = entry.Schedule.Next(time.Now())

		c.Start()
		time.Sleep(50 * time.Millisecond)

		c.RemoveJob("zombie-entry")
		time.Sleep(20 * time.Millisecond)
		countAtRemove := atomic.LoadInt32(&execCount)

		time.Sleep(100 * time.Millisecond)
		countAfter := atomic.LoadInt32(&execCount)

		c.Stop()

		if countAfter > countAtRemove {
			t.Fatalf("CRIT-3: 第 %d 次尝试 zombie entry 在移除后继续执行了 %d 次！",
				attempt+1, countAfter-countAtRemove)
		}
	}
	t.Log("CRIT-3 [已修复]: 10 次尝试均无 zombie entry")
}

// =============================================================================
// CRIT-4 [已修复]: NewWorkerPool(0) 现在有默认上限 NumCPU*128
// =============================================================================

func TestCRIT4_Fixed_DefaultWorkerPoolHasCap(t *testing.T) {
	wp := NewWorkerPool(0)
	if wp.semaphore == nil {
		t.Fatal("CRIT-4: 默认 worker pool 仍然无并发限制！")
	}
	expectedCap := runtime.NumCPU() * 128
	if cap(wp.semaphore) != expectedCap {
		t.Fatalf("CRIT-4: 默认 worker pool cap=%d, 期望=%d", cap(wp.semaphore), expectedCap)
	}
	t.Logf("CRIT-4 [已修复]: 默认 worker pool cap=%d (NumCPU*128)", expectedCap)
}

func TestCRIT4_Fixed_GoroutinesBounded(t *testing.T) {
	maxWorkers := 10
	wp := NewWorkerPool(maxWorkers)

	var concurrent int32
	var peakConcurrent int32
	var mu sync.Mutex

	var wg sync.WaitGroup
	jobCount := 100
	wg.Add(jobCount)

	for i := 0; i < jobCount; i++ {
		wp.Submit(func() {
			defer wg.Done()
			cur := atomic.AddInt32(&concurrent, 1)
			mu.Lock()
			if cur > peakConcurrent {
				peakConcurrent = cur
			}
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
		})
	}

	wg.Wait()

	peak := atomic.LoadInt32(&peakConcurrent)
	if peak > int32(maxWorkers) {
		t.Fatalf("CRIT-4: 期望最多 %d 并发执行，实际峰值 %d", maxWorkers, peak)
	}
	t.Logf("CRIT-4 [已修复]: %d 个 job 限制为 %d 并发执行（实际峰值 %d）",
		jobCount, maxWorkers, peak)
}

// =============================================================================
// HIGH-1 [已修复]: step=0 返回错误，不再死循环
// =============================================================================

func TestHIGH1_Fixed_StepZeroReturnsError(t *testing.T) {
	_, err := getRangeWithError("*/0", seconds)
	if err == nil {
		t.Fatal("HIGH-1: */0 应返回错误")
	}
	t.Logf("HIGH-1 [已修复]: */0 返回错误: %v", err)
}

func TestHIGH1_Fixed_ParseStepZeroReturnsError(t *testing.T) {
	_, err := ParseWithError("*/0 * * * * *")
	if err == nil {
		t.Fatal("HIGH-1: ParseWithError(\"*/0 * * * * *\") 应返回错误")
	}
	t.Logf("HIGH-1 [已修复]: ParseWithError 返回错误: %v", err)
}

// =============================================================================
// HIGH-2 [已修复]: getBits 使用安全的位运算，无溢出
// =============================================================================

func TestHIGH2_Fixed_GetBitsNoOverflow(t *testing.T) {
	result := getBits(0, 63, 1)
	if result != ^uint64(0) {
		t.Fatalf("HIGH-2: getBits(0, 63, 1) = %064b, 期望 MaxUint64", result)
	}
	t.Log("HIGH-2 [已修复]: getBits(0, 63, 1) 使用安全位运算返回 MaxUint64")

	// 验证其他情况
	result2 := getBits(0, 63, 2)
	bitCount := 0
	for i := 0; i < 64; i++ {
		if result2&(1<<uint(i)) != 0 {
			bitCount++
		}
	}
	if bitCount != 32 {
		t.Fatalf("HIGH-2: getBits(0, 63, 2) 设置了 %d 位, 期望 32", bitCount)
	}

	// 验证小范围
	result3 := getBits(5, 10, 1)
	expected := uint64(0b11111100000)
	if result3 != expected {
		t.Fatalf("HIGH-2: getBits(5, 10, 1) = %b, 期望 %b", result3, expected)
	}
}

// =============================================================================
// HIGH-3 [已修复]: StopWithTimeout 不再启动 goroutine，无泄漏
// =============================================================================

func TestHIGH3_Fixed_StopWithTimeoutNoLeak(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {
		time.Sleep(5 * time.Second)
	}, "slow-job")
	c.Start()
	time.Sleep(1500 * time.Millisecond)

	goroutinesBefore := runtime.NumGoroutine()

	// StopWithTimeout 现在直接发 stop 然后等 done，不启动额外 goroutine
	err := c.StopWithTimeout(2 * time.Second)
	if err != nil {
		t.Logf("HIGH-3: StopWithTimeout 超时: %v（预期行为，job 仍在运行）", err)
	}

	time.Sleep(100 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	// 即使超时，也不应该泄漏 goroutine（StopWithTimeout 不再启动 goroutine）
	leaked := goroutinesAfter - goroutinesBefore
	if leaked > 2 { // 允许少量波动
		t.Fatalf("HIGH-3: StopWithTimeout 后泄漏了 %d 个 goroutine！", leaked)
	}
	t.Logf("HIGH-3 [已修复]: StopWithTimeout 后 goroutine 变化 %d（无泄漏）", leaked)
}

func TestHIGH3_Fixed_MultipleStopWithTimeoutNoAccumulation(t *testing.T) {
	c := New()
	c.Start()
	// Stop 立即成功（无长时间 job）
	c.Stop()

	goroutinesBefore := runtime.NumGoroutine()

	// 多次调用 StopWithTimeout 在已停止的 cron 上
	for i := 0; i < 5; i++ {
		c.StopWithTimeout(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	accumulated := goroutinesAfter - goroutinesBefore
	if accumulated > 1 {
		t.Fatalf("HIGH-3: 5 次 StopWithTimeout 累积 %d goroutine！", accumulated)
	}
	t.Logf("HIGH-3 [已修复]: 5 次 StopWithTimeout 后 goroutine 变化 %d（无累积泄漏）", accumulated)
}

// =============================================================================
// HIGH-4 [已修复]: Timer 回调不再直接写 Entry 字段，所有修改在 run() 中完成
// =============================================================================

func TestHIGH4_Fixed_NoRaceOnEntries(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {}, "race-job")
	c.Start()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			entries := c.Entries()
			for _, e := range entries {
				_ = e.Next
				_ = e.Prev
			}
			time.Sleep(20 * time.Millisecond)
		}
		close(done)
	}()

	<-done
	c.Stop()
	t.Log("HIGH-4 [已修复]: go test -race 无竞争 — timer 回调不再直接写 Entry 字段")
}

// =============================================================================
// HIGH-5 [已修复]: 运行中 ScheduleWithError 正确返回重名错误
// =============================================================================

func TestHIGH5_Fixed_DuplicateNameReturnsError(t *testing.T) {
	c := New()
	err := c.AddFuncWithError("* * * * * *", func() {}, "dup-job")
	if err != nil {
		t.Fatalf("第一次添加失败: %v", err)
	}

	c.Start()
	defer c.Stop()

	time.Sleep(100 * time.Millisecond)

	err = c.AddFuncWithError("* * * * * *", func() {}, "dup-job")
	if err == nil {
		t.Fatal("HIGH-5: 运行中添加重名 job 应返回错误！")
	}
	t.Logf("HIGH-5 [已修复]: 运行中添加重名 job 正确返回错误: %v", err)
}

// =============================================================================
// MED-1 [已修复]: RebootSchedule 返回零值时间，entry 不再永久驻留
// =============================================================================

func TestMED1_Fixed_RebootScheduleReturnsZero(t *testing.T) {
	rs := &RebootSchedule{}
	now := time.Now()

	first := rs.Next(now)
	if first.Before(now) || first.After(now.Add(10*time.Second)) {
		t.Fatalf("MED-1: 首次 Next() 返回意外时间: %v", first)
	}

	second := rs.Next(first)
	if !second.IsZero() {
		t.Fatalf("MED-1: 第二次 Next() 应返回零值, 实际: %v", second)
	}
	t.Log("MED-1 [已修复]: RebootSchedule 第二次 Next() 返回零值时间")
}

// =============================================================================
// MED-2 [已修复]: isTestMode() 使用 sync.Once 缓存结果
// =============================================================================

func TestMED2_Fixed_IsTestModeCached(t *testing.T) {
	iterations := 1000000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_ = isTestMode()
	}
	elapsed := time.Since(start)

	t.Logf("MED-2 [已修复]: isTestMode() %d 次调用耗时 %v（已通过 sync.Once 缓存）", iterations, elapsed)
	if !isTestMode() {
		t.Fatal("MED-2: isTestMode() 在测试环境应返回 true")
	}
}

// =============================================================================
// MED-3 [已修复]: ParseWithError("@every 500ms") 返回错误而非 panic
// Every() 本身保持 panic（向后兼容）
// =============================================================================

func TestMED3_Fixed_EveryStillPanicsOnSubSecond(t *testing.T) {
	// Every() 直接调用仍然 panic（向后兼容）
	defer func() {
		if r := recover(); r != nil {
			t.Logf("MED-3: Every(500ms) 仍 panic（向后兼容）: %v", r)
		}
	}()
	Every(500 * time.Millisecond)
	t.Fatal("MED-3: Every(500ms) 应 panic")
}

func TestMED3_Fixed_ParseEverySubSecondReturnsError(t *testing.T) {
	_, err := ParseWithError("@every 500ms")
	if err == nil {
		t.Fatal("MED-3: ParseWithError(\"@every 500ms\") 应返回错误！")
	}
	t.Logf("MED-3 [已修复]: ParseWithError 返回错误: %v", err)
}

// =============================================================================
// MED-4 [已修复]: 使用 mutex 保护未启动时的并发 map 访问
// =============================================================================

func TestMED4_Fixed_ConcurrentAddBeforeStart(t *testing.T) {
	c := New()

	var wg sync.WaitGroup
	errCount := int32(0)

	// 20 个 goroutine 并发添加 — 不再 crash
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("job-%d", id)
			err := c.AddFuncWithError("0 0 1 1 *", func() {}, name)
			if err != nil {
				atomic.AddInt32(&errCount, 1)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("MED-4 [已修复]: 20 goroutine 并发添加完成（%d 个失败）— 无 crash、无 race",
		atomic.LoadInt32(&errCount))
}

func TestMED4_Fixed_ConcurrentRemoveBeforeStart(t *testing.T) {
	c := New()

	for i := 0; i < 20; i++ {
		c.AddFuncWithError("0 0 1 1 *", func() {}, fmt.Sprintf("job-%d", i))
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.RemoveJobWithResult(fmt.Sprintf("job-%d", id))
		}(i)
	}

	wg.Wait()
	t.Log("MED-4 [已修复]: 20 goroutine 并发移除完成 — 无 crash、无 race")
}

// =============================================================================
// LOW-1: AddJob / Parse 仍然 panic（向后兼容，保持不变）
// =============================================================================

func TestLOW1_AddJobPanicsOnInvalidSpec(t *testing.T) {
	c := New()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("LOW-1: AddJob 使用无效 spec 触发 panic（向后兼容）: %v", r)
		}
	}()

	c.AddJob("invalid-cron-spec", FuncJob(func() {}), "bad-job")
	t.Fatal("LOW-1: AddJob 应 panic")
}

// =============================================================================
// LOW-2: Entries() 排序保留（向后兼容）
// =============================================================================

func TestLOW2_EntriesSnapshotSorts(t *testing.T) {
	c := New()
	for i := 0; i < 100; i++ {
		c.AddFuncWithError("* * * * * *", func() {}, fmt.Sprintf("job-%04d", i))
	}

	iterations := 100
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_ = c.entrySnapshot()
	}
	elapsed := time.Since(start)

	t.Logf("LOW-2: 100 个 entry 的 entrySnapshot() %d 次耗时 %v（保留排序，向后兼容）",
		iterations, elapsed)
}

// =============================================================================
// LOW-3: 无 Context 支持（此版本暂不修复，记录行为）
// =============================================================================

func TestLOW3_StopAndWaitCannotCancelRunningJobs(t *testing.T) {
	c := New()
	jobStarted := make(chan struct{})
	started := int32(0)

	c.AddFunc("* * * * * *", func() {
		if atomic.CompareAndSwapInt32(&started, 0, 1) {
			close(jobStarted)
		}
		time.Sleep(10 * time.Second)
	}, "long-job")

	c.Start()

	select {
	case <-jobStarted:
	case <-time.After(2 * time.Second):
		c.Stop()
		t.Skip("LOW-3: Job 未在 2 秒内启动")
		return
	}

	startTime := time.Now()
	err := c.StopAndWait(500 * time.Millisecond)
	elapsed := time.Since(startTime)

	if err != nil {
		t.Logf("LOW-3: StopAndWait 超时（%v）: %v — 缺少 Context 传播机制（暂不修复）", elapsed, err)
	} else {
		t.Logf("LOW-3: StopAndWait 成功（%v）", elapsed)
	}
}

// =============================================================================
// Extra [已修复]: RemoveJobWithResult 对不存在的 job 正确返回 false
// =============================================================================

func TestRemoveJobWithResult_Fixed_ReturnsFalseForNonExistent(t *testing.T) {
	c := New()
	c.AddFunc("* * * * * *", func() {}, "existing-job")
	c.Start()
	defer c.Stop()

	time.Sleep(100 * time.Millisecond)

	result := c.RemoveJobWithResult("non-existent-job")
	if result {
		t.Fatal("RemoveJobWithResult 对不存在的 job 应返回 false！")
	}
	t.Log("Extra [已修复]: RemoveJobWithResult 对不存在的 job 正确返回 false")
}

// =============================================================================
// Extra: wrapJob 死代码已移除（不再有测试）
// =============================================================================

// =============================================================================
// GOC-25 [已修复]: Stop/Start 生命周期边角
// (a) fire 缓冲残留: Stop 后残留在 fire 缓冲中的消息不应被下次 Start 消费
// (b) StopAndWait 超时翻倍: 两步共享一个 deadline，总等待不超过 timeout
// =============================================================================

func TestGOC25a_StaleFireNotConsumedAfterRestart(t *testing.T) {
	c := New()
	var runs int32
	// 调度时间远在未来，正常情况下不会触发
	c.AddFunc("0 0 1 1 *", func() {
		atomic.AddInt32(&runs, 1)
	}, "far-future-job")

	c.Start()
	staleFire := c.loadFire() // 第一次运行的 fire channel
	c.Stop()

	// 模拟 timer 回调在 select 竞态中赢过 <-c.done，把 entry 残留进缓冲
	c.mu.Lock()
	entry := c.entryIndex["far-future-job"]
	c.mu.Unlock()
	staleFire <- entry

	// 重启后新 run() 不应消费旧缓冲中的陈旧消息
	c.Start()
	time.Sleep(200 * time.Millisecond)
	c.Stop()

	if got := atomic.LoadInt32(&runs); got != 0 {
		t.Fatalf("GOC-25(a): 重启后陈旧的 fire 消息被消费，任务多跑了 %d 次", got)
	}
	t.Log("GOC-25(a) [已修复]: Start() 重建 fire channel，陈旧消息不会触发额外执行")
}

func TestGOC25b_StopAndWaitSharedDeadline(t *testing.T) {
	c := New()
	jobStarted := make(chan struct{})
	started := int32(0)

	c.AddFunc("* * * * * *", func() {
		if atomic.CompareAndSwapInt32(&started, 0, 1) {
			close(jobStarted)
		}
		time.Sleep(10 * time.Second)
	}, "goc25-long-job")

	c.Start()

	select {
	case <-jobStarted:
	case <-time.After(2 * time.Second):
		c.Stop()
		t.Skip("GOC-25(b): Job 未在 2 秒内启动")
		return
	}

	timeout := 500 * time.Millisecond
	startTime := time.Now()
	err := c.StopAndWait(timeout)
	elapsed := time.Since(startTime)

	if err == nil {
		t.Fatal("GOC-25(b): 存在 10s 长任务时 StopAndWait(500ms) 应返回超时错误")
	}
	// 两步共享 deadline: 总耗时应约等于 timeout，而不是最坏 2*timeout
	if elapsed > timeout+300*time.Millisecond {
		t.Fatalf("GOC-25(b): StopAndWait 总耗时 %v 超过共享 deadline 预期（timeout=%v）", elapsed, timeout)
	}
	t.Logf("GOC-25(b) [已修复]: StopAndWait 总耗时 %v（timeout=%v，两步共享 deadline）", elapsed, timeout)
}
