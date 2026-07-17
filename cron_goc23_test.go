package cron

import (
	"runtime"
	"sync/atomic"
	"testing"
)

// GOC-23: 旧实现每次 Submit 都无条件新建 goroutine、在 goroutine 内才抢信号量,
// 等待信号量的 goroutine 会无界堆积。现在 Submit 有准入上限(maxInFlight),
// 超过即丢弃并计数,in-flight goroutine 数被硬性限制。

// Submit 达到 in-flight 上限后必须丢弃并计数,而不是继续新建 goroutine。
// inFlight 计数在 Submit 内同步自增,因此本用例是确定性的,不依赖调度时序。
func TestGOC23_SubmitDropsWhenSaturated(t *testing.T) {
	// 并发上限 1,in-flight 上限 3。
	wp := NewWorkerPoolWithQueue(1, 3)

	block := make(chan struct{})
	// 3 个任务占满 in-flight(1 个执行、2 个等待信号量),全部阻塞在 block 上。
	for i := 0; i < 3; i++ {
		wp.Submit(func() { <-block })
	}
	if got := wp.Pending(); got != 3 {
		t.Fatalf("占满后 Pending 应为 3,实际 %d", got)
	}

	// 再提交 5 个:已达上限,必须全部被丢弃(其函数体不得执行)。
	for i := 0; i < 5; i++ {
		wp.Submit(func() { t.Error("被丢弃的任务不应执行") })
	}
	if got := wp.Dropped(); got != 5 {
		t.Fatalf("期望丢弃 5,实际 %d", got)
	}
	if got := wp.Pending(); got != 3 {
		t.Fatalf("丢弃不应改变 in-flight,Pending 应仍为 3,实际 %d", got)
	}
	if got := wp.Running(); got > 1 {
		t.Fatalf("并发执行数不得超过 1,实际 %d", got)
	}

	// 放行后 3 个被接收的任务全部完成,in-flight 归零,丢弃计数不变。
	close(block)
	wp.Wait()
	if got := wp.Pending(); got != 0 {
		t.Fatalf("全部完成后 Pending 应为 0,实际 %d", got)
	}
	if got := wp.Dropped(); got != 5 {
		t.Fatalf("完成后丢弃计数应仍为 5,实际 %d", got)
	}
}

// in-flight 上限之内的突发(远大于并发上限)必须全部执行、不丢弃——保持旧的
// "所有提交最终都会运行"语义,仅并发受限。
func TestGOC23_BurstWithinCapAllRun(t *testing.T) {
	wp := NewWorkerPoolWithQueue(4, 200)

	const jobs = 150 // > 并发上限,但 < in-flight 上限
	var ran int32
	for i := 0; i < jobs; i++ {
		wp.Submit(func() { atomic.AddInt32(&ran, 1) })
	}
	wp.Wait()

	if got := atomic.LoadInt32(&ran); got != jobs {
		t.Fatalf("上限内的突发应全部执行:期望 %d,实际 %d", jobs, got)
	}
	if got := wp.Dropped(); got != 0 {
		t.Fatalf("上限内不应丢弃,实际丢弃 %d", got)
	}
}

// 默认池:并发上限 = NumCPU*128,in-flight 上限 = 并发上限 * defaultQueueFactor。
func TestGOC23_DefaultCaps(t *testing.T) {
	wp := NewWorkerPool(0)
	wantConc := runtime.NumCPU() * 128
	if cap(wp.semaphore) != wantConc {
		t.Fatalf("默认并发上限应为 %d,实际 %d", wantConc, cap(wp.semaphore))
	}
	if int(wp.maxInFlight) != wantConc*defaultQueueFactor {
		t.Fatalf("默认 in-flight 上限应为 %d,实际 %d", wantConc*defaultQueueFactor, wp.maxInFlight)
	}
}

// GetMetrics 应暴露饱和度指标 DroppedJobs。
func TestGOC23_MetricsExposeDropped(t *testing.T) {
	c := NewWithWorkerLimit(1)
	// 直接压满其 worker pool 以产生丢弃(不依赖调度时序)。
	block := make(chan struct{})
	wp := c.workerPool
	limit := int(wp.maxInFlight)
	for i := 0; i < limit; i++ {
		wp.Submit(func() { <-block })
	}
	wp.Submit(func() { t.Error("应被丢弃") }) // 第 limit+1 个:丢弃

	if got := c.GetMetrics().DroppedJobs; got != 1 {
		t.Fatalf("GetMetrics().DroppedJobs 应为 1,实际 %d", got)
	}
	close(block)
	wp.Wait()
}
