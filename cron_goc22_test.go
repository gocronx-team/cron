package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// GOC-22: ScheduleWithError/RemoveJobWithResult 先无锁判定 isRunning、再走加锁直改
// 路径,与并发的 Start() 之间存在 TOCTOU。若在"判定为未运行"与"拿锁直改"之间
// run() 已启动,直改路径会与 run() 无锁读写 c.entries/c.entryIndex 构成数据竞争,
// 且该 entry 不会被 scheduleEntry 调度(静默不执行)。
//
// 本用例用 -race 覆盖竞争,并断言内部结构自洽(entries 切片与 entryIndex 完全一致)。
// 注意:这里不断言"每个报告成功的 add 都存在"——那会受 GOC-21(sync.Pool 复用请求对象、
// 结果通道脏读)影响而误判;GOC-21 是另一个独立问题。本用例只针对 GOC-22 的数据竞争与
// entries/entryIndex 一致性。
func TestGOC22_ConcurrentMutateDuringStartStop(t *testing.T) {
	c := New()

	const workers = 100

	// 后台持续 Start()/Stop(),制造 isRunning 检查与 run() 启动之间的竞态窗口。
	stopCycle := make(chan struct{})
	var cycler sync.WaitGroup
	cycler.Add(1)
	go func() {
		defer cycler.Done()
		for {
			select {
			case <-stopCycle:
				return
			default:
			}
			c.Start()
			c.Stop()
		}
	}()

	// 并发添加(唯一名),与 Start/Stop 交错——正是 GOC-22 的 TOCTOU 触发场景。
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = c.AddFuncWithError("0 0 1 1 *", func() {}, fmt.Sprintf("job-%d", id))
		}(i)
	}
	wg.Wait()

	close(stopCycle)
	cycler.Wait()
	c.Stop() // 确保最终停机,便于安全读取内部状态

	// GOC-22 不变式:数据竞争会破坏 entries/entryIndex 的一致性。停机后二者必须完全对应。
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != len(c.entryIndex) {
		t.Fatalf("entries(%d) 与 entryIndex(%d) 数量不一致(数据竞争破坏了结构)", len(c.entries), len(c.entryIndex))
	}
	for name := range c.entryIndex {
		found := false
		for _, e := range c.entries {
			if e.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s 在 entryIndex 里但不在 entries 切片中(索引/切片不一致)", name)
		}
	}
	for _, e := range c.entries {
		if _, ok := c.entryIndex[e.Name]; !ok {
			t.Errorf("%s 在 entries 切片里但不在 entryIndex 中(索引/切片不一致)", e.Name)
		}
	}
}

// GOC-22(补充): 在调度器运行期间添加的任务,必须真正被调度并执行——
// 而不是走进"直改却不调度"的坏路径导致静默不触发。
func TestGOC22_JobsAddedWhileRunningGetScheduled(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	const n = 10
	var counts [n]int32
	for i := 0; i < n; i++ {
		idx := i
		name := fmt.Sprintf("run-add-%d", idx)
		// 每秒触发
		if err := c.AddFuncWithError("* * * * * *", func() {
			atomic.AddInt32(&counts[idx], 1)
		}, name); err != nil {
			t.Fatalf("运行期间添加 %s 失败: %v", name, err)
		}
	}

	// 等待 ~2.5s,足够每个每秒任务至少执行一次。
	time.Sleep(2500 * time.Millisecond)

	for i := 0; i < n; i++ {
		if atomic.LoadInt32(&counts[i]) == 0 {
			t.Errorf("run-add-%d 在运行期间添加后从未执行(调度丢失)", i)
		}
	}
}
