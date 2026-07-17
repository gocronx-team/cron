package cron

import (
	"sync/atomic"
	"testing"
	"time"
)

// GOC-20: fire 分支原先用"上一次计划时间"推算下一次(Next(e.Next))。进程冻结
// (休眠/容器暂停/长 GC-STW)恢复后,错过的触发点会因 scheduleEntry 把负 duration
// 钳为 0 而背靠背连续补跑——追赶风暴。修复后从 now 推算,跳过错过的触发。
func TestGOC20_MissedFiresSkippedNoCatchUpStorm(t *testing.T) {
	c := New()

	var runs int32
	e := &Entry{
		Schedule: ConstantDelaySchedule{Delay: time.Second},
		Name:     "every-1s",
		Job:      FuncJob(func() { atomic.AddInt32(&runs, 1) }),
	}
	// 模拟进程冻结约 60s:上一次计划时间远在过去。
	past := time.Now().Add(-60 * time.Second)
	e.Next = past

	now := time.Now()
	c.advanceAndReschedule(e, now)

	// 关键不变式:下一次触发必须落在未来(跳过错过的 ~60 次),
	// 而不是停留在过去 → duration 被钳为 0 → 立即反复补跑。
	if !e.Next.After(now) {
		t.Fatalf("追赶风暴未修复:下一次触发 %v 不在 now %v 之后", e.Next, now)
	}
	// 且应落在下一个间隔内(约 1s),而非累积到过去。
	if d := e.Next.Sub(now); d <= 0 || d > 2*time.Second {
		t.Fatalf("下一次触发时延异常:%v(应约为一个间隔)", d)
	}
	// Prev 记录本次(错过的)计划时间。
	if !e.Prev.Equal(past) {
		t.Errorf("Prev 应记录本次计划时间 %v,实际 %v", past, e.Prev)
	}

	// 单次 fire 只提交一次任务。
	c.workerPool.Wait()
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("单次 fire 应只执行一次,实际 %d", got)
	}
	if e.timer != nil {
		e.timer.Stop()
	}

	// 前提说明:旧逻辑 Next(past) 仍在过去,正是它导致 0 delay 风暴。
	if e.Schedule.Next(past).After(now) {
		t.Fatal("测试前提失效:Next(past) 不应在 now 之后")
	}
}

// 正常节奏不受影响:每秒任务在 ~2.5s 内应触发 2~3 次,而非因风暴狂跑。
func TestGOC20_NormalCadenceUnaffected(t *testing.T) {
	c := New()
	var runs int32
	if err := c.AddFuncWithError("@every 1s", func() { atomic.AddInt32(&runs, 1) }, "cadence"); err != nil {
		t.Fatalf("add: %v", err)
	}
	c.Start()
	time.Sleep(2500 * time.Millisecond)
	c.Stop()

	got := atomic.LoadInt32(&runs)
	if got < 1 || got > 5 {
		t.Fatalf("每秒任务 2.5s 内应触发约 2~3 次,实际 %d(过多可能是追赶风暴)", got)
	}
}
