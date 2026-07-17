package cron

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// GOC-21: 请求对象经 sync.Pool 复用,result 是容量 1 的带缓冲 channel。若调用方在
// <-done 分支返回,而 run() 已把结果写入 result,直接 Put 会把带残留值的对象放回池,
// 下一个复用者会脏读到上一次的结果。putAddReq/putRemoveReq 必须在归还前排空缓冲、清引用。

func TestGOC21_PutAddReqDrainsAndClears(t *testing.T) {
	req := &addRequest{result: make(chan error, 1)}
	req.entry = &Entry{Name: "stale"}
	// 模拟 run() 已写入结果、但调用方走了 <-done 分支未消费
	req.result <- errors.New("stale error")

	putAddReq(req)

	if len(req.result) != 0 {
		t.Errorf("putAddReq 未排空 result 缓冲,残留 %d 个值(下一个复用者会脏读)", len(req.result))
	}
	if req.entry != nil {
		t.Error("putAddReq 未清空 entry 引用(池会长期持有 Entry)")
	}
}

func TestGOC21_PutRemoveReqDrainsAndClears(t *testing.T) {
	req := &removeRequest{result: make(chan bool, 1)}
	req.name = "stale"
	req.result <- true // 模拟残留的"成功"结果

	putRemoveReq(req)

	if len(req.result) != 0 {
		t.Errorf("putRemoveReq 未排空 result 缓冲,残留 %d 个值", len(req.result))
	}
	if req.name != "" {
		t.Error("putRemoveReq 未清空 name")
	}
}

// 集成回归:并发添加(唯一名)与 Start/Stop 交错时,凡是 AddFuncWithError 返回 nil 的任务
// 都必须真实存在。GOC-21 修复前,脏读会让某些 add 读到陈旧的 nil 而误报成功(实则未添加)。
func TestGOC21_NoFalseSuccessUnderChurn(t *testing.T) {
	c := New()

	const workers = 100
	added := make([]int32, workers)

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

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := c.AddFuncWithError("0 0 1 1 *", func() {}, fmt.Sprintf("job-%d", id)); err == nil {
				atomic.StoreInt32(&added[id], 1)
			}
		}(i)
	}
	wg.Wait()

	close(stopCycle)
	cycler.Wait()
	c.Stop()

	c.mu.Lock()
	defer c.mu.Unlock()
	for id := 0; id < workers; id++ {
		if atomic.LoadInt32(&added[id]) != 1 {
			continue
		}
		name := fmt.Sprintf("job-%d", id)
		if _, ok := c.entryIndex[name]; !ok {
			t.Errorf("%s 报告添加成功,却不存在(脏读误报成功 / 静默丢失)", name)
		}
	}
}
