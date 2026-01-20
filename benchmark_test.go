package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkAddJob 测试添加任务的性能
func BenchmarkAddJob(b *testing.B) {
	benchmarks := []struct {
		name     string
		jobCount int
	}{
		{"10jobs", 10},
		{"50jobs", 50},
		{"100jobs", 100},
		{"500jobs", 500},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c := New()
				for j := 0; j < bm.jobCount; j++ {
					c.AddFunc("* * * * * *", func() {}, fmt.Sprintf("job-%d", j))
				}
			}
		})
	}
}

// BenchmarkRemoveJob 测试删除任务的性能
func BenchmarkRemoveJob(b *testing.B) {
	benchmarks := []struct {
		name     string
		jobCount int
	}{
		{"10jobs", 10},
		{"50jobs", 50},
		{"100jobs", 100},
		{"500jobs", 500},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			
			// 准备测试数据
			c := New()
			for j := 0; j < bm.jobCount; j++ {
				c.AddFunc("* * * * * *", func() {}, fmt.Sprintf("job-%d", j))
			}
			
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				// 删除中间的任务
				jobName := fmt.Sprintf("job-%d", bm.jobCount/2)
				c.RemoveJob(jobName)
				// 重新添加以便下次测试
				c.AddFunc("* * * * * *", func() {}, jobName)
			}
		})
	}
}

// BenchmarkSchedulerLoop 测试调度循环的性能（模拟运行）
func BenchmarkSchedulerLoop(b *testing.B) {
	benchmarks := []struct {
		name     string
		jobCount int
	}{
		{"10jobs", 10},
		{"50jobs", 50},
		{"100jobs", 100},
		{"500jobs", 500},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			
			c := New()
			var counter int64
			
			// 添加任务
			for j := 0; j < bm.jobCount; j++ {
				c.AddFunc("* * * * * *", func() {
					atomic.AddInt64(&counter, 1)
				}, fmt.Sprintf("job-%d", j))
			}
			
			c.Start()
			defer c.Stop()
			
			b.ResetTimer()
			
			// 让调度器运行 N 次循环
			time.Sleep(time.Duration(b.N) * 100 * time.Millisecond)
		})
	}
}

// BenchmarkConcurrentAdd 测试并发添加任务
func BenchmarkConcurrentAdd(b *testing.B) {
	benchmarks := []struct {
		name        string
		goroutines  int
		jobsPerGo   int
	}{
		{"10goroutines_10jobs", 10, 10},
		{"50goroutines_10jobs", 50, 10},
		{"100goroutines_10jobs", 100, 10},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				c := New()
				c.Start()
				
				var wg sync.WaitGroup
				wg.Add(bm.goroutines)
				
				for g := 0; g < bm.goroutines; g++ {
					go func(gid int) {
						defer wg.Done()
						for j := 0; j < bm.jobsPerGo; j++ {
							c.AddFunc("* * * * * *", func() {}, 
								fmt.Sprintf("job-g%d-j%d", gid, j))
						}
					}(g)
				}
				
				wg.Wait()
				c.Stop()
			}
		})
	}
}

// BenchmarkEntries 测试获取任务列表的性能
func BenchmarkEntries(b *testing.B) {
	benchmarks := []struct {
		name     string
		jobCount int
	}{
		{"10jobs", 10},
		{"50jobs", 50},
		{"100jobs", 100},
		{"500jobs", 500},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			
			c := New()
			for j := 0; j < bm.jobCount; j++ {
				c.AddFunc("* * * * * *", func() {}, fmt.Sprintf("job-%d", j))
			}
			c.Start()
			defer c.Stop()
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = c.Entries()
			}
		})
	}
}

// BenchmarkJobExecution 测试任务执行的吞吐量
func BenchmarkJobExecution(b *testing.B) {
	benchmarks := []struct {
		name       string
		jobCount   int
		workerPool int
	}{
		{"10jobs_noLimit", 10, 0},
		{"10jobs_5workers", 10, 5},
		{"100jobs_noLimit", 100, 0},
		{"100jobs_50workers", 100, 50},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			
			var c *Cron
			if bm.workerPool > 0 {
				c = NewWithWorkerLimit(bm.workerPool)
			} else {
				c = New()
			}
			
			var counter int64
			
			// 添加每秒执行的任务
			for j := 0; j < bm.jobCount; j++ {
				c.AddFunc("* * * * * *", func() {
					atomic.AddInt64(&counter, 1)
					time.Sleep(10 * time.Millisecond) // 模拟工作
				}, fmt.Sprintf("job-%d", j))
			}
			
			c.Start()
			defer c.Stop()
			
			b.ResetTimer()
			
			// 运行 N 秒
			time.Sleep(time.Duration(b.N) * time.Second)
			
			b.ReportMetric(float64(atomic.LoadInt64(&counter))/float64(b.N), "jobs/sec")
		})
	}
}

// BenchmarkMemoryUsage 测试内存使用情况
func BenchmarkMemoryUsage(b *testing.B) {
	benchmarks := []struct {
		name     string
		jobCount int
	}{
		{"10jobs", 10},
		{"100jobs", 100},
		{"1000jobs", 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				c := New()
				for j := 0; j < bm.jobCount; j++ {
					c.AddFunc("* * * * * *", func() {}, fmt.Sprintf("job-%d", j))
				}
				c.Start()
				time.Sleep(100 * time.Millisecond)
				c.Stop()
			}
		})
	}
}
