package taskq_test

import (
	"sync"
	"testing"

	"github.com/vmihailenco/taskq"
	"github.com/vmihailenco/taskq/memqueue"
	"github.com/vmihailenco/taskq/redisq"
)

func BenchmarkConsumerMemq(b *testing.B) {
	benchmarkConsumer(b, memqueue.NewFactory())
}

func BenchmarkConsumerRedisq(b *testing.B) {
	benchmarkConsumer(b, redisq.NewFactory())
}

var (
	once sync.Once
	task *taskq.Task
	wg   sync.WaitGroup
)

func benchmarkConsumer(b *testing.B, factory taskq.Factory) {
	once.Do(func() {
		q := factory.NewQueue(&taskq.QueueOptions{
			Name:  "bench",
			Redis: redisRing(),
		})

		task = q.NewTask(&taskq.TaskOptions{
			Name: "bench",
			Handler: func() {
				wg.Done()
			},
		})

		_ = q.Consumer().Start()
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			_ = task.Call()
		}
		wg.Wait()
	}
}
