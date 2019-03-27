package msgqueue_test

import (
	"fmt"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/memqueue"
)

type RateLimitError string

func (e RateLimitError) Error() string {
	return string(e)
}

func (RateLimitError) Delay() time.Duration {
	return 3 * time.Second
}

func Example_customRateLimit() {
	start := time.Now()
	q := memqueue.NewQueue(&msgqueue.QueueOptions{
		Handler: func() error {
			fmt.Println("retried in", timeSince(start))
			return RateLimitError("calm down")
		},
		RetryLimit: 2,
		MinBackoff: time.Millisecond,
	})

	q.Call()

	// Wait for all messages to be processed.
	_ = q.Close()

	// Output: retried in 0s
	// retried in 3s
}
