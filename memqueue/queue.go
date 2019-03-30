package memqueue

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal"
	"github.com/go-msgqueue/msgqueue/internal/base"
)

type manager struct{}

var _ msgqueue.Manager = (*manager)(nil)

func (manager) NewQueue(opt *msgqueue.QueueOptions) msgqueue.Queue {
	return NewQueue(opt)
}

func (manager) Queues() []msgqueue.Queue {
	var queues []msgqueue.Queue
	for _, q := range Queues() {
		queues = append(queues, q)
	}
	return queues
}

func NewManager() msgqueue.Manager {
	return manager{}
}

type Queue struct {
	base base.Queue

	opt *msgqueue.QueueOptions

	sync    bool
	noDelay bool

	wg sync.WaitGroup
	p  *msgqueue.Processor
}

var _ msgqueue.Queue = (*Queue)(nil)

func NewQueue(opt *msgqueue.QueueOptions) *Queue {
	opt.Init()

	q := Queue{
		opt: opt,
	}
	q.p = msgqueue.NewProcessor(&q)
	if err := q.p.Start(); err != nil {
		panic(err)
	}

	registerQueue(&q)
	return &q
}

func (q *Queue) Name() string {
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Memqueue<Name=%s>", q.Name())
}

func (q *Queue) Options() *msgqueue.QueueOptions {
	return q.opt
}

func (q *Queue) NewTask(opt *msgqueue.TaskOptions) *msgqueue.Task {
	return q.base.NewTask(q, opt)
}

func (q *Queue) GetTask(name string) *msgqueue.Task {
	return q.base.GetTask(name)
}

func (q *Queue) RemoveTask(name string) {
	q.base.RemoveTask(name)
}

func (q *Queue) Processor() *msgqueue.Processor {
	return q.p
}

func (q *Queue) SetSync(sync bool) {
	q.sync = sync
}

func (q *Queue) SetNoDelay(noDelay bool) {
	q.noDelay = noDelay
}

// Close is like CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	defer unregisterQueue(q)

	done := make(chan struct{}, 1)
	timeoutCh := time.After(timeout)

	go func() {
		q.wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timeoutCh:
		return fmt.Errorf("message are not processed after %s", timeout)
	}

	return q.p.StopTimeout(timeout)
}

func (q *Queue) Len() (int, error) {
	return q.Processor().Len(), nil
}

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	if msg.TaskName == "" {
		return internal.ErrTaskNameRequired
	}
	if !q.isUniqueName(msg.Name) {
		return msgqueue.ErrDuplicate
	}
	q.wg.Add(1)
	return q.enqueueMessage(msg)
}

func (q *Queue) enqueueMessage(msg *msgqueue.Message) error {
	if (q.noDelay || q.sync) && msg.Delay > 0 {
		msg.Delay = 0
	}
	msg.ReservedCount++

	if q.sync {
		return q.p.Process(msg)
	}
	return q.p.Add(msg)
}

func (q *Queue) isUniqueName(name string) bool {
	const redisPrefix = "memqueue"

	if name == "" {
		return true
	}

	key := fmt.Sprintf("%s:%s:%s", redisPrefix, q.opt.GroupName, name)
	exists := q.opt.Storage.Exists(key)
	return !exists
}

func (q *Queue) ReserveN(n int, reservationTimeout time.Duration, waitTimeout time.Duration) ([]*msgqueue.Message, error) {
	return nil, internal.ErrNotSupported
}

func (q *Queue) Release(msg *msgqueue.Message) error {
	return q.enqueueMessage(msg)
}

func (q *Queue) Delete(msg *msgqueue.Message) error {
	q.wg.Done()
	return nil
}

func (q *Queue) DeleteBatch(msgs []*msgqueue.Message) error {
	if len(msgs) == 0 {
		return errors.New("msgqueue: no messages to delete")
	}
	for _, msg := range msgs {
		if err := q.Delete(msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Purge() error {
	return q.p.Purge()
}
