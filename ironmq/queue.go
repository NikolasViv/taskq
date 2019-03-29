package ironmq

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iron-io/iron_go3/api"
	iron_config "github.com/iron-io/iron_go3/config"
	"github.com/iron-io/iron_go3/mq"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal"
	"github.com/go-msgqueue/msgqueue/internal/base"
	"github.com/go-msgqueue/msgqueue/internal/msgutil"
	"github.com/go-msgqueue/msgqueue/memqueue"
)

type manager struct {
	cfg *iron_config.Settings
}

var _ msgqueue.Manager = (*manager)(nil)

func (m *manager) NewQueue(opt *msgqueue.QueueOptions) msgqueue.Queue {
	q := mq.ConfigNew(opt.Name, m.cfg)
	return NewQueue(q, opt)
}

func (manager) Queues() []msgqueue.Queue {
	var queues []msgqueue.Queue
	for _, q := range Queues() {
		queues = append(queues, q)
	}
	return queues
}

func NewManager(cfg *iron_config.Settings) msgqueue.Manager {
	return &manager{
		cfg: cfg,
	}
}

type Queue struct {
	base.Queue

	q   mq.Queue
	opt *msgqueue.QueueOptions

	addQueue   *memqueue.Queue
	addTask    *msgqueue.Task
	addBatcher *msgqueue.Batcher

	delQueue   *memqueue.Queue
	delTask    *msgqueue.Task
	delBatcher *msgqueue.Batcher

	p *msgqueue.Processor
}

var _ msgqueue.Queue = (*Queue)(nil)

func NewQueue(mqueue mq.Queue, opt *msgqueue.QueueOptions) *Queue {
	if opt.Name == "" {
		opt.Name = mqueue.Name
	}
	opt.Init()

	q := Queue{
		q:   mqueue,
		opt: opt,
	}

	q.initAddQueue()
	q.initDelQueue()

	registerQueue(&q)
	return &q
}

func (q *Queue) initAddQueue() {
	q.addQueue = memqueue.NewQueue(&msgqueue.QueueOptions{
		Name:      "ironmq:" + q.opt.Name + ":add",
		GroupName: q.opt.GroupName,

		BufferSize: 1000,
		Redis:      q.opt.Redis,
	})
	q.addTask = q.addQueue.NewTask(&msgqueue.TaskOptions{
		Handler:    msgqueue.HandlerFunc(q.add),
		RetryLimit: 3,
		MinBackoff: time.Second,
	})
}

func (q *Queue) initDelQueue() {
	q.delQueue = memqueue.NewQueue(&msgqueue.QueueOptions{
		Name:      "ironmq:" + q.opt.Name + ":delete",
		GroupName: q.opt.GroupName,

		BufferSize: 1000,
		Redis:      q.opt.Redis,
	})
	q.delTask = q.delQueue.NewTask(&msgqueue.TaskOptions{
		Handler:    msgqueue.HandlerFunc(q.delBatcherAdd),
		RetryLimit: 3,
		MinBackoff: time.Second,
	})
	q.delBatcher = msgqueue.NewBatcher(q.delQueue.Processor(), &msgqueue.BatcherOptions{
		Handler:     q.deleteBatch,
		ShouldBatch: q.shouldBatchDelete,
	})
}

func (q *Queue) Name() string {
	return q.q.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<Name=%s>", q.Name())
}

func (q *Queue) Options() *msgqueue.QueueOptions {
	return q.opt
}

func (q *Queue) Processor() *msgqueue.Processor {
	if q.p == nil {
		q.p = msgqueue.NewProcessor(q, q.opt)
	}
	return q.p
}

func (q *Queue) createQueue() error {
	_, err := mq.ConfigCreateQueue(mq.QueueInfo{Name: q.q.Name}, &q.q.Settings)
	return err
}

func (q *Queue) Len() (int, error) {
	queueInfo, err := q.q.Info()
	if err != nil {
		return 0, err
	}
	return queueInfo.Size, nil
}

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	msg = msgutil.WrapMessage(msg)
	return q.addTask.Add(msg)
}

func (q *Queue) ReserveN(n int, reservationTimeout time.Duration, waitTimeout time.Duration) ([]*msgqueue.Message, error) {
	if n > 100 {
		n = 100
	}

	reservationSecs := int(reservationTimeout / time.Second)
	waitSecs := int(waitTimeout / time.Second)

	mqMsgs, err := q.q.LongPoll(n, reservationSecs, waitSecs, false)
	if err != nil {
		if v, ok := err.(api.HTTPResponseError); ok && v.StatusCode() == 404 {
			if strings.Contains(v.Error(), "Message not found") {
				return nil, nil
			}
			if strings.Contains(v.Error(), "Queue not found") {
				_ = q.createQueue()
			}
		}
		return nil, err
	}

	msgs := make([]*msgqueue.Message, len(mqMsgs))
	for i, mqMsg := range mqMsgs {
		msgs[i] = &msgqueue.Message{
			Id:   mqMsg.Id,
			Body: mqMsg.Body,

			ReservationId: mqMsg.ReservationId,
			ReservedCount: mqMsg.ReservedCount,
		}
	}
	return msgs, nil
}

func (q *Queue) Release(msg *msgqueue.Message) error {
	return retry(func() error {
		return q.q.ReleaseMessage(msg.Id, msg.ReservationId, int64(msg.Delay/time.Second))
	})
}

// Delete deletes the message from the queue.
func (q *Queue) Delete(msg *msgqueue.Message) error {
	err := retry(func() error {
		return q.q.DeleteMessage(msg.Id, msg.ReservationId)
	})
	if err == nil {
		return nil
	}
	if v, ok := err.(api.HTTPResponseError); ok && v.StatusCode() == 404 {
		return nil
	}
	return err
}

// Purge deletes all messages from the queue using IronMQ API.
func (q *Queue) Purge() error {
	return q.q.Clear()
}

// Close is CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// Close closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	var firstErr error

	if q.p != nil {
		err := q.p.StopTimeout(timeout)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	err := q.delBatcher.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	err = q.delQueue.CloseTimeout(timeout)
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (q *Queue) add(msg *msgqueue.Message) error {
	msg, err := msgutil.UnwrapMessage(msg)
	if err != nil {
		return err
	}

	body, err := msg.MarshalBody()
	if err != nil {
		return err
	}

	id, err := q.q.PushMessage(mq.Message{
		Body:  body,
		Delay: int64(msg.Delay / time.Second),
	})
	if err != nil {
		return err
	}

	msg.Id = id
	return nil
}

func (q *Queue) delBatcherAdd(msg *msgqueue.Message) error {
	return q.delBatcher.Add(msg)
}

func (q *Queue) deleteBatch(msgs []*msgqueue.Message) error {
	if len(msgs) == 0 {
		return errors.New("ironmq: no messages to delete")
	}

	mqMsgs := make([]mq.Message, len(msgs))
	for i, msg := range msgs {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			return err
		}

		mqMsgs[i] = mq.Message{
			Id:            msg.Id,
			ReservationId: msg.ReservationId,
		}
	}

	err := retry(func() error {
		return q.q.DeleteReservedMessages(mqMsgs)
	})
	if err != nil {
		internal.Logf("ironmq: DeleteReservedMessages failed: %s", err)
		return err
	}

	return nil
}

func (q *Queue) shouldBatchDelete(batch []*msgqueue.Message, msg *msgqueue.Message) bool {
	const messagesLimit = 10
	return len(batch)+1 < messagesLimit
}

func retry(fn func() error) error {
	var err error
	for i := 0; i < 3; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if v, ok := err.(api.HTTPResponseError); ok && v.StatusCode() >= 500 {
			continue
		}
		break
	}
	return err
}
