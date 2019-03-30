package azsqs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-msgqueue/msgqueue"
	"github.com/go-msgqueue/msgqueue/internal"
	"github.com/go-msgqueue/msgqueue/internal/base"
	"github.com/go-msgqueue/msgqueue/internal/msgutil"
	"github.com/go-msgqueue/msgqueue/memqueue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

const delayUntilAttr = "MsgqueueDelayUntil"

type manager struct {
	sqs       *sqs.SQS
	accountId string
}

var _ msgqueue.Manager = (*manager)(nil)

func (m *manager) NewQueue(opt *msgqueue.QueueOptions) msgqueue.Queue {
	return NewQueue(m.sqs, m.accountId, opt)
}

func (manager) Queues() []msgqueue.Queue {
	var queues []msgqueue.Queue
	for _, q := range Queues() {
		queues = append(queues, q)
	}
	return queues
}

func NewManager(sqs *sqs.SQS, accountId string) msgqueue.Manager {
	return &manager{
		sqs:       sqs,
		accountId: accountId,
	}
}

type Queue struct {
	base.Queue

	sqs       *sqs.SQS
	accountID string
	opt       *msgqueue.QueueOptions

	addQueue   *memqueue.Queue
	addTask    *msgqueue.Task
	addBatcher *msgqueue.Batcher

	delQueue   *memqueue.Queue
	delTask    *msgqueue.Task
	delBatcher *msgqueue.Batcher

	mu        sync.RWMutex
	_queueURL string

	p *msgqueue.Processor
}

var _ msgqueue.Queue = (*Queue)(nil)

func NewQueue(sqs *sqs.SQS, accountID string, opt *msgqueue.QueueOptions) *Queue {
	opt.Init()

	q := Queue{
		sqs:       sqs,
		accountID: accountID,
		opt:       opt,
	}

	q.initAddQueue()
	q.initDelQueue()

	registerQueue(&q)
	return &q
}

func (q *Queue) initAddQueue() {
	q.addQueue = memqueue.NewQueue(&msgqueue.QueueOptions{
		Name:      "azsqs:" + q.opt.Name + ":add",
		GroupName: q.opt.GroupName,

		BufferSize: 1000,
		Redis:      q.opt.Redis,
	})
	q.addTask = q.addQueue.NewTask(&msgqueue.TaskOptions{
		Handler:    msgqueue.HandlerFunc(q.addBatcherAdd),
		RetryLimit: 3,
		MinBackoff: time.Second,
	})

	q.addBatcher = msgqueue.NewBatcher(q.addQueue.Processor(), &msgqueue.BatcherOptions{
		Handler:     q.addBatch,
		ShouldBatch: q.shouldBatchAdd,
	})
}

func (q *Queue) initDelQueue() {
	q.delQueue = memqueue.NewQueue(&msgqueue.QueueOptions{
		Name:      "azsqs:" + q.opt.Name + ":delete",
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
	return q.opt.Name
}

func (q *Queue) String() string {
	return fmt.Sprintf("Queue<Name=%s>", q.Name())
}

func (q *Queue) Options() *msgqueue.QueueOptions {
	return q.opt
}

func (q *Queue) GetAddQueue() *memqueue.Queue {
	return q.addQueue
}

func (q *Queue) GetDeleteQueue() *memqueue.Queue {
	return q.delQueue
}

func (q *Queue) Processor() *msgqueue.Processor {
	if q.p == nil {
		q.p = msgqueue.NewProcessor(q, q.opt)
	}
	return q.p
}

func (q *Queue) Len() (int, error) {
	params := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(q.queueURL()),
		AttributeNames: []*string{aws.String("ApproximateNumberOfMessages")},
	}
	resp, err := q.sqs.GetQueueAttributes(params)
	if err != nil {
		return 0, err
	}

	prop := resp.Attributes["ApproximateNumberOfMessages"]
	return strconv.Atoi(*prop)
}

// Add adds message to the queue.
func (q *Queue) Add(msg *msgqueue.Message) error {
	if msg.TaskName == "" {
		return internal.ErrTaskNameRequired
	}
	msg = msgutil.WrapMessage(msg)
	return q.addTask.AddMessage(msg)
}

func (q *Queue) queueURL() string {
	q.mu.RLock()
	queueURL := q._queueURL
	q.mu.RUnlock()
	if queueURL != "" {
		return queueURL
	}

	q.mu.Lock()
	_, _ = q.createQueue()

	queueURL, err := q.getQueueURL()
	if err == nil {
		q._queueURL = queueURL
	}
	q.mu.Unlock()

	return queueURL
}

func (q *Queue) createQueue() (string, error) {
	visTimeout := strconv.Itoa(int(q.opt.ReservationTimeout / time.Second))
	in := &sqs.CreateQueueInput{
		QueueName: aws.String(q.Name()),
		Attributes: map[string]*string{
			"VisibilityTimeout": &visTimeout,
		},
	}
	out, err := q.sqs.CreateQueue(in)
	if err != nil {
		return "", err
	}
	return *out.QueueUrl, nil
}

func (q *Queue) getQueueURL() (string, error) {
	in := &sqs.GetQueueUrlInput{
		QueueName:              aws.String(q.Name()),
		QueueOwnerAWSAccountId: &q.accountID,
	}
	out, err := q.sqs.GetQueueUrl(in)
	if err != nil {
		return "", err
	}
	return *out.QueueUrl, nil
}

func (q *Queue) ReserveN(n int, reservationTimeout time.Duration, waitTimeout time.Duration) ([]*msgqueue.Message, error) {
	if n > 10 {
		n = 10
	}
	in := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(q.queueURL()),
		MaxNumberOfMessages:   aws.Int64(int64(n)),
		WaitTimeSeconds:       aws.Int64(int64(waitTimeout / time.Second)),
		AttributeNames:        []*string{aws.String("ApproximateReceiveCount")},
		MessageAttributeNames: []*string{aws.String(delayUntilAttr)},
	}
	out, err := q.sqs.ReceiveMessage(in)
	if err != nil {
		return nil, err
	}

	msgs := make([]*msgqueue.Message, 0, len(out.Messages))
	for _, sqsMsg := range out.Messages {
		msg := new(msgqueue.Message)

		if *sqsMsg.Body != "_" {
			b, err := base64.RawStdEncoding.DecodeString(*sqsMsg.Body)
			if err != nil {
				return nil, err
			}

			err = msg.UnmarshalBinary(b)
			if err != nil {
				return nil, err
			}
		}

		msg.ReservationID = *sqsMsg.ReceiptHandle

		if v, ok := sqsMsg.Attributes["ApproximateReceiveCount"]; ok {
			var err error
			msg.ReservedCount, err = strconv.Atoi(*v)
			if err != nil {
				return nil, err
			}
		}

		if v, ok := sqsMsg.MessageAttributes[delayUntilAttr]; ok {
			until, err := time.Parse(time.RFC3339, *v.StringValue)
			if err != nil {
				return nil, err
			}

			msg.Delay = until.Sub(time.Now())
			if msg.Delay < 0 {
				msg.Delay = 0
			}
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (q *Queue) Release(msg *msgqueue.Message) error {
	in := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(q.queueURL()),
		ReceiptHandle:     &msg.ReservationID,
		VisibilityTimeout: aws.Int64(int64(msg.Delay / time.Second)),
	}
	var err error
	for i := 0; i < 3; i++ {
		_, err = q.sqs.ChangeMessageVisibility(in)
		if err == nil {
			return nil
		}
		if i > 0 &&
			strings.Contains(err.Error(), "Message does not exist") {
			return nil
		}
		if !strings.Contains(err.Error(), "Please try again") {
			break
		}
	}
	return err
}

// Delete deletes the message from the queue.
func (q *Queue) Delete(msg *msgqueue.Message) error {
	return q.delTask.AddMessage(msgutil.WrapMessage(msg))
}

// Purge deletes all messages from the queue using SQS API.
func (q *Queue) Purge() error {
	in := &sqs.PurgeQueueInput{
		QueueUrl: aws.String(q.queueURL()),
	}
	_, err := q.sqs.PurgeQueue(in)
	return err
}

// Close is like CloseTimeout with 30 seconds timeout.
func (q *Queue) Close() error {
	return q.CloseTimeout(30 * time.Second)
}

// CloseTimeout closes the queue waiting for pending messages to be processed.
func (q *Queue) CloseTimeout(timeout time.Duration) error {
	var firstErr error

	if q.p != nil {
		err := q.p.StopTimeout(timeout)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	err := q.addBatcher.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	err = q.addQueue.CloseTimeout(timeout)
	if err != nil && firstErr == nil {
		firstErr = err
	}

	err = q.delBatcher.Close()
	if err != nil && firstErr == nil {
		firstErr = err
	}

	err = q.delQueue.CloseTimeout(timeout)
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (q *Queue) addBatcherAdd(msg *msgqueue.Message) error {
	return q.addBatcher.Add(msg)
}

func (q *Queue) addBatch(msgs []*msgqueue.Message) error {
	const maxDelay = 15 * time.Minute

	if len(msgs) == 0 {
		return errors.New("azsqs: no messages to add")
	}

	in := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(q.queueURL()),
	}

	for i, msg := range msgs {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			return err
		}

		b, err := msg.MarshalBinary()
		if err != nil {
			internal.Logf("azsqs: MarshalBinary failed: %s", err)
			continue
		}

		body := base64.RawStdEncoding.EncodeToString(b)
		if body == "" {
			body = "_" // SQS requires body.
		}

		entry := &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(i)),
			MessageBody: aws.String(body),
		}
		if msg.Delay <= maxDelay {
			entry.DelaySeconds = aws.Int64(int64(msg.Delay / time.Second))
		} else {
			entry.DelaySeconds = aws.Int64(int64(maxDelay / time.Second))
			delayUntil := time.Now().Add(msg.Delay - maxDelay)
			entry.MessageAttributes = map[string]*sqs.MessageAttributeValue{
				delayUntilAttr: &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(delayUntil.Format(time.RFC3339)),
				},
			}
		}

		in.Entries = append(in.Entries, entry)
	}

	out, err := q.sqs.SendMessageBatch(in)
	if err != nil {
		internal.Logf("azsqs: SendMessageBatch msgs=%d size=%d failed: %s",
			len(msgs), q.batchSize(msgs), err)
		return err
	}

	for _, entry := range out.Failed {
		if entry.SenderFault != nil && *entry.SenderFault {
			internal.Logf(
				"azsqs: SendMessageBatch failed with code=%s message=%q",
				tos(entry.Code), tos(entry.Message))
			continue
		}

		msg := findMessageById(msgs, tos(entry.Id))
		if msg != nil {
			msg.StickyErr = fmt.Errorf("%s: %s", tos(entry.Code), tos(entry.Message))
		} else {
			internal.Logf("azsqs: can't find message with id=%s", tos(entry.Id))
		}
	}

	return nil
}

func (q *Queue) shouldBatchAdd(batch []*msgqueue.Message, msg *msgqueue.Message) bool {
	batch = append(batch, msg)

	const sizeLimit = 250 * 1024
	if q.batchSize(batch) > sizeLimit {
		return false
	}

	const messagesLimit = 10
	return len(batch) < messagesLimit
}

func (q *Queue) batchSize(batch []*msgqueue.Message) int {
	var size int
	for _, msg := range batch {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			internal.Logf("azsqs: UnwrapMessage failed: %s", err)
			continue
		}

		b, err := msg.MarshalBinary()
		if err != nil {
			internal.Logf("azsqs: Message.EncodeBody failed: %s", err)
			continue
		}

		size += base64.RawStdEncoding.EncodedLen(len(b))
	}
	return size
}

func (q *Queue) delBatcherAdd(msg *msgqueue.Message) error {
	return q.delBatcher.Add(msg)
}

func (q *Queue) deleteBatch(msgs []*msgqueue.Message) error {
	if len(msgs) == 0 {
		return errors.New("azsqs: no messages to delete")
	}

	entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(msgs))
	for i, msg := range msgs {
		msg, err := msgutil.UnwrapMessage(msg)
		if err != nil {
			return err
		}

		entries[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(strconv.Itoa(i)),
			ReceiptHandle: &msg.ReservationID,
		}
	}

	in := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(q.queueURL()),
		Entries:  entries,
	}
	out, err := q.sqs.DeleteMessageBatch(in)
	if err != nil {
		internal.Logf("azsqs: DeleteMessageBatch failed: %s", err)
		return err
	}

	for _, entry := range out.Failed {
		if entry.SenderFault != nil && *entry.SenderFault {
			internal.Logf(
				"azsqs: DeleteMessageBatch failed with code=%s message=%q",
				tos(entry.Code), tos(entry.Message),
			)
			continue
		}

		msg := findMessageById(msgs, tos(entry.Id))
		if msg != nil {
			msg.StickyErr = fmt.Errorf("%s: %s", tos(entry.Code), tos(entry.Message))
		} else {
			internal.Logf("azsqs: can't find message with id=%s", tos(entry.Id))
		}
	}
	return nil
}

func (q *Queue) shouldBatchDelete(batch []*msgqueue.Message, msg *msgqueue.Message) bool {
	const messagesLimit = 10
	return len(batch)+1 < messagesLimit
}

func findMessageById(msgs []*msgqueue.Message, id string) *msgqueue.Message {
	i, err := strconv.Atoi(id)
	if err != nil {
		return nil
	}
	if i < len(msgs) {
		return msgs[i]
	}
	return nil
}

func tos(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
