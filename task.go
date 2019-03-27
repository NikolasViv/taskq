package msgqueue

import "time"

type TaskOptions struct {
	Name string

	// Function called to process a message.
	Handler interface{}

	// Compress data before sending to the queue.
	Compress bool

	// Number of tries/releases after which the message fails permanently
	// and is deleted.
	// Default is 64 retries.
	RetryLimit int
	// Minimum backoff time between retries.
	// Default is 30 seconds.
	MinBackoff time.Duration
	// Maximum backoff time between retries.
	// Default is 30 minutes.
	MaxBackoff time.Duration
}

func (opt *TaskOptions) init() {
	if opt.RetryLimit == 0 {
		opt.RetryLimit = 64
	}
	if opt.MinBackoff == 0 {
		opt.MinBackoff = 30 * time.Second
	}
	if opt.MaxBackoff == 0 {
		opt.MaxBackoff = 30 * time.Minute
	}
}

type Task struct {
	queue Queue
	opt   *TaskOptions
}

func NewTask(queue Queue, opt *TaskOptions) *Task {
	return &Task{
		queue: queue,
		opt:   opt,
	}
}

func (t *Task) Add(msg *Message) error {
	return nil
}

func (t *Task) Call(args ...interface{}) error {
	return nil
}

func (t *Task) CallOnce(dur time.Duration, args ...interface{}) error {
	return nil
}
