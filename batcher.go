package msgqueue

import (
	"sync"
	"time"
)

type BatcherOptions struct {
	Handler     func([]*Message) error
	ShouldBatch func([]*Message, *Message) bool

	Timeout time.Duration
}

func (opt *BatcherOptions) init(p *Processor) {
	if opt.Timeout == 0 {
		opt.Timeout = 3 * time.Second
	}
}

// Batcher collects messages for later batch processing.
type Batcher struct {
	p   *Processor
	opt *BatcherOptions

	timer *time.Timer

	mu     sync.Mutex
	batch  []*Message
	closed bool
}

func NewBatcher(p *Processor, opt *BatcherOptions) *Batcher {
	opt.init(p)
	b := Batcher{
		p:   p,
		opt: opt,
	}
	b.timer = time.AfterFunc(time.Minute, b.onTimeout)
	b.timer.Stop()
	return &b
}

func (b *Batcher) wait() {
	if len(b.batch) > 0 {
		b.process(b.batch)
		b.batch = nil
	}
}

func (b *Batcher) Add(msg *Message) error {
	var batch []*Message

	b.mu.Lock()

	if b.closed {
		if len(b.batch) > 0 {
			panic("not reached")
		}
		batch = []*Message{msg}
	} else {
		if len(b.batch) == 0 {
			b.stopTimer()
			b.timer.Reset(b.opt.Timeout)
		}

		if b.opt.ShouldBatch(b.batch, msg) {
			b.batch = append(b.batch, msg)
		} else {
			batch = b.batch
			b.batch = []*Message{msg}
		}
	}

	b.mu.Unlock()

	if len(batch) > 0 {
		b.process(batch)
	}

	return ErrAsyncTask
}

func (b *Batcher) stopTimer() {
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
}

func (b *Batcher) process(batch []*Message) {
	err := b.opt.Handler(batch)
	for _, msg := range batch {
		b.p.Put(msg, err)
	}
}

func (b *Batcher) onTimeout() {
	b.mu.Lock()
	b.wait()
	b.mu.Unlock()
}

func (b *Batcher) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true

	b.stopTimer()
	b.wait()

	return nil
}
