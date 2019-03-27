package base

import (
	"github.com/go-msgqueue/msgqueue"
)

type Queue struct {
	tasks map[string]*msgqueue.Task
}

func (q *Queue) HandleMessage(msg *msgqueue.Message) error {
	return nil
}

func (q *Queue) NewTask(*msgqueue.TaskOptions) *msgqueue.Task {
	return nil
}
