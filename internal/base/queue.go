package base

import (
	"github.com/go-msgqueue/msgqueue"
)

type Queue struct {
	tasks map[string]*msgqueue.Task
}

func (q *Queue) GetTask(name string) *msgqueue.Task {
	return q.tasks[name]
}

func (q *Queue) NewTask(opt *msgqueue.TaskOptions) *msgqueue.Task {
	return nil
}
