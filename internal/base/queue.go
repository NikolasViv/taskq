package base

import (
	"fmt"

	"github.com/go-msgqueue/msgqueue"
)

type Queue struct {
	tasks map[string]*msgqueue.Task
}

func (q *Queue) NewTask(queue msgqueue.Queue, opt *msgqueue.TaskOptions) *msgqueue.Task {
	task, ok := q.tasks[opt.Name]
	if ok {
		panic(fmt.Errorf("% already has %s", q, task))
	}
	task = msgqueue.NewTask(queue, opt)
	q.tasks[opt.Name] = task
	return task
}

func (q *Queue) GetTask(name string) *msgqueue.Task {
	return q.tasks[name]
}
