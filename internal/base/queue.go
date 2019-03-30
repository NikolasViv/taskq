package base

import (
	"fmt"

	"github.com/go-msgqueue/msgqueue"
)

type Queue struct {
	queue msgqueue.Queue
	tasks map[string]*msgqueue.Task
}

func (q *Queue) GetTask(name string) *msgqueue.Task {
	return q.tasks[name]
}

func (q *Queue) NewTask(opt *msgqueue.TaskOptions) *msgqueue.Task {
	task, ok := q.tasks[opt.Name]
	if ok {
		panic(fmt.Errorf("% already has %s", q, task))
	}
	task = msgqueue.NewTask(q.queue, opt)
	q.tasks[opt.Name] = task
	return task
}
