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
	if q.tasks == nil {
		q.tasks = make(map[string]*msgqueue.Task)
	}
	q.tasks[opt.Name] = task
	return task
}

func (q *Queue) GetTask(name string) *msgqueue.Task {
	return q.tasks[name]
}

func (q *Queue) RemoveTask(name string) {
	delete(q.tasks, name)
}
