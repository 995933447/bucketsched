package bucketsched

import (
	"container/heap"
	uuid "github.com/satori/go.uuid"
	"time"
)

func NewTask(bucketId int64, name string, hdl func(task *Task) error, delay uint32, maxAttempt uint32) *Task {
	if len(name) > 1024 {
		name = name[:1024]
	}
	return &Task{
		bucketId:   bucketId,
		taskId:     uuid.NewV4().String(),
		name:       name,
		hdl:        hdl,
		delay:      delay,
		delayAt:    time.Now().UnixNano() + int64(delay)*1000000000,
		maxAttempt: maxAttempt,
	}
}

type Task struct {
	bucketId   int64
	taskId     string
	name       string
	hdl        func(task *Task) error
	maxAttempt uint32
	attempted  uint32
	retryDelay uint32
	delay      uint32
	delayAt    int64
}

func (t *Task) GetAttempted() uint32 {
	return t.attempted
}

func (t *Task) GetTaskId() string {
	return t.taskId
}

type taskHeap []*Task

func newTaskHeap(items ...*Task) *taskHeap {
	var q taskHeap
	for _, item := range items {
		q = append(q, item)
	}

	heap.Init(&q)
	return &q
}

func (q *taskHeap) popTask() (*Task, bool) {
	if len(*q) == 0 {
		return nil, false
	}
	if (*q)[0].delayAt > time.Now().UnixNano() {
		return nil, false
	}
	return heap.Pop(q).(*Task), true
}

func (q *taskHeap) pushTask(task *Task) {
	heap.Push(q, task)
}

func (q *taskHeap) Len() int {
	return len(*q)
}

func (q taskHeap) Swap(i, j int) {
	tmp := q[i]
	q[i] = q[j]
	q[j] = tmp
}

func (q taskHeap) Less(i, j int) bool {
	return q[i].delayAt < q[j].delayAt
}

func (q *taskHeap) Push(x interface{}) {
	*q = append(*q, x.(*Task))
}

func (q *taskHeap) Pop() interface{} {
	queenLen := len(*q)
	if queenLen == 0 {
		return nil
	}
	lastItemIndex := queenLen - 1
	item := (*q)[lastItemIndex]
	*q = (*q)[:lastItemIndex]
	return item
}

func (q taskHeap) Peek() *Task {
	return q[0]
}
