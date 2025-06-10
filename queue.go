package bucketQueue

import (
	"errors"
	uuid "github.com/satori/go.uuid"
	"sync"
	"time"
)

var ErrQueueStackFull = errors.New("queue stack full")

// NewBucketQueue 普通队列, 总共concurWorkerNum个最大可用worker,根据bucket id公平权重调度
func NewBucketQueue(name string, concurWorkerNum uint32) *BucketQueue {
	if name == "" {
		name = uuid.NewV4().String()
	}
	if concurWorkerNum == 0 {
		concurWorkerNum = 1
	}
	return &BucketQueue{
		name:            name,
		bucketMap:       make(map[int64]*Bucket),
		taskCh:          make(chan *Task),
		newTaskInSign:   make(chan struct{}),
		concurWorkerNum: concurWorkerNum,
		bucketConcurRec: map[int64]uint32{},
	}
}

// NewBucketQueueWithBucketConcur bucket限制并发队列, 总共concurWorkerNum个最大可用worker,根据bucket id公平权重调度,
// 且单个bucket, 同时只能有bucketConcurMax个消息被并发消费
func NewBucketQueueWithBucketConcur(name string, concurWorkerNum, bucketConcurMax uint32) *BucketQueue {
	queue := NewBucketQueue(name, concurWorkerNum)
	queue.bucketConcurMax = bucketConcurMax
	return queue
}

type Bucket struct {
	bucketId int64
	next     *Bucket
	pre      *Bucket
	tasks    *taskHeap
}

type BucketQueue struct {
	name            string
	bucketHeader    *Bucket
	bucketTail      *Bucket
	curBucket       *Bucket
	bucketMap       map[int64]*Bucket
	taskCh          chan *Task
	newTaskInSign   chan struct{}
	concurWorkerNum uint32
	bucketConcurMax uint32
	bucketConcurRec map[int64]uint32
	mu              sync.Mutex
	size            int
}

func (q *BucketQueue) SetSize(size int) {
	if size <= 0 {
		return
	}
	q.size = size
}

func (q *BucketQueue) Size() int {
	var queueSize int
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, bucket := range q.bucketMap {
		queueSize += len(*bucket.tasks)
	}
	return queueSize
}

func (q *BucketQueue) Reg(task *Task) error {
	queueSize := q.Size()
	if (q.size > 0 && q.size <= queueSize) || (q.size == 0 && queueSize > 2024000) {
		return ErrQueueStackFull
	}

	if task.delayAt <= 0 {
		if task.attempted == 0 {
			task.delayAt = time.Now().Unix() + int64(task.delay)
		} else {
			task.delayAt = time.Now().Unix() + int64(task.retryDelay)
		}
	}

	defer func() {
		select {
		case q.newTaskInSign <- struct{}{}:
		default:
		}
	}()

	q.mu.Lock()
	defer q.mu.Unlock()

	bucket, ok := q.bucketMap[task.bucketId]
	if ok {
		bucket.tasks.pushTask(task)
		return nil
	}

	bucket = &Bucket{
		bucketId: task.bucketId,
		pre:      q.bucketTail,
		tasks:    newTaskHeap(task),
	}

	if q.bucketTail != nil {
		q.bucketTail.next = bucket
	}
	q.bucketTail = bucket

	if q.bucketHeader == nil {
		q.bucketHeader = bucket
	}

	q.bucketMap[task.bucketId] = bucket
	return nil
}

func (q *BucketQueue) hasBucketConcurLimit() bool {
	return q.bucketConcurMax > 0
}

func (q *BucketQueue) Run() {
	for i := uint32(0); i < q.concurWorkerNum; i++ {
		go func() {
			for {
				task := <-q.taskCh
				task.attempted++
				err := task.hdl(task)
				if q.hasBucketConcurLimit() {
					q.mu.Lock()
					concurNum := q.bucketConcurRec[task.bucketId]
					if concurNum > 0 {
						q.bucketConcurRec[task.bucketId] = concurNum - 1
					}
					q.mu.Unlock()
				}
				if err != nil {
					if task.maxAttempt <= task.attempted {
						continue
					}
					if task.retryDelay > 0 {
						task.delayAt = time.Now().Unix() + int64(task.retryDelay)
					} else {
						task.delayAt = time.Now().Unix() + int64(task.delay)
					}
					if err = q.Reg(task); err != nil {
						// TODO
					}
				}
			}
		}()
	}
	q.sched()
}

func (q *BucketQueue) waitNewTaskIn() {
	waitNewTaskTimeoutTk := time.NewTimer(time.Second)
	select {
	case <-waitNewTaskTimeoutTk.C:
		waitNewTaskTimeoutTk.Stop()
	case <-q.newTaskInSign:
		waitNewTaskTimeoutTk.Stop()
	}
}

func (q *BucketQueue) sched() {
	var loopEmptyCnt int
	for {
		q.mu.Lock()
		if q.curBucket == nil {
			if q.bucketHeader == nil {
				q.mu.Unlock()
				q.waitNewTaskIn()
				continue
			}
			q.curBucket = q.bucketHeader
		}

		var (
			overBucketConcurMax bool
			bucketConcurNum     uint32
		)
		limitedBucketConcur := q.hasBucketConcurLimit()
		if limitedBucketConcur {
			bucketConcurNum = q.bucketConcurRec[q.curBucket.bucketId]
			overBucketConcurMax = bucketConcurNum >= q.bucketConcurMax
		}

		var (
			ok   bool
			task *Task
		)
		if !overBucketConcurMax {
			task, ok = q.curBucket.tasks.popTask()
			if ok && limitedBucketConcur {
				q.bucketConcurRec[q.curBucket.bucketId] = bucketConcurNum + 1
			}
		}

		q.mu.Unlock()
		if ok {
			loopEmptyCnt = 0
			q.taskCh <- task
		} else {
			loopEmptyCnt++
			q.mu.Lock()
			// 所有桶里都没有到期执行的任务，休眠1s
			if loopEmptyCnt >= len(q.bucketMap) {
				var rmBuckets []*Bucket
				for _, bucket := range q.bucketMap {
					if len(*bucket.tasks) == 0 {
						rmBuckets = append(rmBuckets, bucket)
					}
				}
				for _, bucket := range rmBuckets {
					if bucket.pre != nil {
						bucket.pre.next = bucket.next
					}
					if bucket.next != nil {
						bucket.next.pre = bucket.pre
					}
					if q.bucketHeader == bucket {
						q.bucketHeader = bucket.next
					}
					if q.bucketTail == bucket {
						q.bucketTail = bucket.pre
					}
					if q.curBucket == bucket {
						q.curBucket = bucket.next
					}
					delete(q.bucketMap, bucket.bucketId)
				}
				q.mu.Unlock()
				q.waitNewTaskIn()
				loopEmptyCnt = 0
			} else {
				q.mu.Unlock()
			}
		}
		q.mu.Lock()
		if q.curBucket != nil {
			q.curBucket = q.curBucket.next
		}
		q.mu.Unlock()
	}
}
