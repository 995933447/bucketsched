package bucketsched

import (
	"github.com/995933447/log-go"
	"github.com/995933447/log-go/impl/loggerwriter"
	"github.com/995933447/std-go/print"
	uuid "github.com/satori/go.uuid"
	"sync"
	"time"
)

func NewBucketQueue(name string, concurWorkerNum uint32, logger *log.Logger) *BucketQueue {
	if name == "" {
		name = uuid.NewV4().String()
	}
	if concurWorkerNum == 0 {
		concurWorkerNum = 1
	}
	if logger == nil {
		logger = log.NewLogger(loggerwriter.NewStdoutLoggerWriter(print.ColorNil))
	}
	return &BucketQueue{
		name:            name,
		bucketMap:       make(map[int64]*Bucket),
		taskCh:          make(chan *Task),
		concurWorkerNum: concurWorkerNum,
		logger:          logger,
	}
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
	concurWorkerNum uint32
	mu              sync.Mutex
	logger          *log.Logger
}

func (q *BucketQueue) Reg(task *Task) {
	if task.delayAt <= 0 {
		if task.attempted == 0 {
			task.delayAt = time.Now().Unix() + int64(task.delay)
		} else {
			task.delayAt = time.Now().Unix() + int64(task.retryDelay)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	bucket, ok := q.bucketMap[task.bucketId]
	if ok {
		bucket.tasks.pushTask(task)
		return
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
}

func (q *BucketQueue) Run() {
	for i := uint32(0); i < q.concurWorkerNum; i++ {
		go func() {
			task := <-q.taskCh
			task.attempted++
			q.logger.Infof(nil, "BucketQueue(name:%s) exec task.%s.id:%s, attempted:%d", q.name, task.name, task.taskId, task.attempted)
			err := task.hdl(task)
			if err != nil {
				q.logger.Errorf(
					nil,
					"BucketQueue(name:%s) exec task.%s.id:%s, attempted:%d, FAILED!!reason:%v",
					q.name, task.name, task.taskId, task.attempted, err,
				)
				if task.maxAttempt <= task.attempted {
					return
				}
				if task.retryDelay > 0 {
					task.delayAt = time.Now().Unix() + int64(task.retryDelay)
				} else {
					task.delayAt = time.Now().Unix() + int64(task.delay)
				}
				q.Reg(task)
			}
		}()
	}
	q.sched()
}

func (q *BucketQueue) sched() {
	var loopEmptyCnt int
	for {
		q.mu.Lock()
		if q.curBucket == nil {
			if q.bucketHeader == nil {
				q.mu.Unlock()
				time.Sleep(time.Second)
				continue
			}
			q.curBucket = q.bucketHeader
		}
		task, ok := q.curBucket.tasks.popTask()
		q.mu.Unlock()
		if ok {
			loopEmptyCnt = 0
			q.taskCh <- task
		} else {
			loopEmptyCnt++
			q.mu.Lock()
			// 所有桶里都没有到期执行的任务，休眠1s
			if loopEmptyCnt >= len(q.bucketMap) {
				var rmBucketIds []int64
				for _, bucket := range q.bucketMap {
					if len(*bucket.tasks) == 0 {
						rmBucketIds = append(rmBucketIds, bucket.bucketId)
					}
				}
				for _, bucketId := range rmBucketIds {
					delete(q.bucketMap, bucketId)
				}
				q.mu.Unlock()
				time.Sleep(time.Second)
				loopEmptyCnt = 0
			} else {
				q.mu.Unlock()
			}
		}
		q.mu.Lock()
		q.curBucket = q.curBucket.next
		q.mu.Unlock()
	}
}
