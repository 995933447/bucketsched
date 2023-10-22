package bucketsched

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	q := NewBucketQueue("testQ", 10, nil)

	go q.Run()

	q.Reg(NewTask(1, "taskA", func(task *Task) error {
		fmt.Println("i am task A, bucketID 2, delay 5")
		return nil
	}, 5, 2))

	q.Reg(NewTask(1, "taskB", func(task *Task) error {
		fmt.Println("i am task B, bucketID 2, delay 0")
		return nil
	}, 0, 2))

	q.Reg(NewTask(1, "taskC", func(task *Task) error {
		fmt.Println("i am task C, bucketID 2, delay 1")
		return nil
	}, 1, 2))

	q.Reg(NewTask(2, "taskA", func(task *Task) error {
		fmt.Println("i am task A,  bucketID 2, delay 5")
		return nil
	}, 5, 2))

	q.Reg(NewTask(2, "taskB", func(task *Task) error {
		fmt.Println("i am task B, bucketID 2, delay 0")
		return nil
	}, 0, 2))

	q.Reg(NewTask(2, "taskC", func(task *Task) error {
		fmt.Println("i am task C, bucketID 2, delay 1")
		return nil
	}, 1, 2))

	q.Reg(NewTask(3, "taskA", func(task *Task) error {
		fmt.Println("i am task A,  bucketID 3, delay 5")
		if task.GetAttempted() > 1 {
			return nil
		}
		return errors.New("retry error")
	}, 5, 2))

	q.Reg(NewTask(3, "taskB", func(task *Task) error {
		fmt.Println("i am task B, bucketID 3, delay 0")
		return nil
	}, 0, 2))

	q.Reg(NewTask(3, "taskC", func(task *Task) error {
		fmt.Println("i am task C, bucketID 3, delay 1")
		return nil
	}, 1, 2))

	go func() {
		time.Sleep(time.Second * 6)
		q.Reg(NewTask(4, "taskA", func(task *Task) error {
			fmt.Println("i am task C, bucketID 4, delay 0")
			return nil
		}, 0, 1))
	}()

	time.Sleep(30 * time.Minute)
}
