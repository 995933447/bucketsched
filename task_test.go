package bucketsched

import (
	"fmt"
	"testing"
	"time"
)

func TestTaskHeap(t *testing.T) {
	var q taskHeap
	q.pushTask(NewTask(1, "testTask1", func(task *Task) error {
		fmt.Println("i'm task1")
		return nil
	}, 60, 2))

	q.pushTask(NewTask(1, "testTask1", func(task *Task) error {
		fmt.Println("i'm task2")
		return nil
	}, 0, 2))

	q.pushTask(NewTask(1, "testTask1", func(task *Task) error {
		fmt.Println("i'm task3")
		return nil
	}, 1, 2))

	fmt.Println("first loop")
	for {
		t, ok := q.popTask()
		if !ok {
			break
		}
		t.hdl(t)
	}

	time.Sleep(time.Second)
	fmt.Println("sec loop")
	for {
		t, ok := q.popTask()
		if !ok {
			break
		}
		t.hdl(t)
	}

	t.Logf("task heap size:%d", len(q))
}
