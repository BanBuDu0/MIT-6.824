package mr

import (
	"time"
)

type MyTaskPhase int

const (
	MapPhase    MyTaskPhase = 0
	ReducePhase MyTaskPhase = 1
)

type Status int

const (
	IDLE      Status = 0
	RUNNING   Status = 1
	COMPLETED Status = 2
	ERROR     Status = 3
)

type Task struct {
	TaskId        int
	DataSource    string
	TaskPhase     MyTaskPhase
	TaskStartTime time.Time
	TaskStatus    Status
}

func (t *Task) isTaskAlive() bool {
	condition1 := t.TaskStartTime.IsZero()
	condition2 := t.TaskStatus == IDLE
	condition3 := t.TaskStatus == RUNNING && time.Now().After(t.TaskStartTime.Add(MaxTaskRunTime))
	return condition1 || condition2 || condition3
}

const MaxTaskRunTime = time.Second * 5
