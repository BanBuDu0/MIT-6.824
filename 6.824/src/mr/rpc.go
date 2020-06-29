package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type RegisterArgs struct {
}

type RegisterReply struct {
	workId  int
	nMap    int
	nReduce int
}

type GetTaskArgs struct {
	workId int
}

type GetTaskReply struct {
	task *Task
}

type UpdateTaskArgs struct {
	task   *Task
	finish bool
	msg    string
}
type UpdateTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
