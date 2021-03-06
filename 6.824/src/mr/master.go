package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

/**
map reduce是按task任务来分的，任务分为map任务和reduce任务，
任务可以调度给work来执行
*/
type Master struct {
	files     []string
	fileNum   int
	nReduce   int
	mu        sync.Mutex
	workerNum int
	TaskPhase MyTaskPhase
	Tasks     []Task
	done      bool
}

/**
RPC CALL
*/
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerNum += 1
	reply.WorkId = m.workerNum
	reply.NReduce = m.nReduce
	reply.NMap = m.fileNum
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.Tasks); i++ {
		if m.Tasks[i].TaskStatus == IDLE {
			m.Tasks[i].WorkerId = args.WorkId
			m.Tasks[i].TaskStartTime = time.Now()
			m.Tasks[i].TaskStatus = RUNNING
			reply.Task = &m.Tasks[i]
			return nil
		}
	}
	return nil
}

func (m *Master) UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	finish := args.Finish
	task := args.Task
	if finish {
		m.Tasks[task.TaskId].TaskStatus = COMPLETED
	} else {
		m.Tasks[task.TaskId].TaskStatus = ERROR
		log.Fatalf("task: %v \n task phase: %v \n work: %v \n msg: %v",
			task.TaskId, task.TaskPhase, task.WorkerId, args.Msg)
	}
	return nil
}

/**
RPC CALL END
*/

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) RegisterMapTask() {
	m.TaskPhase = MapPhase
	m.Tasks = make([]Task, m.fileNum)
	//这里只初始化状态为空闲，等work来取task的时候再赋值workId和startTime
	for i := 0; i < m.fileNum; i += 1 {
		m.Tasks[i] = Task{
			TaskId:     i,
			DataSource: m.files[i],
			TaskPhase:  MapPhase,
			TaskStatus: IDLE,
		}
	}
}

func (m *Master) RegisterReduceTask() {
	m.TaskPhase = ReducePhase
	m.Tasks = make([]Task, m.nReduce)
	//和73同理
	for i := 0; i < m.nReduce; i += 1 {
		m.Tasks[i] = Task{
			TaskId: i,
			//DataSource: m.files[i],
			TaskPhase:  ReducePhase,
			TaskStatus: IDLE,
		}
	}
}

func (m *Master) run() {
	for !m.done {
		//go m.schedule()
		//time.Sleep(100 * time.Millisecond)
		mapAll := true
		//遍历任务，如果发现有空闲的任务就去调度他
		//这里找空闲的任务，因为一个任务如果error了会被置为空闲
		for i := 0; i < len(m.Tasks); i++ {
			if m.Tasks[i].TaskStatus == IDLE || m.Tasks[i].TaskStatus == ERROR || m.Tasks[i].TaskStatus == RUNNING {
				mapAll = false
				go m.schedule(i)
			}
		}
		if mapAll {
			if m.TaskPhase == MapPhase {
				m.RegisterReduceTask()
			} else if m.TaskPhase == ReducePhase {
				m.done = true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *Master) schedule(index int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return
	}

	task := m.Tasks[index]

	switch task.TaskStatus {
	case IDLE:
		m.done = false
	case RUNNING:
		m.done = false
		if time.Now().Sub(task.TaskStartTime) > MaxTaskRunTime {
			m.Tasks[index].TaskStatus = IDLE
		}
	case ERROR:
		m.done = false
		m.Tasks[index].TaskStatus = IDLE
	default:
		panic("t.status err")
	}

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// init Master
	m := Master{}
	m.files = files
	m.fileNum = len(files)
	m.nReduce = nReduce
	m.done = false

	// init Map Task
	m.RegisterMapTask()
	// run Master
	go m.run()
	m.server()
	return &m
}
