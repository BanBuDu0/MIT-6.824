package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type work struct {
	id         int
	mapFunc    func(string, string) []KeyValue
	reduceFunc func(string, []string) string
}

func (w *work) registerWork() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	call("Master.Register", &args, &reply)
	w.id = reply.workId
}

func (w *work) start() {
	for {
		task := w.requestTask()
		w.doTask()
	}
}

func (w *work) requestTask() Task {

}

func (w *work) doTask() {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	work := work{}
	work.mapFunc = mapf
	work.reduceFunc = reducef
	//每个worker启动的时候先去master注册
	work.registerWork()
	//启动worker
	work.start()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
