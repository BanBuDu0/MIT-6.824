package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
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

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	nMap       int
	nReduce    int
}

func (w *work) registerWork() error {
	args := RegisterArgs{}
	reply := RegisterReply{}
	e := call("Master.Register", &args, &reply)
	if !e {
		return errors.New("RPC CALL Master.GetTask ERROR")
	}
	w.id = reply.WorkId
	w.nReduce = reply.NReduce
	w.nMap = reply.NMap
	return nil
}

func (w *work) start() {
	for {
		task, e := w.requestTask()
		if e != nil {
			panic(e.Error())
		}
		w.doTask(task)
	}
}

func (w *work) requestTask() (*Task, error) {
	args := GetTaskArgs{
		WorkId: w.id,
	}
	reply := GetTaskReply{}
	e := call("Master.GetTask", &args, &reply)
	if !e {
		return nil, errors.New("RPC CALL Master.GetTask ERROR")
	}
	return reply.Task, nil
}

func (w *work) doTask(t *Task) {
	switch t.TaskPhase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic("ERROR TASK PHASE")
	}
}

/**
当running状态的task运行出错或运行结束的时候调用
*/
func (w *work) updateTask(t *Task, f bool, m string) error {
	args := UpdateTaskArgs{
		Task:   t,
		Finish: f,
		Msg:    m,
	}
	reply := GetTaskReply{}
	e := call("Master.UpdateTask", &args, &reply)
	if !e {
		return errors.New("RPC CALL Master.UpdateTask ERROR")
	}
	return nil
}

func (w *work) doMapTask(t *Task) {
	file, err := os.Open(t.DataSource)
	if err != nil {
		msg := "cannot open " + t.DataSource
		_ = w.updateTask(t, false, msg)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		msg := "cannot read " + t.DataSource
		_ = w.updateTask(t, false, msg)
		return
	}
	file.Close()
	kva := w.mapFunc(t.DataSource, string(content))

	pwd, _ := os.Getwd()
	files := make([]*os.File, w.nReduce)
	fileNames := make([]string, w.nReduce)
	for i := 0; i < w.nReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v-*", t.TaskId, i)
		intermediateFile, err := ioutil.TempFile(pwd, intermediateFileName)
		if err != nil {
			msg := "Cannot create temp inter file: " + intermediateFileName
			_ = w.updateTask(t, false, msg)
			for j := 0; j < i; j++ {
				_ = os.Remove(fileNames[j])
			}
			return
		}
		files[i] = intermediateFile
		fileNames[i] = intermediateFile.Name()
	}

	//保持中间结果
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % w.nReduce
		interFile := files[reduceIndex]

		encoder := json.NewEncoder(interFile)
		err = encoder.Encode(kv)
		if err != nil {
			msg := "Cannot encode " + kv.Key + ": " + kv.Value + "; error: " + err.Error()
			_ = w.updateTask(t, false, msg)
			for i := 0; i < w.nReduce; i++ {
				_ = os.Remove(fileNames[i])
			}
			return
		}
	}
	//关闭所有文件
	for i := 0; i < w.nReduce; i++ {
		err := files[i].Close()
		if err != nil {
			msg := "Cannot close " + files[i].Name() + "; error: " + err.Error()
			_ = w.updateTask(t, false, msg)
			for i := 0; i < w.nReduce; i++ {
				_ = os.Remove(fileNames[i])
			}
			return
		}
	}
	_ = w.updateTask(t, true, "")
}

func (w *work) doReduceTask(t *Task) {
	var intermediate []KeyValue
	for i := 0; i < w.nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, t.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			msg := "Cannot open " + fileName
			_ = w.updateTask(t, false, msg)
			return
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	//output
	outputFileName := fmt.Sprintf("mr-out-%d-*", t.TaskId)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		msg := "Cannot create " + outputFileName
		_ = w.updateTask(t, false, msg)
		_ = os.Remove(outputFileName)
		return
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reduceFunc(intermediate[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outputFile.Close()
	_ = w.updateTask(t, true, "")
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
	e := work.registerWork()
	if e != nil {
		panic("Register worker Error")
	}
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
