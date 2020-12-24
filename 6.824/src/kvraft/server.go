package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // 操作的类型
	ClientId  int64
	SerialNum int32
	// 操作的 key value
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	threshold    int

	// Your definitions here.

	//Your server keeps track of the latest sequence number it has seen for each client,
	//and simply ignores any operation that it has already seen.
	clientLastSeq map[int64]int32   //记录每个client的最后一个sequence num
	db            map[string]string //记录key-value数据对
	agreeChs      map[int]chan Op
	stopCh        chan struct{}

	//3B
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//按照论文里所说，即使是get也不能直接从 leader server里面取，需要和其他server通信一轮，确保自己真的是最新的leader
	// Your code here.
	op := Op{
		Type:      GetOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
	}

	isLeader := kv.waitOp(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	value, ok := kv.db[op.Key]
	kv.mu.Unlock()

	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = value
	//这里是没发生错误的正确返回
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Key:       args.Key,
		Value:     args.Value,
	}

	isLeader := kv.waitOp(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) waitOp(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	agreeCh, ok := kv.agreeChs[index]
	if !ok {
		agreeCh = make(chan Op, 1)
		kv.agreeChs[index] = agreeCh
	}
	kv.mu.Unlock()
	_, _ = DPrintf("waitting agreeCh, kvserver: %d", kv.me)

	select {
	case agreeOp := <-agreeCh:
		_, _ = DPrintf("waitting agreeCh success, kvserver: %d", kv.me)
		if !(op.ClientId == agreeOp.ClientId && op.SerialNum == agreeOp.SerialNum) {
			return false
		}
	case <-time.After(Timeout):
		return false
	}
	return true
}

func (kv *KVServer) waitApply() {
	for {
		select {
		case <-kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				kv.mu.Lock()
				lastSeq, ok := kv.clientLastSeq[op.ClientId]
				if !ok || op.SerialNum > lastSeq {
					kv.clientLastSeq[op.ClientId] = op.SerialNum
					switch op.Type {
					case PutOp:
						kv.db[op.Key] = op.Value
					case AppendOp:
						kv.db[op.Key] += op.Value
					case GetOp:

					default:
						panic("ERROR OP type")
					}
					go kv.doSnapshot(msg.CommandIndex)
				}
				agreeCh, ok := kv.agreeChs[msg.CommandIndex]
				if ok {
					agreeCh <- op
				}
				kv.mu.Unlock()
			} else {
				switch msg.Command.(string) {
				case "Snapshot":
					_, _ = DPrintf("after install snapshot, reinstall snapshot in kv server")
					kv.readSnapshot(msg.SnapshotData)
				}
			}
		}
	}
}

func (kv *KVServer) doSnapshot(lastApplyIndex int) {
	kv.mu.Lock()
	if kv.maxraftstate < 0 || kv.persister.RaftStateSize() < kv.threshold {
		kv.mu.Unlock()
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_, _ = DPrintf("server: %d, start do snapshot, maxraftstate: %d, kv.RaftStateSize: %d, kv.snapshotsize: %d",
		kv.me, kv.maxraftstate, kv.persister.RaftStateSize(), kv.persister.SnapshotSize())
	err1 := e.Encode(kv.db)
	err2 := e.Encode(kv.clientLastSeq)
	kv.mu.Unlock()
	if err1 != nil || err2 != nil {
		_, _ = DPrintf("encode error")
	}
	kv.rf.DoSnapshot(lastApplyIndex, w.Bytes())
	_, _ = DPrintf("server: %d, start do snapshot, maxraftstate: %d, kv.RaftStateSize: %d, kv.snapshotsize: %d",
		kv.me, kv.maxraftstate, kv.persister.RaftStateSize(), kv.persister.SnapshotSize())
}

// read之前需要先搞明白snapshot里面有什么
func (kv *KVServer) readSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) <= 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.db) != nil || d.Decode(&kv.clientLastSeq) != nil {
		_, _ = DPrintf("kv server: %v, readSnapshot error", kv.me)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.stopCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 整个 KV-server 跟 raft 模块交互的只有两个地方
// 1. raft.Start(command interface{}), 传入的是需要执行的命令，返回的是该命令在 raft 中的 index, 该命令所处的Term，和该raft server是否leader
// 2. raft commit 日志之后，向applyCh写入信息，包括Start传入的命令，commit是否成功和 command在 raft中的index
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.threshold = int(float32(maxraftstate) * float32(0.8))
	kv.persister = persister
	DPrintf("maxraftstate: %d", maxraftstate)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.clientLastSeq = make(map[int64]int32)
	kv.db = make(map[string]string)
	kv.stopCh = make(chan struct{})
	kv.agreeChs = make(map[int]chan Op)
	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.waitApply()
	return kv
}
