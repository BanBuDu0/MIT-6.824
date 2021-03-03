package shardmaster

import (
	"../raft"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs       []Config // indexed by config num
	stopCh        chan struct{}
	clientLastSeq map[int64]int32 //记录每个client的最后一个sequence num
	agreeChs      map[int]chan Op
}

type Op struct {
	// Your data here.
	Type      string
	ClientId  int64
	SerialNum int32
	Args      interface{}
}

func (sm *ShardMaster) waitOp(op Op) bool {
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	agreeCh, ok := sm.agreeChs[index]
	if !ok {
		agreeCh = make(chan Op, 1)
		sm.agreeChs[index] = agreeCh
	}
	sm.mu.Unlock()

	select {
	case agreeOp := <-agreeCh:
		if !(op.ClientId == agreeOp.ClientId && op.SerialNum == agreeOp.SerialNum) {
			return false
		}
	case <-time.After(Timeout):
		return false
	}
	return true

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:      JoinOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Args:      *args,
	}
	isLeader := sm.waitOp(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:      LeaveOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Args:      *args,
	}
	isLeader := sm.waitOp(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      MoveOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Args:      *args,
	}
	isLeader := sm.waitOp(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	}
}

//Query OK
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      QueryOp,
		ClientId:  args.ClientId,
		SerialNum: args.SerialNum,
		Args:      *args,
	}
	isLeader := sm.waitOp(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num == -1 || args.Num >= len(sm.configs) {
		//If the number is -1 or bigger than the biggest known configuration number,
		//the shardctrler should reply with the latest configuration.
		reply.Config = sm.getCopyOfConf(len(sm.configs) - 1)
	} else {
		reply.Config = sm.getCopyOfConf(args.Num)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.clientLastSeq = make(map[int64]int32)
	sm.agreeChs = make(map[int]chan Op)
	sm.stopCh = make(chan struct{})

	go sm.waitApply()
	return sm
}

func (sm *ShardMaster) waitApply() {
	for {
		select {
		case <-sm.stopCh:
			return
		case msg := <-sm.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				//先检测op是不是旧的
				sm.mu.Lock()
				lastSeq, ok := sm.clientLastSeq[op.ClientId]
				if !ok || op.SerialNum > lastSeq {
					sm.clientLastSeq[op.ClientId] = op.SerialNum
					switch op.Type {
					case JoinOp:
						sm.doJoin(op.Args.(JoinArgs))
					case LeaveOp:
						sm.doLeave(op.Args.(LeaveArgs))
					case MoveOp:
						sm.doMove(op.Args.(MoveArgs))
					case QueryOp:
					default:
						panic("ERROR OP type")
					}
				}
				agreeCh, ok := sm.agreeChs[msg.CommandIndex]
				if ok {
					agreeCh <- op
				}
				sm.mu.Unlock()
			}
		}
	}
}

func (sm *ShardMaster) doJoin(args JoinArgs) {
	conf := sm.getCopyOfConf(len(sm.configs) - 1)
	conf.Num++
	for gid, servers := range args.Servers {
		temp := make([]string, len(servers))
		copy(temp, servers)
		conf.Groups[gid] = temp
	}
	sm.reShard(&conf)
	sm.configs = append(sm.configs, conf)
}

func (sm *ShardMaster) doLeave(args LeaveArgs) {
	conf := sm.getCopyOfConf(len(sm.configs) - 1)
	conf.Num++
	for _, gid := range args.GIDs {
		delete(conf.Groups, gid)
	}
	sm.reShard(&conf)
	sm.configs = append(sm.configs, conf)
}

func (sm *ShardMaster) doMove(args MoveArgs) {
	conf := sm.getCopyOfConf(len(sm.configs) - 1)
	conf.Num++
	if conf.Shards[args.Shard] != args.GID {
		conf.Shards[args.Shard] = args.GID
	}
	sm.configs = append(sm.configs, conf)
}

/**
reShard的作用是，在Join和Leave之后，重新对Shards进行分配
*/
func (sm *ShardMaster) reShard(conf *Config) {
	//totalGroup := len(conf.Groups)
	totalServer := 0
	for _, servers := range conf.Groups {
		totalServer += len(servers)
	}

	right := 0
	for gid, servers := range conf.Groups {
		serverNum := len(servers)
		temp := NShards * serverNum / totalServer
		if temp == 0 {
			temp = 1
		}
		for i := 0; i < temp; i++ {
			if right >= NShards {
				break
			}
			conf.Shards[right] = gid
			right++
		}
	}
}

func (sm *ShardMaster) getCopyOfConf(index int) Config {
	var temp [NShards]int
	for i := 0; i < NShards; i++ {
		temp[i] = sm.configs[index].Shards[i]
	}
	conf := Config{
		Num:    sm.configs[index].Num,
		Shards: temp,
		Groups: make(map[int][]string),
	}
	for key, val := range sm.configs[index].Groups {
		conf.Groups[key] = append([]string{}, val...)
	}
	return conf
}
