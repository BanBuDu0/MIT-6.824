package kvraft

import (
	"../labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader    int
	clientId  int64
	serialNum int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.serialNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	serverId := ck.leader
	curSerialNum := atomic.AddInt32(&ck.serialNum, 1)
	for {
		server := ck.servers[serverId]
		args := &GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			SerialNum: curSerialNum,
		}
		reply := &GetReply{}
		ok := server.Call("KVServer.Get", args, reply)
		if ok == false {
			serverId = int(nrand()) % len(ck.servers)
			continue
		}
		if reply.IsLeader == false {
			serverId = reply.LeaderId
			ck.leader = reply.LeaderId
			continue
		}
		return reply.Value
	}

	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	serverId := ck.leader
	curSerialNum := atomic.AddInt32(&ck.serialNum, 1)
	for {
		server := ck.servers[serverId]
		args := &PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClientId:  ck.clientId,
			SerialNum: curSerialNum,
		}
		reply := &PutAppendReply{}
		ok := server.Call("KVServer.PutAppend", args, reply)
		if ok == false {
			serverId = int(nrand()) % len(ck.servers)
			continue
		}
		if reply.IsLeader == false {
			serverId = reply.LeaderId
			ck.leader = reply.LeaderId
			continue
		}
		//	TODO PutAppend 成功
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
