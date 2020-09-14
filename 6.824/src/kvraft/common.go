package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const Timeout = 500 * time.Millisecond

type Err string

const (
	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	SerialNum int32
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	SerialNum int32
}

type GetReply struct {
	Err   Err
	Value string
}
