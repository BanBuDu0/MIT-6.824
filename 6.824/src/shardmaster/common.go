package shardmaster

import "time"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
// 一共有10个分片，每个分片对应一个string类型的server数组
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	JoinOp         = "JOIN"
	LeaveOp        = "LEAVE"
	MoveOp         = "MOVE"
	QueryOp        = "Query"
)
const Timeout = 500 * time.Millisecond

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	SerialNum int32
	ClientId  int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	SerialNum int32
	ClientId  int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	SerialNum int32
	ClientId  int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	SerialNum int32
	ClientId  int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
