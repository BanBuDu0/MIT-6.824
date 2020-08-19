package raft

import "math"

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 0 1 2 3 4 5 6 7 8
	//nextIndex[i] = 6
	//args.PrevLogIndex = 5
	//args.Entries = 6 7 8

	_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, but his term is small than me, I will reject him", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}

	// leader的Term是up-to-date
	rf.electionTime.Reset(randomizedElectionTimeouts())
	_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, changeROle to follower and reset electionTime", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)

	// 2. Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	// If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log,
	// you should handle it the same as if you did have that entry but the term did not match
	// mismatch
	// len(rf.logEntries)-1 < args.PrevLogIndex 表示如果我的最后一个Log都不是prevLogIndex, 即leader的prevLogIndex超出了我的最大logIndex
	// rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm 表示在prevLogIndex这个位置我和leader的log的term不一致
	//if len(rf.logEntries)-1 < args.PrevLogIndex || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get AppendEntries from %v, but rely on #2, I will reject him", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
	//	reply.Success = false
	//	reply.Term = rf.currentTerm
	//	return
	//}

	// 如果在log里面找不到prevLogIndex
	if len(rf.logEntries)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.logEntries)
		return
	}

	// 如果log里面有prevLogIndex，但是Term不匹配
	conflictTerm := -1
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm = rf.logEntries[args.PrevLogIndex].Term
	}
	if conflictTerm != -1 {
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logEntries[i].Term != conflictTerm || i == 0 {
				reply.Success = false
				reply.Term = rf.currentTerm
				reply.ConflictIndex = i + 1
				reply.ConflictTerm = conflictTerm
				return
			}
		}
	}

	// 3.If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 找传过来的Logs和我本地有没有不同，找到不同的index
	// 这里找的是prevLogIndex之后的log和传过来的
	conflictIndex := -1
	for index, entry := range args.Entries {
		if len(rf.logEntries)-1 < args.PrevLogIndex+1+index ||
			rf.logEntries[args.PrevLogIndex+1+index].Term != entry.Term {
			conflictIndex = index
			break
		}
	}
	if conflictIndex != -1 {
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get AppendEntries from %v, rely on #3, I find a conflict index, i will delete the existing entry and all that follow it", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
		rf.logEntries = rf.logEntries[:args.PrevLogIndex+1+conflictIndex]
		rf.logEntries = append(rf.logEntries, args.Entries[conflictIndex:]...)
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get AppendEntries from %v, rely on #5, I will set my commitIndex = %v, and apply the msg to my state machine", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID, int(math.Min(float64(args.LeaderCommit), float64(len(rf.logEntries)-1))))
		lastNewEntry := len(rf.logEntries) - 1
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntry)))
		rf.applyMsg()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("peer: %v, reply true, and now he is: %+v", rf.me, rf)
}
