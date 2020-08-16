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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 0 1 2 3 4 5 6 7 8
	//nextIndex[i] = 6
	//args.PrevLogIndex = 5
	//args.Entries = 6 7 8

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, but his term is small than me, I will reject him", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// leader的Term是up-to-date
	rf.currentTerm = args.Term
	rf.changeRole(FOLLOWER)

	// 2. Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	// If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log,
	// you should handle it the same as if you did have that entry but the term did not match
	// mismatch
	// len(rf.logEntries)-1 < args.PrevLogIndex 表示如果我的最后一个Log都不是prevLogIndex, 即leader的prevLogIndex超出了我的最大logIndex
	// rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm 表示在prevLogIndex这个位置我和leader的log的term不一致
	if len(rf.logEntries)-1 < args.PrevLogIndex || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 3.If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 找传过来的Logs和我本地有没有不同，找到不同的index
	conflictIndex := -1
	for index, entry := range args.Entries {
		if args.PrevLogIndex+1+index < len(rf.logEntries) &&
			rf.logEntries[args.PrevLogIndex+1+index].Term != entry.Term {
			conflictIndex = index
		}
	}
	if conflictIndex != -1 {
		rf.logEntries = rf.logEntries[:args.PrevLogIndex+1+conflictIndex]
		rf.logEntries = append(rf.logEntries, args.Entries[conflictIndex:]...)

	}

	// 4. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntry := len(rf.logEntries) - 1
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastNewEntry)))
		rf.applyMsg()
	}

}
