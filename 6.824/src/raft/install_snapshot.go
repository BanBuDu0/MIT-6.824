package raft

// 当 Leader 在 append entry 的时候发现 follower 需要的 log 已经被自己 snapshot 的时候触发
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1.Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}

	// 6.If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	if len(rf.logEntries) > rf.getRelativeIndex(args.LastIncludedIndex) &&
		rf.logEntries[rf.getRelativeIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {

	}

	//TODO
}
