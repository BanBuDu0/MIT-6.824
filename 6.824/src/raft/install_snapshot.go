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
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1.Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}

	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	}

	if len(rf.logEntries) > rf.getRelativeIndex(args.LastIncludedIndex) &&
		rf.logEntries[rf.getRelativeIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		// 6.If existing log entry has same index and term as snapshot’s last included entry,
		// retain log entries following it and reply
		rf.logEntries = rf.logEntries[rf.getRelativeIndex(args.LastIncludedIndex):]
	} else {
		// 7.Discard the entire log
		rf.logEntries = make([]Entry, 1)
		rf.logEntries[0].Term = args.LastIncludedTerm
	}

	// 8.Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.genPersistData(), args.Snapshot)
	reply.Term = rf.currentTerm
	if rf.lastApplied > rf.LastIncludedIndex {
		return
	}
	msg := ApplyMsg{
		CommandValid: false,
		Command:      "Snapshot",
		CommandIndex: rf.LastIncludedIndex,
		SnapshotData: rf.persister.ReadSnapshot(),
	}

	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(msg)
}
