package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
	IsHeartBeat  bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, but his term is small than me, I will reject him", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.IsHeartBeat {
		reply.Term = args.Term
		reply.Success = true
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, I will change to follower", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, I will change to follower, and i will change term form %v to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	} else {

	}
}
