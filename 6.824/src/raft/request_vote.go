package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}
	//condition1 votedFor is null or candidateId
	condition1 := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	//condition2 candidate's log is up-to-date
	//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//If the logs end with the same term, then whichever log is longer is more up-to-date.
	lastLogIndex := len(rf.logEntries) - 1
	condition2 := args.LastLogTerm >= rf.logEntries[lastLogIndex].Term ||
		(args.LastLogTerm == rf.logEntries[lastLogIndex].Term && lastLogIndex >= args.LastLogIndex)
	if condition1 && condition2 {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.electionTime.Reset(randomizedElectionTimeouts())
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}
