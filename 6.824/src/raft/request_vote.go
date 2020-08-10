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
	// term更新的，index更新的
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
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}
	//condition1 votedFor is null or candidateId
	condition1 := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	//condition2 candidate's log is up-to-date
	condition2 := args.Term >= rf.currentTerm && args.LastLogIndex >= rf.commitIndex
	if condition1 && condition2 {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
	}
}
