package raft

import (
	//"fmt"
	"sync/atomic"
	"time"
)

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

	//fmt.Printf("me: %d, now: %v, lasttime: %v, add: %v", rf.me, time.Now(), rf.lastAppendTime, rf.lastAppendTime.Add(ElectionTimeout))

	if time.Now().Before(rf.lastAppendTime.Add(ElectionTimeout)) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//fmt.Printf("I believe a current leader exists!\n")
		return
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRole(FOLLOWER)
	}
	//condition1 votedFor is null or candidateId
	condition1 := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	//condition2 candidate's log is up-to-date
	//If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	//If the logs end with the same term, then whichever log is longer is more up-to-date.
	lastLogIndex := len(rf.logEntries) - 1
	//condition2 := args.LastLogTerm >= rf.logEntries[lastLogIndex].Term ||
	//	(args.LastLogTerm == rf.logEntries[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex)
	//_, _ = DPrintf("id: %v get request vote args, my term: %v, my lastLogIndex: %v, my lastLogTerm: %v", rf.me, rf.currentTerm, lastLogIndex, rf.logEntries[lastLogIndex].Term)
	// condition2 = true: requester is not up-to-date.
	condition2 := args.LastLogTerm < rf.logEntries[lastLogIndex].Term ||
		(args.LastLogTerm == rf.logEntries[lastLogIndex].Term && args.LastLogIndex < rf.getAbsoluteIndex(lastLogIndex))

	if condition1 && !condition2 {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.electionTime.Reset(randomizedElectionTimeouts())
		rf.persist()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.electionTime.Reset(randomizedElectionTimeouts())
	if rf.mRole == LEADER {
		return
	}

	rf.changeRole(CANDIDATE)

	lastLogIndex := len(rf.logEntries) - 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getAbsoluteIndex(lastLogIndex),
		LastLogTerm:  rf.logEntries[lastLogIndex].Term,
	}
	rf.mu.Unlock()

	var voteNum int32
	voteNum = 1
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//students-guide-to-raft, Term confusion, what you should do when you get old RPC replies.
			//compare the current term with the term you sent in your original RPC.
			//If the two are different, drop the reply and return.
			if !ok || rf.mRole != CANDIDATE || rf.currentTerm != args.Term {
				_, _ = DPrintf("%d RequestVote early return %d, !ok: %v, mRole: %v, condition3: %v",
					rf.me, i, !ok, rf.mRole != CANDIDATE, rf.currentTerm != args.Term)
				return
			}

			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.changeRole(FOLLOWER)
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&voteNum, 1)
				//如果获得票数超过一半了，就当选leader
				DPrintf("%d granted vote from %d", rf.me, i)
				if atomic.LoadInt32(&voteNum) > (int32)(len(rf.peers)/2) {
					rf.changeRole(LEADER)
				}
			}

		}(index)
	}
}
