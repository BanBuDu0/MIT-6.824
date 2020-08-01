package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Role int
type Entry struct {
	Commend interface{}
	Term    int
}

const (
	FOLLOWER  Role = 0
	CANDIDATE Role = 1
	LEADER    Role = 2
)

const (
	ElectionTimeout  = time.Millisecond * 300 // 选举超时时间基础
	HeartBeatTimeout = time.Millisecond * 150 // 心跳包间隔
)

func randomizedElectionTimeouts() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	halfPeerNum int32

	//2A
	stopCh       chan struct{}
	mRole        Role // 当前角色
	currentTerm  int
	votedFor     int // =-1 表示为空
	electionTime *time.Timer
	appendTime   *time.Timer

	//2B
	logEntries  []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.mRole == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//给每个peer发送添加entry的RPC
func (rf *Raft) callAppendEntries(heartBeat bool) {
	_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: start heartbeat", rf.me, rf.votedFor, rf.mRole, rf.currentTerm)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {
			if heartBeat {
				_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: send heartbeat to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, i)
				args := AppendEntriesArgs{
					Term:        rf.currentTerm,
					LeaderID:    rf.me,
					IsHeartBeat: heartBeat,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				if ok {
					_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: call append entries success", rf.me, rf.votedFor, rf.mRole, rf.currentTerm)
					if rf.mRole != LEADER {
						rf.mu.Unlock()
						return
					}
					if !reply.Success {
						_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: someone's term is large than me, and i will change term form %v to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						_, _ = DPrintf("call AppendEntries here change to follow")
						rf.changeRole(FOLLOWER)
					}
				} else {
					_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: call append entries error %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, i)
				}
				rf.mu.Unlock()
			}
		}(index)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if isLeader {
		index = len(rf.logEntries)
		rf.logEntries = append(rf.logEntries, Entry{
			Term:    term,
			Commend: command,
		})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.callAppendEntries(false)
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.electionTime.Reset(randomizedElectionTimeouts())
	_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: start election", rf.me, rf.votedFor, rf.mRole, rf.currentTerm)
	if rf.mRole == LEADER {
		_, _ = DPrintf("%d already leader", rf.me)
		return
	}

	rf.changeRole(CANDIDATE)
	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var voteNum int32
	for index := range rf.peers {
		if index == rf.me {
			atomic.AddInt32(&voteNum, 1)
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			//_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: sendRequestVote to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, i)
			s := rf.sendRequestVote(i, &args, &reply)
			rf.mu.Lock()
			if s {
				_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: sendRequestVote to %v, reply: %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, i, reply)
				if reply.VoteGranted && rf.currentTerm >= reply.Term {
					atomic.AddInt32(&voteNum, 1)
					_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get vote num %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, atomic.LoadInt32(&voteNum))
					//如果获得票数超过一半了，就当选leader
					if atomic.LoadInt32(&voteNum) > rf.halfPeerNum && rf.mRole == CANDIDATE {
						rf.changeRole(LEADER)
					}
				} else {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.changeRole(FOLLOWER)
						rf.electionTime.Stop()
						rf.electionTime.Reset(randomizedElectionTimeouts())
					}
				}

			} else {
				_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: sendRequestVote to %v ERROR", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, i)
			}
			rf.mu.Unlock()
		}(index)
	}
}

func (rf *Raft) changeRole(role Role) {
	temp := rf.mRole
	rf.mRole = role
	switch role {
	case FOLLOWER:
		rf.appendTime.Stop()
		rf.electionTime.Reset(randomizedElectionTimeouts())
		rf.votedFor = -1
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: change %v to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, temp, role)
	case CANDIDATE:
		rf.votedFor = rf.me
		rf.electionTime.Stop()
		rf.electionTime.Reset(randomizedElectionTimeouts())
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: change %v to %v, and I will add 1 to my term, now term is %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, temp, role, rf.currentTerm+1)
		rf.currentTerm += 1
	case LEADER:
		rf.votedFor = -1
		rf.electionTime.Stop()
		rf.appendTime.Reset(HeartBeatTimeout)
		_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: change %v to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, temp, role)
		rf.callAppendEntries(true)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mRole = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTime = time.NewTimer(randomizedElectionTimeouts())
	rf.appendTime = time.NewTimer(HeartBeatTimeout)
	rf.halfPeerNum = (int32)(len(rf.peers) / 2)
	rf.stopCh = make(chan struct{})

	_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: I init my term with %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, rf.currentTerm)

	//election leader
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTime.C:
				rf.startElection()
			}
		}
	}()

	// heartbeats
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.appendTime.C:
				rf.mu.Lock()
				if rf.mRole == LEADER {
					rf.callAppendEntries(true)
					rf.appendTime.Reset(HeartBeatTimeout)
				}
				rf.mu.Unlock()
			}
		}

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
