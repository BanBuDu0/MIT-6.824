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

import "bytes"
import "../labgob"

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
	SnapshotData []byte
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
	HeartBeatTimeout = time.Millisecond * 120 // 心跳包间隔
)

func randomizedElectionTimeouts() time.Duration {
	return ElectionTimeout + time.Duration(rand.Int63())%ElectionTimeout
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//2A
	stopCh       chan struct{}
	mRole        Role // 当前角色
	currentTerm  int  // Persistent state
	votedFor     int  // =-1 表示为空 Persistent state
	electionTime *time.Timer
	appendTime   *time.Timer

	//2B
	logEntries  []Entry // Persistent state
	commitIndex int
	lastApplied int
	// only leader
	nextIndex  []int // 当前最后一个log的index+1
	matchIndex []int //复制到其他server的log entry的index
	applyCh    chan ApplyMsg

	//3B
	//the index of the last entry in the log that the snapshot replaces (the last entry the state machine had applied)
	LastIncludedIndex int
	//the term of last entry
	LastIncludedTerm int
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
	rf.persister.SaveRaftState(rf.genPersistData())
}

func (rf *Raft) genPersistData() []byte {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		_, _ = DPrintf("encode rf.currentTerm error")
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		_, _ = DPrintf("encode rf.voteFor error")
	}
	err = e.Encode(rf.logEntries)
	if err != nil {
		_, _ = DPrintf("encode rf.logEntries error")
	}
	err = e.Encode(rf.LastIncludedTerm)
	if err != nil {
		_, _ = DPrintf("encode rf.LastIncludedTerm error")
	}
	err = e.Encode(rf.LastIncludedIndex)
	if err != nil {
		_, _ = DPrintf("encode rf.LastIncludedIndex error")
	}
	data := w.Bytes()
	return data
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var logEntries []Entry
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&logEntries) != nil || d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		_, _ = DPrintf("readPersist error")
	} else {
		rf.mu.Lock()
		rf.logEntries = logEntries
		rf.votedFor = voteFor
		rf.currentTerm = currentTerm
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) DoSnapshot(lastApplyIndex int, serverData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastApplyIndex <= rf.LastIncludedIndex {
		return
	}

	DPrintf("server: %d, lastApplyIndex: %d, LastIncludedIndex: %d, log: %v", rf.me, lastApplyIndex, rf.LastIncludedIndex, len(rf.logEntries))

	rf.LastIncludedTerm = rf.logEntries[rf.getRelativeIndex(lastApplyIndex)].Term
	rf.logEntries = append(make([]Entry, 0), rf.logEntries[rf.getRelativeIndex(lastApplyIndex):]...)
	//rf.logEntries = rf.logEntries[rf.getRelativeIndex(lastApplyIndex):]
	rf.LastIncludedIndex = lastApplyIndex
	DPrintf("server: %d, lastApplyIndex: %d, LastIncludedIndex: %d, log: %v", rf.me, lastApplyIndex, rf.LastIncludedIndex, len(rf.logEntries))
	rf.persister.SaveStateAndSnapshot(rf.genPersistData(), serverData)
}

func (rf *Raft) getAbsoluteIndex(index int) int {
	return rf.LastIncludedIndex + index
}

func (rf *Raft) getRelativeIndex(index int) int {
	return index - rf.LastIncludedIndex
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) changeRole(role Role) {
	switch role {
	case FOLLOWER:
		rf.appendTime.Stop()
		rf.electionTime.Reset(randomizedElectionTimeouts())
		rf.votedFor = -1
		rf.mRole = role
		rf.persist()
	case CANDIDATE:
		rf.votedFor = rf.me
		rf.electionTime.Reset(randomizedElectionTimeouts())
		rf.currentTerm += 1
		rf.mRole = role
		rf.persist()
	case LEADER:
		if rf.mRole != CANDIDATE {
			DPrintf("My rolw is not CANDIDATE, I am %v, so return", rf.mRole)
			return
		}
		for index := range rf.nextIndex {
			rf.nextIndex[index] = rf.getAbsoluteIndex(len(rf.logEntries))
		}

		for index := range rf.matchIndex {
			rf.matchIndex[index] = rf.LastIncludedIndex
		}
		rf.mRole = role
		_, _ = DPrintf("id: %v become leader", rf.me)
		rf.electionTime.Stop()
		rf.appendTime.Reset(HeartBeatTimeout)
		go rf.callAppendEntries()
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

//给每个peer发送添加entry的RPC
func (rf *Raft) callAppendEntries() {
	_, _ = DPrintf("start callAppendEntries, raft: %+v", rf.me)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(server int) {
			for {
				rf.mu.Lock()
				if rf.mRole != LEADER {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[server] - 1

				if prevLogIndex < rf.LastIncludedIndex {
					rf.mu.Unlock()
					rf.callInstallSnapshot(server)
					return
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.logEntries[rf.getRelativeIndex(prevLogIndex)].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      append(make([]Entry, 0), rf.logEntries[rf.getRelativeIndex(rf.nextIndex[server]):]...),
				}

				DPrintf("%v send AppendEntries RPC to %v, nextIndex = %v, prevLogIndex = %d, commitIndex = %d， log len: %d", rf.me, server, rf.nextIndex, prevLogIndex, rf.commitIndex, len(args.Entries))
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				// 这里为什么又可以加锁呢，因为这是在go func里面，外面的锁不会进入到go func里面来，
				// 又因为上面的RPC肯定比本地函数退栈更耗时间，所以这里再锁的时候前面changeRole函数外面的锁早就已经解开了
				rf.mu.Lock()

				//students-guide-to-raft, Term confusion, what you should do when you get old RPC replies.
				//compare the current term with the term you sent in your original RPC.
				//If the two are different, drop the reply and return.
				if !ok || rf.mRole != LEADER || rf.currentTerm != args.Term {
					_, _ = DPrintf("%d AppendEntries early return %d, !ok: %v, mRole: %v, condition3: %v",
						rf.me, server, !ok, rf.mRole != LEADER, rf.currentTerm != args.Term)
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: %d's term is large than me, and i will change term form %v to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, server, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.changeRole(FOLLOWER)
					rf.mu.Unlock()
					return
				}

				_, _ = DPrintf("%d AppendEntries Reply from peer: %v, reply: %+v", rf.me, server, reply)
				if !reply.Success {
					// 如果append失败的话就往前减少nextIndex继续append
					// based on students-guide-to-raft accelerated log backtracking optimization
					rf.nextIndex[server] = reply.ConflictIndex
					if reply.ConflictTerm != -1 {
						_, _ = DPrintf("%d peer: %v not match, nextIndex: %v", rf.me, server, rf.nextIndex[server])
						for i := args.PrevLogIndex; i >= rf.LastIncludedIndex+1; i-- {
							if rf.logEntries[rf.getRelativeIndex(i-1)].Term == reply.ConflictTerm {
								rf.nextIndex[server] = i
								break
							}
						}
					}
					rf.mu.Unlock()
				} else {
					//base on students-guide-to-raft
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.matchIndex[rf.me] = rf.getAbsoluteIndex(len(rf.logEntries) - 1)
					_, _ = DPrintf("leader: %d, i: %d, apply msg, matchIndex: %+v, nextIndex: %+v, lastApply: %d, commitIndex: %d",
						rf.me, server, rf.matchIndex, rf.nextIndex, rf.lastApplied, rf.commitIndex)
					for i := rf.getAbsoluteIndex(len(rf.logEntries) - 1); i > rf.commitIndex; i-- {
						matched := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= i {
								matched++
								// log复制到了一半以上的peer
								if matched > len(rf.peers)/2 &&
									rf.logEntries[rf.getRelativeIndex(i)].Term == rf.currentTerm {
									rf.commitIndex = i
									rf.applyMsg()
									break
								}
							}
						}
					}
					rf.mu.Unlock()
					return
				}
			}
		}(index)
	}
}

func (rf *Raft) callInstallSnapshot(server int) {
	rf.mu.Lock()
	DPrintf("%v send InstallSnapshot RPC to %v", rf.me, server)
	arg := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Snapshot:          rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &arg, &reply)
	if !ok {
		DPrintf("%v callInstallSnapshot error %d", rf.me, server)
		return
	}
	DPrintf("callInstallSnapshot success, %d to %d", rf.me, server)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.mRole != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("callInstallSnapshot, id: %d, voteFor: %v, role: %v, term: %v: someone's term is large than me, and i will change term form %v to %v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.changeRole(FOLLOWER)
		return
	}

	rf.matchIndex[server] = arg.LastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
}

func (rf *Raft) applyMsg() {
	// apply to state machine
	// 因为这里使用的是go func，所以改变lastApplied的时候需要重新加锁
	//go func() {
	//	rf.mu.Lock()
	if rf.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.LastIncludedIndex
	}

	if rf.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.LastIncludedIndex
	}

	if rf.commitIndex > rf.lastApplied {
		startIndex := rf.lastApplied + 1
		endIndex := rf.commitIndex + 1
		logs := rf.logEntries[rf.getRelativeIndex(startIndex):rf.getRelativeIndex(endIndex)]
		for index, log := range logs {
			applyArgs := ApplyMsg{
				Command:      log.Commend,
				CommandValid: true,
				CommandIndex: startIndex + index,
			}

			rf.applyCh <- applyArgs
			rf.lastApplied++
			DPrintf("id: %d, voteFor: %v, role: %v, term: %v, lastApplied: %v, commitIndex: %v: Apply Msg %+v", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, rf.lastApplied, rf.commitIndex, applyArgs)
		}
	}
	//rf.mu.Unlock()
	//}()
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
		_, _ = DPrintf("%d Start a command %+v", rf.me, command)
		rf.logEntries = append(rf.logEntries, Entry{
			Term:    term,
			Commend: command,
		})
		index = rf.getAbsoluteIndex(len(rf.logEntries) - 1)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.appendTime.Reset(HeartBeatTimeout)
		go rf.callAppendEntries()
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
	rf.stopCh = make(chan struct{})
	rf.nextIndex = make([]int, len(rf.peers))
	//第一次添加log就从1开始添加
	//这样的话初始commitIndex = matchIndex = 0
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	// 因为nextIndex初始化为1，所以创建一个空的log占位置
	rf.logEntries = make([]Entry, 1)
	rf.applyCh = applyCh

	_, _ = DPrintf("raft init : %+v", rf)

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
					rf.appendTime.Reset(HeartBeatTimeout)
					go rf.callAppendEntries()
				}
				rf.mu.Unlock()

			}
		}

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
