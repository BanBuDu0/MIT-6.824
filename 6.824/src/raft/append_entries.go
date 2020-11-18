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
	//_, _ = DPrintf("id: %d, voteFor: %v, role: %v, term: %v: get heartbeat from %v, changeROle to follower and reset electionTime", rf.me, rf.votedFor, rf.mRole, rf.currentTerm, args.LeaderID)

	// LastIncludedIndex的一定是apply过的msg，所以如果传过来的PrevLogIndex比apply过的msg要小，直接返回就行了
	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 2. Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	// If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log,
	// you should handle it the same as if you did have that entry but the term did not match
	// mismatch
	// len(rf.logEntries)-1 < args.PrevLogIndex 表示如果我的最后一个Log都不是prevLogIndex, 即leader的prevLogIndex超出了我的最大logIndex
	// rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm 表示在prevLogIndex这个位置我和leader的log的term不一致
	// 2.1 如果在log里面找不到prevLogIndex
	if len(rf.logEntries)-1 < rf.getRelativeIndex(args.PrevLogIndex) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.getAbsoluteIndex(len(rf.logEntries))
		return
	}

	// 2.2 如果log里面有prevLogIndex，但是Term不匹配
	conflictTerm := -1
	if rf.logEntries[rf.getRelativeIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		conflictTerm = rf.logEntries[rf.getRelativeIndex(args.PrevLogIndex)].Term
	}
	if conflictTerm != -1 {
		for i := args.PrevLogIndex; i >= rf.LastIncludedIndex; i-- {
			if rf.logEntries[rf.getRelativeIndex(i)].Term != conflictTerm || i == rf.LastIncludedIndex {
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
		if rf.getAbsoluteIndex(len(rf.logEntries)-1) < args.PrevLogIndex+1+index ||
			rf.logEntries[rf.getRelativeIndex(args.PrevLogIndex+1+index)].Term != entry.Term {
			conflictIndex = index
			break
		}
	}
	if conflictIndex != -1 {
		_, _ = DPrintf("id: %d, term: %v: get AppendEntries from %v, rely on #3, find conflictIndex: %d, delete the existing entry and all that follow it", rf.me, rf.currentTerm, args.LeaderID, conflictIndex)
		rf.logEntries = rf.logEntries[:rf.getRelativeIndex(args.PrevLogIndex+1+conflictIndex)]
		rf.logEntries = append(rf.logEntries, args.Entries[conflictIndex:]...)
		rf.persist()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	_, _ = DPrintf("peer: %v, reply true", rf.me)
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 这里检测Min是必要的，因为raft只会在检测到conflictIndex的时候才会修改自己的日志，
	// 如果leader传过来的日志都匹配，但是传过来的日志之后还有日志，但是不匹配的，如果不检测min就会把后面的这些不匹配的日志也commit了
	if args.LeaderCommit > rf.commitIndex {
		_, _ = DPrintf("id: %d, term: %v: get AppendEntries from %v, rely on #5, set commitIndex = %v, and apply msg", rf.me, rf.currentTerm, args.LeaderID, int(math.Min(float64(args.LeaderCommit), float64(len(rf.logEntries)-1))))
		lastNewEntry := rf.getAbsoluteIndex(len(rf.logEntries) - 1)
		if args.LeaderCommit < lastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntry
		}
		rf.applyMsg()
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
					if reply.ConflictIndex != -1 {
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
