package raft

type AppendEntriesArgs struct {
	Term         int        // leader's currentTerm
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // currentTerm of prevLogIndex entry
	Entries      []LogEntry // log Entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int  // current currentTerm, for leader to update itself
	Success       bool // true if follower containes entry matching prevLogIndex and prevLogTerm
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Receiver implementation, reference raft paper's Figure 2:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = true

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// reset election timer even log does not match
	// args.LeaderId is the current term's Leader
	rf.electionTimer.Reset(randElectionTimeout())

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	_, lastLogIndex := rf.lastLogTermIndex()
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.logEntries)
		reply.ConflictTerm = None
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if rf.logEntries[(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.logEntries[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for rf.logEntries[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	// 4. Append any new entries not already in the log compare from rf.log[args.PrevLogIndex + 1]
	unmatchedLogIndex := None
	for idx := range args.Entries {
		if len(rf.logEntries) < (args.PrevLogIndex+2+idx) ||
			rf.logEntries[(args.PrevLogIndex+1+idx)].Term != args.Entries[idx].Term {
			unmatchedLogIndex = idx
			break
		}
	}

	if unmatchedLogIndex != None {
		// there are unmatch entries
		// truncate unmatch Follower entries, and apply Leader entries
		rf.logEntries = rf.logEntries[:(args.PrevLogIndex + 1 + unmatchedLogIndex)]
		rf.logEntries = append(rf.logEntries, args.Entries[unmatchedLogIndex:]...)
	}

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(min(args.LeaderCommit, len(rf.logEntries)-1))
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) heartbeats() {
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := rf.buildAppendEntriesArgs(peer)
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if ok {
				rf.processAppendEntriesReply(peer, &args, &reply)
			}
		}(peer)
	}
}

func (rf *Raft) buildAppendEntriesArgs(peer int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	nextIndex := rf.nextIndex[peer]
	if lastLogIndex >= nextIndex {
		prevLogIndex := nextIndex - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.logEntries[prevLogIndex].Term
		args.Entries = rf.logEntries[nextIndex:]
	} else {
		args.PrevLogIndex = lastLogIndex
		args.PrevLogTerm = lastLogTerm
	}

	return args
}

func (rf *Raft) processAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	// If last log index ≥ nextIndex for a follower: send
	// AppendEntries RPC with log entries starting at nextIndex
	// • If successful: update nextIndex and matchIndex for
	// follower (§5.3)
	// • If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry (§5.3)
	if reply.Success {
		// successfully replicated args.Entries
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		for N := (len(rf.logEntries) - 1); N > rf.commitIndex; N-- {
			count := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= N {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				rf.setCommitIndex(N)
				break
			}
		}

	} else {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex

			// if term found, override it to
			// the first entry after entries in ConflictTerm
			if reply.ConflictTerm != None {
				for i := args.PrevLogIndex; i >= 1; i-- {
					if rf.logEntries[i-1].Term == reply.ConflictTerm {
						// in next trial, check if log entries in ConflictTerm matches
						rf.nextIndex[peer] = i
						break
					}
				}
			}
		}
	}

}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// if commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	if rf.commitIndex > rf.lastApplied {
		entriesToApply := append([]LogEntry{}, rf.logEntries[(rf.lastApplied+1):(rf.commitIndex+1)]...)

		go func(startIdx int, entries []LogEntry) {
			for idx, entry := range entries {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = startIdx + idx
				rf.applyCh <- msg

				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}
