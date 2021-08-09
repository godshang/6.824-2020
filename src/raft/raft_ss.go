package raft

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// Send the entire snapshot in a single InstallSnapshot RPC. Don't implement Figure 13's offset mechanism for splitting up the snapshot.
	// Offset            int    // byte offset where chunk is positioned in the snapshot file
	// Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) ReplaceLogWithSnapshot(appliedIndex int, kvSnapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if appliedIndex <= rf.snapshottedIndex {
		return
	}

	rf.logEntries = rf.logEntries[rf.getRelativeLogIndex(appliedIndex):]
	rf.snapshottedIndex = appliedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), kvSnapshot)

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.syncSnapshotTo(server)
	}
}

func (rf *Raft) syncSnapshotTo(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshottedIndex,
		LastIncludedTerm:  rf.logEntries[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
		} else {
			if rf.matchIndex[server] < args.LastIncludedIndex {
				rf.matchIndex[server] = args.LastIncludedIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 1. Reply immediately if term < currentTerm
	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.snapshottedIndex {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// step 2, 3, 4 is skipped cause not implement offset mechanism

	// 6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	lastIncludedRelativeIndex := rf.getRelativeLogIndex(args.LastIncludedIndex)
	if len(rf.logEntries) > lastIncludedRelativeIndex {
		rf.logEntries[lastIncludedRelativeIndex].Term = args.LastIncludedTerm
		rf.logEntries = rf.logEntries[lastIncludedRelativeIndex:]
	} else {
		// 7. Discard the entire log
		rf.logEntries = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex, Command: nil}}
	}
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.snapshottedIndex = args.LastIncludedIndex

	if rf.commitIndex < rf.snapshottedIndex {
		rf.commitIndex = rf.snapshottedIndex
	}
	if rf.lastApplied < rf.snapshottedIndex {
		rf.lastApplied = rf.snapshottedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), args.Data)

	if rf.lastApplied > rf.snapshottedIndex {
		return
	}

	applyMsg := ApplyMsg{
		CommandIndex: rf.snapshottedIndex,
		Command:      "InstallSnapshot",
		CommandValid: false,
		CommandData: rf.persister.ReadSnapshot(),
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(applyMsg)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
