package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's currentTerm
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // currentTerm of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Receiver implementation, reference raft paper's Figure 2:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// reset timer after grant vote
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) startElection() {
	rf.log("start election")

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	rf.electionTimer.Reset(randElectionTimeout())

	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := rf.buildRequestVoteArgs()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			if ok {
				rf.processRequestVoteReply(&args, &reply)
			}
		}(peer)
	}
}

func (rf *Raft) buildRequestVoteArgs() RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

func (rf *Raft) processRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		return
	}

	// if rpc request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
	}

	// if votes received from majority of servers, become leader
	if reply.VoteGranted && rf.role == Candidate {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}
