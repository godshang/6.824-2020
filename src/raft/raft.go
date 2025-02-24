package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, currentTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (currentTerm, isLeader)
//   ask a Raft for its current currentTerm, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ElectionTimeout  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 100
)

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2

	None = -1
)

//
// as each Raft peer becomes aware that successive log Entries are
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
	CommandData  []byte
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int // current currentTerm
	votedFor    int
	logEntries  []LogEntry

	voteCount int
	role      Role // current role

	// volatile state on  all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increase monotonically)

	snapshottedIndex  int
	lastIncludedIndex int
	lastIncludedTerm  int

	applyCh        chan ApplyMsg
	stopCh         chan struct{}
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return Term and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	if len(rf.logEntries) < 1 {
		return
	}
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.snapshottedIndex)
	return w.Bytes()
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm      int
		votedFor         int
		logEntries       []LogEntry
		snapshottedIndex int
	)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil || d.Decode(&snapshottedIndex) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.snapshottedIndex = snapshottedIndex

		rf.commitIndex = snapshottedIndex
		rf.lastApplied = snapshottedIndex
	}
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
// currentTerm. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, lastLogIndex := rf.lastLogTermIndex()
	index := lastLogIndex + 1
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).
	if isLeader {
		logEntry := LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		}
		rf.logEntries = append(rf.logEntries, logEntry)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = None
	rf.voteCount = 0
	rf.role = Follower
	rf.logEntries = make([]LogEntry, 1)

	rf.applyCh = applyCh
	rf.stopCh = make(chan struct{})
	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logEntries)
	}

	rf.eventLoop()

	return rf
}

func (rf *Raft) eventLoop() {
	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				switch rf.role {
				case Follower:
					rf.becomeCandidate()
				case Candidate:
					rf.startElection()
				}
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.role == Leader {
					rf.heartbeats()
					rf.heartbeatTimer.Reset(HeartBeatTimeout)
				}
				rf.mu.Unlock()
			}
		}
	}()
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = None
	rf.heartbeatTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	rf.log("become leader")
	rf.role = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getAbsoluteLogIndex(len(rf.logEntries))
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = rf.snapshottedIndex
	}
	rf.electionTimer.Stop()
	rf.heartbeats()
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.logEntries[len(rf.logEntries)-1].Term
	index := rf.getAbsoluteLogIndex(len(rf.logEntries) - 1)
	return term, index
}

func (rf *Raft) getAbsoluteLogIndex(index int) int {
	return index + rf.snapshottedIndex
}

func (rf *Raft) getRelativeLogIndex(index int) int {
	return index - rf.snapshottedIndex
}

func (rf *Raft) log(format string, a ...interface{}) {
	if Debug <= 0 {
		return
	}
	r := fmt.Sprintf(format, a...)
	s := fmt.Sprintf("me=%d, role=%v, currentTerm=%d, nextIndex=%v, matchIndex=%v, commitIndex=%d, lastApplied=%d",
		rf.me, rf.role, rf.currentTerm, rf.nextIndex, rf.matchIndex, rf.commitIndex, rf.lastApplied)
	log.Printf("- raft - %s - %s\n", s, r)
}
