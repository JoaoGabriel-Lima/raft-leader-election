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

	"labrpc"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type raftRole int

const (
	follower raftRole = iota
	candidate
	leader
)

const (
	noVote                = -1
	electionTimeoutMin    = 400 * time.Millisecond
	electionTimeoutMax    = 800 * time.Millisecond
	electionCheckInterval = 10 * time.Millisecond
	heartbeatInterval     = 150 * time.Millisecond
)

type logEntry struct {
	Term    int
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

	currentTerm      int
	votedFor         int
	role             raftRole
	log              []logEntry
	electionDeadline time.Time
	votes            int
	done             chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader && !rf.stopped()
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	*reply = RequestVoteReply{Term: rf.currentTerm}
	if rf.stopped() || args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		reply.Term = rf.currentTerm
	}
	if rf.votedFor != noVote && rf.votedFor != args.CandidateID {
		return
	}

	lastIndex, lastTerm := rf.lastLogInfoLocked()
	if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		return
	}

	rf.votedFor = args.CandidateID
	rf.resetElectionDeadlineLocked()
	reply.VoteGranted = true
	DPrintf("nó %d votou no nó %d no termo %d", rf.me, args.CandidateID, rf.currentTerm)
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

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped() || args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{Term: rf.currentTerm}
		return
	}

	if rf.role != follower || args.Term > rf.currentTerm {
		DPrintf("nó %d reconheceu o líder %d no termo %d", rf.me, args.LeaderID, args.Term)
	}
	rf.becomeFollowerLocked(args.Term)
	rf.resetElectionDeadlineLocked()
	*reply = AppendEntriesReply{Term: rf.currentTerm, Success: true}
}

func (rf *Raft) runHeartbeats(term int) {
	args := AppendEntriesArgs{Term: term, LeaderID: rf.me}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		currentTerm, isLeader := rf.GetState()
		if !isLeader || currentTerm != term {
			return
		}
		DPrintf("nó %d iniciou rodada de heartbeats no termo %d", rf.me, term)
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.sendHeartbeat(peer, args)
			}
		}

		select {
		case <-rf.done:
			return
		case <-ticker.C:
		}
	}
}

func (rf *Raft) sendHeartbeat(peer int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if rf.stopped() || !rf.peers[peer].Call("Raft.AppendEntries", &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped() || reply.Term <= rf.currentTerm {
		return
	}
	if rf.role == leader {
		rf.resetElectionDeadlineLocked()
	}
	rf.becomeFollowerLocked(reply.Term)
}

func (rf *Raft) resetElectionDeadlineLocked() {
	jitter := time.Duration(rand.Int63n(int64(electionTimeoutMax - electionTimeoutMin)))
	rf.electionDeadline = time.Now().Add(electionTimeoutMin + jitter)
}

func (rf *Raft) lastLogInfoLocked() (int, int) {
	lastIndex := len(rf.log)
	if lastIndex == 0 {
		return 0, 0
	}
	return lastIndex, rf.log[lastIndex-1].Term
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = noVote
	}
	rf.role = follower
}

func (rf *Raft) runElectionLoop() {
	ticker := time.NewTicker(electionCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-ticker.C:
			rf.startElectionIfDue()
		}
	}
}

func (rf *Raft) startElectionIfDue() {
	rf.mu.Lock()
	if rf.stopped() || rf.role == leader || time.Now().Before(rf.electionDeadline) {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.role = candidate
	rf.votedFor = rf.me
	rf.votes = 1
	rf.resetElectionDeadlineLocked()
	lastIndex, lastTerm := rf.lastLogInfoLocked()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	DPrintf("nó %d iniciou eleição no termo %d", rf.me, args.Term)
	rf.promoteIfMajorityLocked()
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {
			go rf.requestVote(peer, args)
		}
	}
}

func (rf *Raft) requestVote(peer int, args RequestVoteArgs) {
	var reply RequestVoteReply
	if rf.stopped() || !rf.sendRequestVote(peer, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if rf.role != candidate || rf.currentTerm != args.Term {
		return
	}
	if reply.Term == args.Term && reply.VoteGranted {
		rf.votes++
		rf.promoteIfMajorityLocked()
	}
}

func (rf *Raft) promoteIfMajorityLocked() {
	if rf.votes > len(rf.peers)/2 {
		rf.role = leader
		DPrintf("nó %d foi eleito líder no termo %d", rf.me, rf.currentTerm)
		go rf.runHeartbeats(rf.currentTerm)
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.stopped() {
		close(rf.done)
	}
}

func (rf *Raft) stopped() bool {
	select {
	case <-rf.done:
		return true
	default:
		return false
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		votedFor:  noVote,
		role:      follower,
		done:      make(chan struct{}),
	}
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionDeadlineLocked()
	go rf.runElectionLoop()

	return rf
}
