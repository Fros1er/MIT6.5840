package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []any
	logTerm     []int

	commitIndex int
	lastApplied int

	isLeader           bool
	nextIndex          []int
	matchIndex         []int
	receivedFromLeader chan struct{}

	isVoting  bool
	voted     []bool
	votedLock sync.Mutex
}

func electionTimeout() time.Duration {
	return time.Millisecond * time.Duration(1500+rand.Int63n(1000))
}

func (rf *Raft) lastLogInfo() (int, int) {
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.logTerm[len(rf.log)-1]
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) updateTerm(newTerm int) {
	assert(mutexLocked(&rf.mu), "mutex should be locked when updateTerm")
	assert(rf.currentTerm <= newTerm, "newTerm is older!")
	if rf.currentTerm < newTerm {
		rf.votedFor = -1
		rf.currentTerm = newTerm
	}
}

func (rf *Raft) hasMajority(n int) bool {
	return n >= (len(rf.peers)/2 + 1)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FTracef("Node %d enter GetState", rf.me)
	//var term int
	//var isleader bool
	// Your code here (3A).
	return rf.currentTerm, rf.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	assert(args.CandidateId != rf.me, "self RequestVote should not happen")
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FTracef("Node %d enter ReqVote", rf.me)
	log.Printf("Node %d (term=%d, %t %t) get reqVote from Node %d (term=%d)\n", rf.me, rf.currentTerm, rf.isLeader, rf.isVoting, args.CandidateId, args.Term)

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		log.Printf("Node %d (term=%d) No vote to Node %d (term=%d)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		log.Printf("Node %d (prevTerm=%d)'s term updated in RequestVote.", rf.me, rf.currentTerm)
		if rf.isVoting {
			log.Printf("Node %d (term=%d)'s vote cancelled due to ReqVote\n", rf.me, rf.currentTerm)
		}
		rf.isVoting = false
		if rf.isLeader {
			log.Printf("Node %d (term=%d) no longer a leader due to ReqVote\n", rf.me, rf.currentTerm)
		}
		rf.isLeader = false
		rf.updateTerm(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		assertf(rf.isVoting == false, "Should not happen")
		assert(rf.isLeader == false, "Should not happen")
		lastLogIndex, lastLogTerm := rf.lastLogInfo()
		if args.LastLogTerm >= lastLogTerm && args.LastLogIndex >= lastLogIndex {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			log.Printf("Node %d (prevTerm=%d) vote to Node %d (term=%d)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
			select {
			case rf.receivedFromLeader <- struct{}{}:
			default:
			}
		} else {
			todo()
		}
		return
	} else {
		log.Printf("Node %d (term=%d) No vote to Node %d (term=%d): votedFor=%d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.votedFor)
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []any
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	assert(args.LeaderId != rf.me, "self AppendEntries should not happen")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FTracef("Node %d enter AppendE", rf.me)

	if args.Term < rf.currentTerm {
		log.Printf("Node %d (term=%d) observed outdated AppendEntries from Node %d (term=%d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	log.Printf("Node %d (term=%d): received AppendEntries from Node %d (term=%d)", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	if args.Term == rf.currentTerm {
		assert(!rf.isLeader, "term have two leader!")
		if rf.isVoting {
			log.Printf("Node %d (prevTerm=%d) cancel a vote due to AppendEntries.", rf.me, rf.currentTerm)
			rf.isVoting = false
			rf.updateTerm(args.Term)
			rf.votedFor = args.LeaderId
		}
	}
	if args.Term > rf.currentTerm {
		log.Printf("Node %d (prevTerm=%d)'s term updated in AppendEntries.", rf.me, rf.currentTerm)
		if rf.isLeader {
			log.Printf("Node %d (prevTerm=%d) no longer a leader due to AppendEntries.", rf.me, rf.currentTerm)
		}
		if rf.isVoting {
			log.Printf("Node %d (prevTerm=%d) cancel a vote due to AppendEntries.", rf.me, rf.currentTerm)
		}
		rf.isLeader = false
		rf.isVoting = false
		rf.updateTerm(args.Term)
		rf.votedFor = args.LeaderId
	}

	reply.Success = true
	select {
	case rf.receivedFromLeader <- struct{}{}:
	default:
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startVoting() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FTracef("Node %d enter startVoting", rf.me)

	rf.isVoting = true
	rf.isLeader = false
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votedLock.Lock()
	clear(rf.voted)
	rf.votedLock.Unlock()
	log.Printf("Node %d timeout, startVoting for term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) heartBeat() {
	assert(mutexLocked(&rf.mu), "mutex should be locked when heartBeat")
	assert(rf.isLeader, "Non-leader called heartbeat!")
	FTracef("Node %d enter heartbeat", rf.me)

	arg := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	replyChan := make(chan AppendEntriesReply, len(rf.peers))
	timeoutChan := make(chan struct{})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func() {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &arg, &reply)
			select {
			case <-timeoutChan:
			default:
				if ok {
					replyChan <- reply
				}
			}
		}()
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(timeoutChan)
	}()

outer:
	for {
		select {
		case reply := <-replyChan:
			if !reply.Success {
				assert(rf.currentTerm < reply.Term, "currentTerm >= reply.Term!")
				log.Printf("Node %d (term=%d) observed Term %d, no longer a leader\n", rf.me, rf.currentTerm, reply.Term)
				rf.isLeader = false
				rf.updateTerm(reply.Term)
				break outer
			}
		case <-timeoutChan:
			break outer
		}
	}
}

func (rf *Raft) doVote() {
	assert(mutexLocked(&rf.mu), "mutex should be locked when doVote")
	FTracef("Node %d enter doVote", rf.me)

	lastLogIndex, lastLogTerm := rf.lastLogInfo()

	arg := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	replyChan := make(chan RequestVoteReply, len(rf.peers))
	timeoutChan := make(chan struct{})
	rf.votedLock.Lock()
	for i := range rf.peers {
		if i == rf.me || rf.voted[i] {
			continue
		}
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &arg, &reply)
			select {
			case <-timeoutChan:
			default:
				if ok {
					replyChan <- reply
					rf.votedLock.Lock()
					rf.voted[i] = true
					rf.votedLock.Unlock()
				}
			}
		}()
	}
	rf.votedLock.Unlock()
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(timeoutChan)
	}()

	voted := 1
outer:
	for {
		select {
		case reply := <-replyChan:
			if reply.VoteGranted {
				voted += 1
				if rf.hasMajority(voted) {
					break outer
				}
			} else if reply.Term >= rf.currentTerm {
				log.Printf("Node %d observed larger Term %d, curr %d\n", rf.me, reply.Term, rf.currentTerm)
				rf.updateTerm(reply.Term)
				rf.isVoting = false
				return
			}
		case <-timeoutChan:
			break outer
		}
	}

	if rf.hasMajority(voted) {
		// became leader
		rf.isLeader = true
		rf.isVoting = false
		log.Printf("Node %d is voted as leader of Term %d\n", rf.me, rf.currentTerm)
		return
	}

	log.Printf("Node %d failed to get enough vote in Term %d (got %d)\n", rf.me, rf.currentTerm, voted)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		timeout := electionTimeout()
		start := time.Now()
		for {
			if !(time.Since(start) < timeout) {
				break
			}

			startInner := time.Now()
			rf.mu.Lock()
			FTracef("Node %d enter ticker, with %t %t", rf.me, rf.isLeader, rf.isVoting)
			if rf.isVoting {
				rf.doVote()
			} else if rf.isLeader {
				rf.heartBeat()
			}
			rf.mu.Unlock()
			select {
			case <-time.After(time.Millisecond*time.Duration(50+rand.Int63n(100)) - time.Since(startInner)):
				// Do nothing
			case <-time.After(timeout - time.Since(start)):
				break
			case <-rf.receivedFromLeader:
				// guarantee we are follower here
				start = time.Now()
			}
		}
		// timeout!
		rf.startVoting()
	}

	// pause for a random amount of time between 50 and 350
	// milliseconds.
	//ms := 50 + (rand.Int63() % 300)
	//time.Sleep(time.Duration(ms) * time.Millisecond)

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	n := len(peers)
	rf := &Raft{
		votedFor:           -1,
		log:                make([]any, 0),
		logTerm:            make([]int, 0),
		isLeader:           false,
		nextIndex:          make([]int, n),
		matchIndex:         make([]int, n),
		receivedFromLeader: make(chan struct{}),
		isVoting:           false,
		voted:              make([]bool, n),
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
