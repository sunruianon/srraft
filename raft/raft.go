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

import "sync"
import "labrpc"
import "time"

import "bytes"
import "encoding/gob"

type ServerState string

const (
	Follower ServerState = "Follower"
	Candidate = "Candidate"
	Leader = "Leader"

	RPCTimeout = 50 * time.Millisecond
	RPCMaxTries = 3
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot	[]byte
}

type Raft struct {
	sync.Mutex

	peers     []*labrpc.ClientEnd
	persister *Persister          // Object to hold this peer's persisted state

	// General state
	id			  	 string
	me			 	 int // index into peers[]
	state			 ServerState
	isDecommissioned bool

	// Election state
	currentTerm int
	votedFor	string
	leaderID	string

	// Log state
	log			[]LogEntry
	commitIndex int
	lastApplied int

	// Log compaction state, if snapshots are enabled
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// leader state
	nextIndex	   []int
	matchIndex	   []int
	sendAppendChan []chan struct{}

	// Liveness state
	lastHeartBeat time.Time
}

type RaftPersistence struct {
	CurrentTerm int
	Log			[]LogEntry
	VotedFor	string
}

type LogEntry struct{
	Index int
	Term int
	Command interface{}
}

func (rf *Raft) GetState() (int, bool) {
	rf.Lock()
	defer rf.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return rf.lastSnapshotIndex, rf.lastSnapshotTerm
}

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(
		RaftPersistence{
			CurrentTerm:	   rf.currentTerm,
			Log:			   rf.log,
			VotedFor:		   rf.votedFor,
		})
	rf.persister.SaveRaftState(buf.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	obj := RaftPersistence{}
	d.Decode(&obj)
}


type RequestVoteArgs struct {
	Term		 int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}


type RequestVoteReply struct {
	Term		int
	VoteGranted bool
	Id			string
}

func (rf *Raft) transitionToCandidate() {
	rf.state = Candidate
	// Increment currentTerm and vote for self
	rf.currentTerm++
	rf.votedFor = rf.id
}

func (rf *Raft) transitionToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = ""
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	lastIndex, lastTerm := rf.getLastEntryInfo()
	logUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LastLogIndex
		}
		return lastTerm < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf. currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm && logUpToDate{
		rf.transitionToFollower(args.Term)
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else if (rf.votedFor == "" || args.CandidateID == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	rf.persist()
}

func SendRPCRequest(requestName string, request func() bool) bool {
	makeRequest := func(successChan chan struct{}) {
		if ok := request(); ok {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < RPCMaxTries; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)
		select {
		case <-rpcChan:
			return true
		case <-time.After(RPCTimeout):
		}
	}

	return false
}

func (rf *Raft) sendRequestVote(serverConn *labrpc.ClientEnd, server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	requestName := "Raft.RequestVote"
	request := func() bool{
		return serverConn.Call(requestName, args, reply)
	}
	if ok := SendRPCRequest(requestName, request); ok{
		voteChan <- server
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}
	
	rf.Lock()
	defer rf.Unlock()

	nextIndex := func() int {
		if len(rf.log) > 0 {
			return rf.log[len(rf.log)-1].Index + 1
		}
		return Max(1, rf.lastSnapshotIndex+1)
	}()

	entry := LogEntry{Index: nextIndex, Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)

	return nextIndex, term, isLeader
}

func (rf *Raft) Kill() {
	rf.Lock()
	defer rf.Unlock()

	rf.isDecommissioned = true
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:		 peers,
		persister:	 persister,
		me:			 me,
		id:			 string(rune(me + 'A')),
		state:		 Follower,
		commitIndex: 0,
		lastApplied: 0,
	}


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
