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

type SErverState string

type ApplyMsg struct {
	Index       int
	Command     interface{}
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
	Log			[]LogEntry
	commitIndex int
	lastApplied int

	// leader state
	nextIndex	   []int
	matchIndex	   []int
	sendAppendChan []chan struct{}

	// Liveness state
	lastHeartBeat time.time
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

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(
		RaftPersistence{
			CurrentTerm:	   rf.currentTerm,
			Log:			   rf.log,
			VotedFor:		   rf.votedFor,
			LastSnapshotIndex: rf.lastSnapshotIndex,
			LastSnapshotTerm:  rf.lastSnapshotTerm,
		})
	RaftDebug("Persisting node data (%d bytes)", rf, buf.Len())
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.Lock()
	defer rf.Unlock()

	lastIndex, lastTerm := rf.getLastEntryInfo()
	logUpToDate := func() bool {
		if lastTerm == args.LastLogTerm {
			return lastIndex <= args.LatLogIndex
		}
		return lastTerm < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm
	reply.Id = rf.id

	if args.Term < rf. currentTerm {
		reply.VoteGranted = false
	} else if arfs.Term >= rf.currentTerm && logUpToDate{
		rf.transitionToFollower(args.Term)
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	} else if (rf.votedFor == "" || args.CandidateID == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	rf.persist()
	RaftInfo("Vote requested for: %s on term: %d. Log up-to-date? %v. Vote granted? %v", rf, args.CandidateID, args.Term, logUpToDate, reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(serverConn *labrpc.ClientEnd, server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	requestName := "Raft.RequestVote"
	request := func() bool{
		return severConn.Call(requestName, args, reply)
	}
	if ok := SendrpcRequest(requestName, request); ok{
		voteChan <- server
	}
}

func (rf *Raft) AppendEntries(args *AppendEntrisesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	defer rf.Unlock()

	RaftInfo("Request from %s, w/ %d entries. Args.Prev:[Index %d, Term %d]", rf, args.LeaderID, len(args.LogEntries), args.PreviousLogIndex, args.PreviousLogTerm)
	
		reply.Term = rf.currentTerm
		if args.Term < rf.currentTerm {
			reply.Success = false
			return
		} else if args.Term >= rf.currentTerm {
			rf.transitionToFollower(args.Term)
			rf.leaderID = args.LeaderID
		}
	
		if rf.leaderID == args.LeaderID {
			rf.lastHeartBeat = time.Now()
		}
	
		// Try to find supplied previous log entry match in our log
		prevLogIndex := -1
		for i, v := range rf.log {
			if v.Index == args.PreviousLogIndex {
				if v.Term == args.PreviousLogTerm {
					prevLogIndex = i
					break
				} else {
					reply.ConflictingLogTerm = v.Term
				}
			}
		}
	
		PrevIsInSnapshot := args.PreviousLogIndex == rf.lastSnapshotIndex && args.PreviousLogTerm == rf.lastSnapshotTerm
		PrevIsBeginningOfLog := args.PreviousLogIndex == 0 && args.PreviousLogTerm == 0
	
		if prevLogIndex >= 0 || PrevIsInSnapshot || PrevIsBeginningOfLog {
			if len(args.LogEntries) > 0 {
				RaftInfo("Appending %d entries from %s", rf, len(args.LogEntries), args.LeaderID)
			}
	
			// Remove any inconsistent logs and find the index of the last consistent entry from the leader
			entriesIndex := 0
			for i := prevLogIndex + 1; i < len(rf.log); i++ {
				entryConsistent := func() bool {
					localEntry, leadersEntry := rf.log[i], args.LogEntries[entriesIndex]
					return localEntry.Index == leadersEntry.Index && localEntry.Term == leadersEntry.Term
				}
				if entriesIndex >= len(args.LogEntries) || !entryConsistent() {
					// Additional entries must be inconsistent, so let's delete them from our local log
					rf.log = rf.log[:i]
					break
				} else {
					entriesIndex++
				}
			}
	
			// Append all entries that are not already in our log
			if entriesIndex < len(args.LogEntries) {
				rf.log = append(rf.log, args.LogEntries[entriesIndex:]...)
			}
	
			// Update the commit index
			if args.LeaderCommit > rf.commitIndex {
				var latestLogIndex = rf.lastSnapshotIndex
				if len(rf.log) > 0 {
					latestLogIndex = rf.log[len(rf.log)-1].Index
				}
	
				if args.LeaderCommit < latestLogIndex {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = latestLogIndex
				}
			}
			reply.Success = true
		} else {
			// §5.3: When rejecting an AppendEntries request, the follower can include the term of the
			//	 	 conflicting entry and the first index it stores for that term.
	
			// If there's no entry with `args.PreviousLogIndex` in our log. Set conflicting term to that of last log entry
			if reply.ConflictingLogTerm == 0 && len(rf.log) > 0 {
				reply.ConflictingLogTerm = rf.log[len(rf.log)-1].Term
			}
	
			for _, v := range rf.log { // Find first log index for the conflicting term
				if v.Term == reply.ConflictingLogTerm {
					reply.ConflictingLogIndex = v.Index
					break
				}
			}
	
			reply.Success = false
		}
		rf.persist()
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}
	
	rf.Lock()
	defer rf.Unlock()

	nextIndex := func() int{
		if len(rf.log) > 0{
			return rf.log[len(rf.log)-1].Index + 1
		}
		return Max(1, rf.lastSnapshotIndex+1)
	}()

	entry := LogEntry{Index: nextIndex, Term: rf.currentTrem, Commang: commang}
	rf.log = append(rf.log, entry)
	RaftInfo("New entry appended to leader's log: %s", rf, entry)

	return nextindex, term, isLeader
}

func (rf *Raft) Kill() {
	rf.Lock()
	defer rf.Unlock()

	rf.isDecommissioned = true
	RaftInfo("Node killed", rf)
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

	RaftInfo("Node created", rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
