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
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

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
}

// Log entry type.
type LogEntry struct {
	term int
	data []byte
}

// Server roles.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role
	// Persistent states.
	votedFor int
	term     int
	logs     []LogEntry
	// Volatile states for all servers.
	committedIndex int
	lastApplied    int
	// Volatile states for leader.
	nextIndex []int
	lastMatch []int
	// Count of votes for candidate.
	votes int
	// For election timeout.
	timeoutValue  int
	timeoutRemain int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.term)
	enc.Encode(rf.logs)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var term int
	var votedFor int
	var logs []LogEntry
	if dec.Decode(&term) != nil || dec.Decode(&votedFor) != nil || dec.Decode(logs) != nil {
		log.Fatalln("Fail to decode persisted data")
	}
	rf.term = term
	rf.votedFor = votedFor
	rf.logs = logs
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("Server %d receives RequestVote RPC from candidate %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.Granted = false
		return
	}
	if args.Term > rf.term {
		rf.role = Follower
	}
	// Leader should heartbeat upon election.
	if rf.role == Leader {
		rf.heartBeat()
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == rf.me) &&
		(args.LastLogTerm > rf.term || args.LastLogTerm == rf.term && args.LastLogIndex >= len(rf.logs)) {
		rf.votedFor = args.CandidateId
		rf.timeoutRemain = rf.timeoutValue
		reply.Granted = true
	} else {
		reply.Granted = false
	}
}

func (rf *Raft) heartBeat() {
	prevLogIndex := -1
	prevLogTerm := 0
	if len(rf.logs) > 0 {
		prevLogIndex = len(rf.logs) - 1
		prevLogTerm = rf.logs[prevLogIndex].term
	}
	for peerId := range rf.peers {
		if peerId != rf.me {
			rf.sendAppendEntries(peerId, &AppendEntriesArgs{
				Term:         rf.term,
				LeaderId:     rf.me,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      make([]LogEntry, 0),
				LeaderCommit: rf.committedIndex,
			}, &AppendEntriesReply{})
		}
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
	log.Printf("Candidate %d sends RequestVote RPC to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	if args.Term > rf.term {
		rf.role = Follower
	}
	// If term of log at PrevLogIndex doesn't match leader's, return false
	// so that leader will decrement PrevLogIndex.
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// Remove conflicting entries (same index, different term) and append new ones from leader.
	conflictIndex := len(rf.logs)
	for i := args.PrevLogIndex; i < len(rf.logs); i++ {
		if rf.logs[i].term != args.Entries[i].term {
			conflictIndex = i
			break
		}
	}
	rf.logs = rf.logs[:conflictIndex]
	if conflictIndex < len(rf.logs) {
		for i := conflictIndex; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, args.Entries[i])
		}
	}
	// Update commitIndex if necessary.
	if args.LeaderCommit > rf.committedIndex {
		rf.committedIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}
	defer rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("Leader %d sends AppendEntries RPC to server %d\n", rf.me, server)
	success := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// Convert to follower if peer's term is newer.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term < reply.Term {
		rf.role = Follower
	}
	return success
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.term = 0
	rf.votedFor = -1
	rf.role = Follower
	r := rand.New(rand.NewSource(1))
	rf.timeoutValue = 250 + r.Intn(151)
	rf.timeoutRemain = rf.timeoutValue
	go checkElectionTimeout(rf)

	// TODO: if role is leader, send enpty AppendEntries periodically.

	// TODO: apply commited logs and update lastApplied.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func checkElectionTimeout(rf *Raft) {
	for {
		time.Sleep(10)
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}
		if rf.role == Candidate && rf.votes > len(rf.peers)/2 {
			rf.role = Leader
			rf.mu.Unlock()
			continue
		}
		rf.timeoutRemain -= 10
		// Election timeout reached, start an election.
		if rf.timeoutRemain < 0 {
			rf.timeoutRemain = rf.timeoutValue
			rf.role = Candidate
			rf.term += 1
			rf.votedFor = rf.me
			for peerId := range rf.peers {
				if peerId != rf.me {
					lastLogTerm := 0
					if len(rf.logs) > 0 {
						lastLogTerm = rf.logs[len(rf.logs)-1].term
					}
					go func() {
						success := rf.sendRequestVote(
							peerId,
							&RequestVoteArgs{
								Term:         rf.term,
								CandidateId:  rf.me,
								LastLogIndex: len(rf.logs),
								LastLogTerm:  lastLogTerm,
							},
							&RequestVoteReply{},
						)
						// TODO: check if lock can be obtained here.
						if success {
							rf.mu.Lock()
							rf.votes += 1
							rf.mu.Unlock()
						}
					}()
				}
			}
		}
		rf.mu.Unlock()
	}
}
