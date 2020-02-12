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
	Term int
	Data []byte
}

// Server roles.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Election timeout and heartbeat timeout constants.
const MinElectionTimeout int = 300
const HeartbeatTimeout int = MinElectionTimeout / 2

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
	// For debugging.
	alive bool
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
	rf.debug("Server %d receives RequestVote RPC from candidate %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.debug("Candidate term is obsolete. (candidate %d: %d; follower %d: %d)\n", args.CandidateId, args.Term, rf.me, rf.term)
		reply.Granted = false
		return
	}
	if args.Term > rf.term {
		rf.convertToFollower(args.Term)
	}
	lastLog := LogEntry{
		Term: -1,
		Data: nil,
	}
	if len(rf.logs) > 0 {
		lastLog = rf.logs[len(rf.logs)-1]
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLog.Term == -1 || args.LastLogTerm > lastLog.Term || args.LastLogTerm == lastLog.Term && args.LastLogIndex >= len(rf.logs)-1) {
		rf.votedFor = args.CandidateId
		rf.timeoutRemain = rf.timeoutValue
		rf.debug("Server %d grants vote to candidate %d\n", rf.me, args.CandidateId)
		reply.Granted = true
	} else {
		reply.Granted = false
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
	rf.debug("Candidate %d sends RequestVote RPC to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
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
	rf.debug("Follower %d (term: %d) receives AppendEntries RPC from leader %d (term: %d)\n", rf.me, rf.term, args.LeaderID, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	// If leader's term is stale, return false.
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	if args.Term > rf.term {
		rf.convertToFollower(args.Term)
	}
	// If term of log at PrevLogIndex doesn't match leader's, return false
	// so that leader will decrement PrevLogIndex.
	if len(rf.logs) <= args.PrevLogIndex || args.PrevLogIndex == -1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	rf.timeoutRemain = rf.timeoutValue
	// Remove conflicting entries (same index, different term) and append new ones from leader.
	conflictIndex := len(rf.logs)
	for i := args.PrevLogIndex; i < len(rf.logs); i++ {
		if rf.logs[i].Term != args.Entries[i].Term {
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
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.debug("Leader %d sends AppendEntries RPC to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.alive = false
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
	rf.votes = 0
	rf.alive = true
	rf.timeoutValue = MinElectionTimeout + rand.Intn(MinElectionTimeout/2)
	rf.timeoutRemain = rf.timeoutValue
	go checkElectionTimeout(rf)
	go checkHeartbeat(rf)

	// TODO: apply commited logs and update lastApplied.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func checkElectionTimeout(rf *Raft) {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		rf.timeoutRemain -= 10
		// Convert to (or remain) Candidate when follower's (or Candidate's)
		// election timeout is reached, and trigger new election.
		if rf.role == Candidate && rf.votes > len(rf.peers)/2 {
			rf.role = Leader
			rf.votedFor = -1
			rf.debug("Server %d elected as new leader\n", rf.me)
			rf.heartbeat()
			rf.mu.Unlock()
			continue
		}
		// Election timeout reached: Leader sends heartbeat, Candidate/Follower starts an election.
		if rf.timeoutRemain < 0 {
			rf.debug("Election timeout reached on server %d\n", rf.me)
			rf.timeoutRemain = rf.timeoutValue
			if rf.role == Leader {
				rf.mu.Unlock()
				continue
			}
			rf.term++
			rf.debug("Server %d converts to candidate (term: %d)\n", rf.me, rf.term)
			rf.role = Candidate
			rf.votes = 1
			rf.votedFor = rf.me
			rf.getVotes()
		}
		rf.mu.Unlock()
	}
}

// getVvotes() function should be used when lock is obtained.
func (rf *Raft) getVotes() {
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	// Pass copies of states to new goroutines to avoid inconsistency.
	term := rf.term
	lastLogIndex := len(rf.logs) - 1
	for peerID := range rf.peers {
		if peerID != rf.me {
			go func(peer int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(
					peer,
					&RequestVoteArgs{
						Term:         term,
						CandidateId:  rf.me,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					},
					&reply,
				)
				if !ok {
					rf.debug("RequestVote RPC failed between candidate %d and server %d\n", rf.me, peer)
					return
				}
				rf.mu.Lock()
				// Check that server's term hasn't changed after re-acquiring the lock.
				if rf.term == term {
					if reply.Term > term {
						rf.convertToFollower(reply.Term)
					}
					if reply.Granted {
						rf.votes++
					}
				}
				rf.mu.Unlock()

			}(peerID)
		}
	}
}

func checkHeartbeat(rf *Raft) {
	for {
		time.Sleep((time.Duration)(HeartbeatTimeout) * time.Millisecond)
		rf.mu.Lock()
		// If server's role is Leader, send out AppendEntries periodically.
		if rf.role == Leader {
			rf.heartbeat()
		}
		rf.mu.Unlock()
	}
}

// heartbeat() function should be used when lock is obtained.
func (rf *Raft) heartbeat() {
	prevLogIndex := -1
	prevLogTerm := 0
	if len(rf.logs) > 0 {
		prevLogIndex = len(rf.logs) - 1
		prevLogTerm = rf.logs[prevLogIndex].Term
	}
	// Pass copies of states to new goroutines to avoid inconsistency.
	term := rf.term
	committedIndex := rf.committedIndex
	for peerID := range rf.peers {
		if peerID != rf.me {
			go func(peer int) {
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(peer, &AppendEntriesArgs{
					Term:         term,
					LeaderID:     rf.me,
					PrevLogTerm:  prevLogTerm,
					PrevLogIndex: prevLogIndex,
					Entries:      make([]LogEntry, 0),
					LeaderCommit: committedIndex,
				}, &reply)
				if !ok {
					rf.debug("AppendEntries RPC failed between leader %d and server %d\n", rf.me, peer)
					return
				}
				rf.mu.Lock()
				// Check that server's term hasn't changed after re-acquiring the lock.
				if rf.term == term && rf.term < reply.Term {
					rf.convertToFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(peerID)
		}
	}
}

func (rf *Raft) debug(s string, a ...interface{}) {
	if rf.alive {
		log.Printf(s, a...)
	}
}

func (rf *Raft) convertToFollower(newTerm int) {
	rf.role = Follower
	rf.term = newTerm
	rf.votedFor = -1
}
