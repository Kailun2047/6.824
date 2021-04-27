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
	SnapshotData []byte
}

// Log entry type.
type LogEntry struct {
	Term    int
	Command interface{}
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

// PartialLogs represents log entries after a start index.
type PartialLogs struct {
	StartIndex int
	Entries    []LogEntry
}

func NewPartialLogEntries() *PartialLogs {
	partialLogs := PartialLogs{
		Entries: []LogEntry{LogEntry{}},
	}
	return &partialLogs
}

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

	// Persistant states for all servers.
	votedFor int
	term     int
	logs     *PartialLogs

	// Volatile states for all servers.
	role           Role
	votes          int // Count of votes for candidate in current term.
	timeoutValue   int
	timeoutRemain  int
	committedIndex int
	lastApplied    int

	// Volatile states for leaders.
	nextIndex  []int
	matchIndex []int

	alive       bool          // For debugging.
	enableDebug bool          // For debugging.
	applyCh     chan ApplyMsg // For server to apply new entries.
	cond        *sync.Cond    // To kick the apply entries gorontine.
	muCond      sync.Mutex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.term
	isleader = rf.role == Leader
	return term, isleader
}

func (rf *Raft) encodeRaftState() []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.term)
	enc.Encode(rf.logs.StartIndex)
	firstNonEmptyIndex := 1
	if rf.logs.StartIndex > 0 {
		firstNonEmptyIndex = 0
	}
	for i, log := range rf.logs.Entries {
		if i < firstNonEmptyIndex {
			continue
		}
		enc.Encode(log)
	}
	return buf.Bytes()
}

func (rf *Raft) encodeSnapshot(lastTerm, lastIndex int, kvs map[string]string, exxecuted map[int64]int64) []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(lastTerm)
	enc.Encode(lastIndex)
	enc.Encode(kvs)
	enc.Encode(exxecuted)
	return buf.Bytes()
}

func (rf *Raft) decodeLastTermAndIndex() (int, int) {
	buf := bytes.NewBuffer(rf.persister.ReadSnapshot())
	dec := labgob.NewDecoder(buf)
	var lastTerm, lastIndex int
	dec.Decode(&lastTerm)
	dec.Decode(&lastIndex)
	return lastTerm, lastIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	raftStateData := rf.encodeRaftState()
	rf.persister.SaveRaftState(raftStateData)
}

// This method should be called while holding rf.mu.
func (rf *Raft) getLastLogIndex() int {
	return rf.logs.StartIndex + len(rf.logs.Entries) - 1
}

// This method should be called while holding rf.mu.
func (rf *Raft) getPartialLogIndex(index int) int {
	return index - rf.logs.StartIndex
}

// This method should be called while holding rf.mu.
func (rf *Raft) getLastLogTerm(index int) int {
	var lastLogTerm int
	if index >= rf.logs.StartIndex {
		lastLogTerm = rf.logs.Entries[rf.getPartialLogIndex(index)].Term
	} else {
		lastLogTerm, _ = rf.decodeLastTermAndIndex()
	}
	return lastLogTerm
}

// This method should be called while holding rf.mu.
func (rf *Raft) calculateAndInstallSnapshot(peer int) {
	lastTerm, lastIndex := rf.decodeLastTermAndIndex()
	installArgs := &InstallSnapshotArgs{
		Term:              rf.term,
		LastIncludedTerm:  lastTerm,
		LastIncludedIndex: lastIndex,
		LeaderID:          rf.me,
		Data:              rf.persister.ReadSnapshot(),
	}
	go tryInstall(rf, peer, installArgs)
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
	var votedFor, term, startIndex int
	if dec.Decode(&votedFor) != nil || dec.Decode(&term) != nil || dec.Decode(&startIndex) != nil {
		log.Fatalln("Fail to decode persisted data")
	}
	if startIndex > 0 {
		rf.logs = &PartialLogs{
			StartIndex: startIndex,
		}
		rf.committedIndex = startIndex - 1
		rf.lastApplied = startIndex - 1
	} else {
		rf.logs = NewPartialLogEntries()
	}

	var entry LogEntry
	for dec.Decode(&entry) == nil {
		rf.debug("Server %d recovers a log at index %d.\n", rf.me, len(rf.logs.Entries))
		rf.logs.Entries = append(rf.logs.Entries, entry)
	}
	rf.term = term
	rf.votedFor = votedFor
	rf.debug("Server %d loaded persisted state (start index: [%d], term: [%d])", rf.me, rf.logs.StartIndex, rf.term)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug("Server %d (index: %d, term: %d) receives RequestVote RPC from candidate %d (index: %d, term: %d)\n", rf.me, rf.getLastLogIndex(), rf.term, args.CandidateId, args.LastLogIndex, args.Term)
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.debug("Candidate term is obsolete. (candidate %d: %d; follower %d: %d)\n", args.CandidateId, args.Term, rf.me, rf.term)
		reply.Granted = false
		return
	}
	if args.Term > rf.term {
		rf.convertToFollower(args.Term)
	}

	lastIncludedTerm, _ := rf.decodeLastTermAndIndex()
	lastLog := LogEntry{
		Term: lastIncludedTerm,
	}
	if len(rf.logs.Entries) > 0 {
		lastLog = rf.logs.Entries[len(rf.logs.Entries)-1]
	}
	lastLogIndex := rf.getLastLogIndex()

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLog.Term || args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLogIndex) {
		rf.votedFor = args.CandidateId
		rf.persist()
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
	Term             int
	Success          bool
	ConflictingTerm  int
	ConflictingIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug("Follower %d (index: %d, term: %d) receives AppendEntries RPC from leader %d (PrevLogIndex: %d, term: %d)\n", rf.me, rf.getLastLogIndex(), rf.term, args.LeaderID, args.PrevLogIndex, args.Term)
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	rf.timeoutRemain = rf.timeoutValue
	if args.Term > rf.term {
		rf.convertToFollower(args.Term)
	}
	// If PrevLogIndex is greater than this peer's max index, or term of log at PrevLogIndex doesn't match leader's,
	// return false so that leader will decrement PrevLogIndex.
	lastIndex := rf.getLastLogIndex()
	if lastIndex < args.PrevLogIndex {
		reply.ConflictingIndex = lastIndex + 1
		reply.ConflictingTerm = -1
		return
	}
	prevLogTerm := rf.getLastLogTerm(args.PrevLogIndex)
	if args.PrevLogIndex > 0 && prevLogTerm != args.PrevLogTerm {
		conflictingIndex := lastIndex
		reply.ConflictingTerm = rf.logs.Entries[rf.getPartialLogIndex(conflictingIndex)].Term
		for conflictingIndex > rf.logs.StartIndex && rf.logs.Entries[rf.getPartialLogIndex(conflictingIndex)].Term == reply.ConflictingTerm {
			conflictingIndex--
		}
		reply.ConflictingIndex = conflictingIndex
		reply.Success = false
		return
	}
	conflictIndex := rf.logs.StartIndex - 1
	for i := 0; i < len(args.Entries); i++ {
		idx := args.PrevLogIndex + 1 + i
		if idx < rf.logs.StartIndex {
			continue
		}
		if idx > lastIndex || rf.logs.Entries[rf.getPartialLogIndex(idx)].Term != args.Entries[i].Term {
			conflictIndex = idx
			break
		}
	}
	// Truncate old logs and append new logs ONLY IF there is a conflicting entry.
	if conflictIndex >= rf.logs.StartIndex {
		rf.logs.Entries = rf.logs.Entries[:rf.getPartialLogIndex(conflictIndex)]
		for i := conflictIndex - args.PrevLogIndex - 1; i < len(args.Entries); i++ {
			rf.logs.Entries = append(rf.logs.Entries, args.Entries[i])
		}
		rf.persist()
	}
	if args.LeaderCommit > rf.committedIndex {
		// min() is necessary since LeaderCommit could be beyond
		// the up-to-date entries on this follower.
		rf.committedIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.cond.Broadcast()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.debug("Leader %d sends AppendEntries RPC to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	LeaderID          int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.debug("Follower %d (index: %d, term: %d) receives InstallSnapshot RPC from leader %d (LastIncludedTerm: %d, LastIncludedIndex: %d)\n", rf.me, rf.getLastLogIndex(), rf.term, args.LeaderID, args.LastIncludedTerm, args.LastIncludedIndex)
	curTerm := rf.term
	if args.Term < curTerm {
		reply.Term = curTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > curTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = args.Term
	rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		SnapshotData: args.Data,
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstRemainingEntry := len(rf.logs.Entries)
	if curTerm == args.Term && rf.getLastLogIndex() > args.LastIncludedIndex {
		firstRemainingEntry = args.LastIncludedIndex - rf.logs.StartIndex + 1
	}
	rf.logs.StartIndex = args.LastIncludedIndex + 1
	rf.logs.Entries = rf.logs.Entries[firstRemainingEntry:]
	rf.committedIndex = max(rf.committedIndex, args.LastIncludedIndex)
	rf.lastApplied = rf.committedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), args.Data)
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.debug("Leader %d sends AppendEntries RPC to server %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0
	term := 0
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		rf.logs.Entries = append(rf.logs.Entries, LogEntry{
			Term:    rf.term,
			Command: command,
		})
		rf.persist()
		term = rf.term
		index = rf.getLastLogIndex()
		isLeader = true
		rf.matchIndex[rf.me] = index
		rf.debug("Leader %d starts agreement on command %v (index: %d).\n", rf.me, command, index)
		rf.appendNewEntries()
	}

	return index, term, isLeader
}

// This method should be invoked when rf.mu is held.
func (rf *Raft) appendNewEntries() {
	// Make a copy of leader states to avoid inconsistency.
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// It's possible that a leader and a follower are partitioned
		// into a minority, and when this leader is reconnected to majority
		// some logs are discarded and therefore nextIndex[peer] > len(rf.logs).
		rf.nextIndex[peer] = min(rf.nextIndex[peer], rf.logs.StartIndex+len(rf.logs.Entries))
		if rf.nextIndex[peer] < rf.logs.StartIndex {
			// if the next log entry to send is included in snapshot, send snapshot instead.
			rf.calculateAndInstallSnapshot(peer)
		} else {
			prevLogIndex := rf.nextIndex[peer] - 1
			prevLogTerm := 0
			if prevLogIndex > 0 {
				prevLogTerm = rf.getLastLogTerm(prevLogIndex)
			}
			appendArgs := &AppendEntriesArgs{
				Term:         rf.term,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      rf.logs.Entries[rf.getPartialLogIndex(rf.nextIndex[peer]):],
				LeaderCommit: rf.committedIndex,
			}
			go tryAppend(rf, peer, appendArgs)
		}
	}
}

func tryInstall(rf *Raft, peer int, args *InstallSnapshotArgs) {
	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(peer, args, &reply)
	if !ok {
		rf.debug("InstallSnapshot RPC between leader %d and server %d failed\n", rf.me, peer)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term != args.Term {
		return
	}
	if rf.term < reply.Term {
		rf.convertToFollower(reply.Term)
		return
	}
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
}

func tryAppend(rf *Raft, peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		rf.debug("AppendEntries RPC between leader %d and server %d failed\n", rf.me, peer)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term != args.Term {
		return
	}
	if !reply.Success {
		if args.Term < reply.Term {
			rf.convertToFollower(reply.Term)
			return
		}
		// If follower doesn't have PrevLogIndex or leader doesn't have logs
		// with ConflictingTerm, set nextIndex to Conflicting Index;
		// if leader has logs with ConflictingTerm (i.e. follower has more logs
		// with ConflictingTerm than leader), set nextIndex to the index beyond
		// the last entry on the leader with that term.
		rf.nextIndex[peer] = reply.ConflictingIndex
		if reply.ConflictingTerm != -1 {
			for i := range rf.logs.Entries {
				if rf.logs.Entries[i].Term == reply.ConflictingTerm {
					for i < len(rf.logs.Entries) && rf.logs.Entries[i].Term == reply.ConflictingTerm {
						i++
					}
					rf.nextIndex[peer] = rf.logs.StartIndex + i
					break
				}
			}
		}
		if rf.nextIndex[peer] < rf.logs.StartIndex {
			rf.calculateAndInstallSnapshot(peer)
			return
		}
		args.Entries = rf.logs.Entries[rf.getPartialLogIndex(rf.nextIndex[peer]):]
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		args.PrevLogTerm = rf.getLastLogTerm(args.PrevLogIndex)
		go tryAppend(rf, peer, args)
	} else {
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
	}
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
	rf.logs = NewPartialLogEntries()
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.muCond)
	rf.votedFor = -1
	rf.role = Follower
	rf.alive = true
	rf.enableDebug = false
	rf.timeoutValue = MinElectionTimeout + rand.Intn(MinElectionTimeout/2)
	rf.timeoutRemain = rf.timeoutValue
	go checkElectionTimeout(rf)
	go checkHeartbeat(rf)
	go checkApplyEntries(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func checkElectionTimeout(rf *Raft) {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		rf.timeoutRemain -= 10
		if rf.role == Candidate && rf.votes > len(rf.peers)/2 {
			rf.role = Leader
			rf.debug("Server %d elected as new leader in term [%d]\n", rf.me, rf.term)
			// When a server first comes to power, initialize nextIndex[] and matchIndex[].
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			// Conservatively set matchIndex to 0 (not sharing anything),
			// optimistically set nextIndex to len(rf.logs).
			for i := range rf.matchIndex {
				rf.matchIndex[i] = 0
				rf.nextIndex[i] = rf.logs.StartIndex + len(rf.logs.Entries)
			}
			rf.appendNewEntries()
			rf.mu.Unlock()
			continue
		}
		// Election timeout reached: Candidate/Follower starts an election.
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
			rf.persist()
			rf.getVotes()
		}
		rf.mu.Unlock()
	}
}

// getVotes() function should be used when lock is obtained.
func (rf *Raft) getVotes() {
	// Pass copies of states to new goroutines to avoid inconsistency.
	term := rf.term
	lastLogTerm := 0
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex > 0 {
		lastLogTerm = rf.getLastLogTerm(lastLogIndex)
	}
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
		if rf.role == Leader {
			rf.appendNewEntries()
		}
		rf.mu.Unlock()
	}
}

func checkApplyEntries(rf *Raft) {
	go applyEntries(rf)
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			newCommittedIndex := rf.committedIndex
			for i := range rf.peers {
				// Leader can only commit logs whose term is the current term.
				if rf.getPartialLogIndex(rf.matchIndex[i]) >= len(rf.logs.Entries) {
					rf.debug("On Raft [%d], the real index [%d] for match index [%d] is out of bound ([%d])", i, rf.getPartialLogIndex(rf.matchIndex[i]), rf.matchIndex[i], len(rf.logs.Entries))
				}
				if rf.matchIndex[i] > newCommittedIndex && rf.logs.Entries[rf.getPartialLogIndex(rf.matchIndex[i])].Term == rf.term {
					count := 0
					for j := range rf.peers {
						if rf.matchIndex[j] >= rf.matchIndex[i] {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						newCommittedIndex = rf.matchIndex[i]
					}
				}
			}
			if newCommittedIndex > rf.committedIndex {
				rf.committedIndex = newCommittedIndex
				rf.debug("Leader %d updated committedIndex to %d.\n", rf.me, rf.committedIndex)
				rf.cond.Broadcast()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) committedEqualsApplied() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied == rf.committedIndex
}

func applyEntries(rf *Raft) {
	rf.cond.L.Lock()
	for {
		for rf.committedEqualsApplied() {
			rf.cond.Wait()
		}
		rf.mu.Lock()
		commandIndex := rf.lastApplied + 1
		// It's possible that rf.me is not in majority.
		if commandIndex > rf.getLastLogIndex() {
			rf.mu.Unlock()
			continue
		}
		rf.debug("Server [%d] (start index: [%d], last log index: [%d]) applying log [%d]", rf.me, rf.logs.StartIndex, rf.getLastLogIndex(), commandIndex)
		if rf.getPartialLogIndex(commandIndex) >= len(rf.logs.Entries) {
			rf.debug("On server [%d], real index [%d] for command [%d] is greater than max log entry [%d]", rf.me, rf.getPartialLogIndex(commandIndex), commandIndex, len(rf.logs.Entries)-1)
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs.Entries[rf.getPartialLogIndex(commandIndex)].Command,
			CommandIndex: commandIndex,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied++
		rf.mu.Unlock()
		rf.debug("Server %d applied log %v (index: %d)\n", rf.me, msg, commandIndex)
	}
}

func (rf *Raft) debug(s string, a ...interface{}) {
	if rf.alive && rf.enableDebug {
		log.Printf(s, a...)
	}
}

// convertToFollower() should be used when lock is held.
func (rf *Raft) convertToFollower(newTerm int) {
	rf.role = Follower
	rf.term = newTerm
	rf.votedFor = -1
	rf.persist()
	rf.debug("Server %d converts to follower.\n", rf.me)
}

// GetStateSize returns number of bytes of current Raft state.
func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

// GetSnapshot retrieves the persisted snapshot.
func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}

// CreateSnapshot accepts a snapshot from kvserver, discards old log entries included in the snapshot,
// and persists Raft state and snapshot together.
func (rf *Raft) CreateSnapshot(kvs map[string]string, executed map[int64]int64, lastLogIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Discard log entries that are included in snapshot.
	lastLogTerm := rf.logs.Entries[rf.getPartialLogIndex(lastLogIndex)].Term
	rf.logs.Entries = append([]LogEntry{}, rf.logs.Entries[rf.getPartialLogIndex(lastLogIndex+1):]...)
	rf.logs.StartIndex = lastLogIndex + 1
	rf.debug("Server [%d] created snapshot up until index [%d] in term [%d]; new start index: [%d], new logs: [%v]\n", rf.me, lastLogIndex, lastLogTerm, rf.logs.StartIndex, rf.logs.Entries)
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), rf.encodeSnapshot(lastLogTerm, lastLogIndex, kvs, executed))
}
