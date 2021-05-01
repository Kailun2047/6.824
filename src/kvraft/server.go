package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	GetType    = "Get"
	PutType    = "Put"
	AppendType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	CommandID string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	executed         map[int64]int64 // Record largest executed command number for each client.
	pairs            map[string]string
	cond             *sync.Cond
	applied          map[int]chan string
	enableDebug      bool
	lastCommandIndex int // Used for snapshotting.
	snapshottedIndex int // Used for snapshotting.
}

func parseCommandID(commandID string) (int64, int64) {
	strs := strings.Split(commandID, "+")
	clientID, err := strconv.ParseInt(strs[0], 10, 64)
	if err != nil {
		log.Fatalf("Cannot parse client ID correctly: [%v]", err)
	}
	commandNumber, err := strconv.ParseInt(strs[1], 10, 64)
	if err != nil {
		log.Fatalf("Cannot parse command number correctly: [%v]", err)
	}
	return clientID, commandNumber
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		Type:      GetType,
		Key:       args.Key,
		Value:     "",
		CommandID: args.CommandID,
	})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("Leader %d receives Get request %v\n", kv.me, *args)
	clientID, commandNumber := parseCommandID(args.CommandID)
	if commandNumber <= kv.executed[clientID] {
		reply.Err = ""
		reply.WrongLeader = false
		reply.Value = kv.pairs[args.Key]
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.applied[index]; !ok {
		kv.applied[index] = make(chan string)
	}
	kv.mu.Unlock()
	commandID := <-kv.applied[index]
	if commandID != args.CommandID {
		reply.Err = "Leadership changed before commit (new leader applied new command)"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.pairs[args.Key]
	delete(kv.applied, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		CommandID: args.CommandID,
	})
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("Leader %d receives PutAppend request %v\n", kv.me, *args)
	clientID, commandNumber := parseCommandID(args.CommandID)
	if commandNumber <= kv.executed[clientID] {
		reply.Err = ""
		reply.WrongLeader = false
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.applied[index]; !ok {
		kv.applied[index] = make(chan string)
	}
	kv.mu.Unlock()
	commandID := <-kv.applied[index]
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.applied, index)
	if commandID != args.CommandID {
		reply.Err = "Leadership changed before commit (new leader applied new command)"
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func decodeSnapshot(snapshotData []byte) (int, int, map[string]string, map[int64]int64) {
	kvs := make(map[string]string)
	executed := make(map[int64]int64)
	var lastIncludedTerm, lastIncludedIndex int
	buf := bytes.NewBuffer(snapshotData)
	decoder := labgob.NewDecoder(buf)

	err := decoder.Decode(&lastIncludedTerm)
	if err != nil {
		log.Panicf("Error decoding last included term: [%v]\n", err)
	}
	err = decoder.Decode(&lastIncludedIndex)
	if err != nil {
		log.Panicf("Error decoding last included index: [%v]\n", err)
	}
	err = decoder.Decode(&kvs)
	if err != nil {
		log.Panicf("Error decoding k-v pairs: [%v]", err)
	}
	err = decoder.Decode(&executed)
	if err != nil {
		log.Panicf("Error decoding executed command IDs: [%v]", err)
	}

	return lastIncludedTerm, lastIncludedIndex, kvs, executed
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.executed = make(map[int64]int64)
	kv.pairs = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.applied = make(map[int]chan string)
	kv.enableDebug = false
	snapshotBytes := kv.rf.GetSnapshot()

	if len(snapshotBytes) > 0 {
		_, lastIncludedIndex, kvs, executed := decodeSnapshot(snapshotBytes)
		kv.lastCommandIndex = lastIncludedIndex
		kv.snapshottedIndex = kv.lastCommandIndex
		kv.pairs = kvs
		kv.executed = executed
	}

	go readAppliedCommand(kv)
	if kv.maxraftstate >= 0 {
		go checkRaftStateSize(kv)
	}

	return kv
}

func readAppliedCommand(kv *KVServer) {
	for {
		time.Sleep(10 * time.Millisecond)
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if !ok {
				log.Panicf("Cannot convert command at index [%d]", applyMsg.CommandIndex)
			}
			kv.mu.Lock()
			clientID, commandNumber := parseCommandID(op.CommandID)
			if commandNumber > kv.executed[clientID] {
				switch op.Type {
				case PutType:
					kv.pairs[op.Key] = op.Value
				case AppendType:
					kv.pairs[op.Key] = kv.pairs[op.Key] + op.Value
				}
				kv.executed[clientID] = commandNumber
			}
			kv.lastCommandIndex = applyMsg.CommandIndex
			kv.mu.Unlock()
			if ch, ok := kv.applied[applyMsg.CommandIndex]; ok {
				ch <- op.CommandID
			}
		} else if len(applyMsg.SnapshotData) > 0 {
			// Follower receives a snapshot.
			_, lastIncludedIndex, kvs, executed := decodeSnapshot(applyMsg.SnapshotData)
			kv.mu.Lock()
			kv.pairs, kv.executed = kvs, executed
			kv.lastCommandIndex = lastIncludedIndex
			kv.snapshottedIndex = lastIncludedIndex
			kv.mu.Unlock()
		}
	}
}

func checkRaftStateSize(kv *KVServer) {
	for {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		if kv.rf.GetStateSize() > kv.maxraftstate && kv.lastCommandIndex > kv.snapshottedIndex {
			kv.rf.CreateSnapshot(kv.pairs, kv.executed, kv.lastCommandIndex)
			kv.snapshottedIndex = kv.lastCommandIndex
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) debug(s string, a ...interface{}) {
	if kv.enableDebug {
		log.Printf(s, a...)
	}
}
