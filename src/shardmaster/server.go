package shardmaster

import (
	"labgob"
	"labrpc"
	"log"
	"math"
	"raft"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type OpType = string

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

var argsReflectPat = "[a-z]+\\.([a-zA-Z]+)Args"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	applied     map[int]chan string // Used to notify a command has finished execution.
	executed    map[int64]int64     // Record largest executed command number for each client.
	enableDebug bool

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type      OpType
	CommandID string
	Args      interface{}
}

func (sm *ShardMaster) startAgreementAndWait(args interface{}, commandID string, configNum int) (err Err, wrongLeader bool, config Config) {
	fullArgsType := reflect.TypeOf(args).String()
	pat := regexp.MustCompile(argsReflectPat)
	matches := pat.FindAllStringSubmatch(fullArgsType, -1)
	if matches == nil || len(matches) > 1 {
		log.Fatalf("Args type [%s] has wrong number of submatches on pattern [%s]", fullArgsType, argsReflectPat)
	}
	argsType := matches[0][1] // The first match is the whole string.

	sm.mu.Lock()
	index, _, isLeader := sm.rf.Start(Op{
		Type:      argsType,
		Args:      args,
		CommandID: commandID,
	})
	if !isLeader {
		wrongLeader = true
		sm.mu.Unlock()
		return
	}
	sm.debug("Leader %d receives [%s] request [%+v]\n", sm.me, argsType, args)

	if _, ok := sm.applied[index]; !ok {
		sm.applied[index] = make(chan string)
	}

	if configNum == -1 || configNum >= len(sm.configs) {
		configNum = len(sm.configs) - 1
	}
	config = sm.configs[configNum]

	sm.mu.Unlock()
	actualCommandID := <-sm.applied[index]
	if actualCommandID != commandID {
		err = "Leadership changed before commit (new leader applied new command)"
		wrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.applied, index)
	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err, reply.WrongLeader, _ = sm.startAgreementAndWait(*args, args.CommandID, -1)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err, reply.WrongLeader, _ = sm.startAgreementAndWait(*args, args.CommandID, -1)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err, reply.WrongLeader, _ = sm.startAgreementAndWait(*args, args.CommandID, -1)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err, reply.WrongLeader, reply.Config = sm.startAgreementAndWait(*args, args.CommandID, args.Num)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.executed = make(map[int64]int64)
	sm.applied = make(map[int]chan string)
	sm.enableDebug = false
	go sm.readAppliedCommand()

	return sm
}

func getTargetAllocation(numShards, numGroups int) []int {
	res := make([]int, numGroups)
	avgShards := int(math.Ceil(float64(numShards) / float64(numGroups)))
	// avg * (numGroups - x) + (avg - 1) * x = numShards
	// x = avg * numGroups - numShards
	lessThanAvgCnt := avgShards*numGroups - numShards
	i := 0
	for ; i < numGroups-lessThanAvgCnt; i++ {
		res[i] = avgShards
	}
	for ; i < numGroups; i++ {
		res[i] = avgShards - 1
	}
	return res
}

// newConfig is the config with new Num and Groups but unchanged shards.
func reallocateShards(newConfig *Config, newGroups []int, removedGroups []int) {
	var shardsToReallocate []int // Indexes of shards that need reallocation.
	groupsToShards := make(map[int][]int)
	for i, gid := range newConfig.Shards {
		if gid == 0 {
			continue
		}
		groupsToShards[gid] = append(groupsToShards[gid], i)
	}
	var sortedGroups []int // Gids sorted by their shards, in descending order.
	for gid := range newConfig.Groups {
		sortedGroups = append(sortedGroups, gid)
	}
	sort.Slice(sortedGroups, func(i, j int) bool {
		if len(groupsToShards[sortedGroups[i]]) != len(groupsToShards[sortedGroups[j]]) {
			return len(groupsToShards[sortedGroups[i]]) > len(groupsToShards[sortedGroups[j]])
		}
		// This tie breaker is added for testability.
		return sortedGroups[i] < sortedGroups[j]
	})

	if newGroups != nil {
		targetAllocation := getTargetAllocation(len(newConfig.Shards), len(newConfig.Groups))
		// Transform current shard allocation to target allocation.
		if len(groupsToShards) == 0 {
			for i := 0; i < len(newConfig.Shards); i++ {
				shardsToReallocate = append(shardsToReallocate, i)
			}
		} else {
			for i, gid := range sortedGroups {
				for j := 0; j < len(groupsToShards[gid])-targetAllocation[i]; j++ {
					shardsToReallocate = append(shardsToReallocate, groupsToShards[gid][j])
				}
			}
		}
		curIdx := 0
		for i, gid := range newGroups {
			for j := 0; j < targetAllocation[len(sortedGroups)-len(newGroups)+i]; j++ {
				newConfig.Shards[shardsToReallocate[curIdx]] = gid
				curIdx++
			}
		}
	} else {
		if len(newConfig.Groups) == 0 {
			for i := range newConfig.Shards {
				newConfig.Shards[i] = 0
			}
			return
		}

		targetAllocation := getTargetAllocation(len(newConfig.Shards), len(newConfig.Groups))
		for _, gid := range removedGroups {
			shardsToReallocate = append(shardsToReallocate, groupsToShards[gid]...)
		}
		curIdx := 0
		for i, gid := range sortedGroups {
			for j := 0; j < targetAllocation[i]-len(groupsToShards[gid]); j++ {
				newConfig.Shards[shardsToReallocate[curIdx]] = gid
				curIdx++
			}
		}
	}
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

func (sm *ShardMaster) getPrevConfig() Config {
	prevConfig := Config{
		Num:    -1,
		Groups: make(map[int][]string),
	}
	if len(sm.configs) > 0 {
		prevConfig = sm.configs[len(sm.configs)-1]
	}
	return prevConfig
}

func (sm *ShardMaster) reconfigure(joinArgs *JoinArgs, leaveArgs *LeaveArgs) {
	prevConfig := sm.getPrevConfig()
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Groups: copyGroups(prevConfig.Groups),
		Shards: copyShards(prevConfig.Shards),
	}

	if joinArgs != nil && leaveArgs != nil {
		log.Fatalf("Only one of joinArgs and leaveArgs can be non-nil")
	}

	if joinArgs != nil {
		var newGroups []int
		for gid := range joinArgs.Servers {
			newConfig.Groups[gid] = joinArgs.Servers[gid]
			newGroups = append(newGroups, gid)
		}
		reallocateShards(&newConfig, newGroups, nil)
	} else {
		for _, gid := range leaveArgs.GIDs {
			delete(newConfig.Groups, gid)
		}
		reallocateShards(&newConfig, nil, leaveArgs.GIDs)
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) moveShards(moveArgs *MoveArgs) {
	prevConfig := sm.getPrevConfig()
	newConfig := Config{
		Num:    prevConfig.Num + 1,
		Groups: copyGroups(prevConfig.Groups),
		Shards: copyShards(prevConfig.Shards),
	}
	newConfig.Shards[moveArgs.Shard] = moveArgs.GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) readAppliedCommand() {
	for {
		time.Sleep(10 * time.Millisecond)
		applyMsg := <-sm.applyCh
		if applyMsg.CommandValid {
			op, ok := applyMsg.Command.(Op)
			if !ok {
				log.Fatalf("Invalid command type")
			}

			sm.mu.Lock()
			clientID, commandNumber := parseCommandID(op.CommandID)
			if commandNumber > sm.executed[clientID] {
				switch op.Type {
				case Join:
					joinArgs, _ := op.Args.(JoinArgs)
					sm.reconfigure(&joinArgs, nil)
				case Leave:
					leaveArgs, _ := op.Args.(LeaveArgs)
					sm.reconfigure(nil, &leaveArgs)
				case Move:
					moveArgs, _ := op.Args.(MoveArgs)
					sm.moveShards(&moveArgs)
				}
				sm.executed[clientID] = commandNumber
			}
			sm.mu.Unlock()
			if ch, ok := sm.applied[applyMsg.CommandIndex]; ok {
				ch <- op.CommandID
			}
		} else {
			log.Fatalf("Unexpected snapshot on shard master")
		}
	}
}

func (sm *ShardMaster) debug(s string, a ...interface{}) {
	if sm.enableDebug {
		log.Printf(s, a...)
	}
}
