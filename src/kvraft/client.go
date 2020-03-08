package raftkv

import (
	"crypto/rand"
	"labrpc"
	"log"
	"math/big"
	"strconv"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkID       int64
	commandNumber int // Serial number for current command.
	leaderID      int
	enableDebug   bool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = nrand()
	ck.commandNumber = 0
	ck.leaderID = -1
	ck.enableDebug = false
	return ck
}

func (ck *Clerk) generateCommandID() string {
	commandID := strconv.FormatInt(ck.clerkID, 10) + "+" + strconv.Itoa(ck.commandNumber)
	ck.commandNumber++
	return commandID
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		CommandID: ck.generateCommandID(),
	}
	res := ""
	i := ck.leaderID
	if i == -1 {
		i = 0
	}
	doneCh := make(chan bool)
	for {
		var reply GetReply
		go func() {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			doneCh <- ok
		}()
		select {
		case <-time.After(600 * time.Millisecond):
			ck.debug("No reply in 600 ms, clerk %d retry request %v\n", ck.clerkID, args)
		case ok := <-doneCh:
			if ok && !reply.WrongLeader && len(reply.Err) == 0 {
				ck.leaderID = i
				res = reply.Value
				ck.debug("Client receives reply from server %d for Get request %v\n", i, args)
				return res
			}
		}
		i = (i + 1) % len(ck.servers)
	}
	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandID: ck.generateCommandID(),
	}
	i := ck.leaderID
	if i == -1 {
		i = 0
	}
	doneCh := make(chan bool)
	for {
		var reply PutAppendReply
		go func() {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			doneCh <- ok
		}()
		select {
		case <-time.After(600 * time.Millisecond):
			ck.debug("No reply in 600 ms, clerk %d retry request %v\n", ck.clerkID, args)
		case ok := <-doneCh:
			if ok && !reply.WrongLeader && len(reply.Err) == 0 {
				ck.leaderID = i
				ck.debug("Client receives reply from server %d for PutAppend request %v\n", i, args)
				return
			}
		}
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) debug(s string, a ...interface{}) {
	if ck.enableDebug {
		log.Printf(s, a...)
	}
}
