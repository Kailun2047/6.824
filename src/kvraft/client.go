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
	ck.enableDebug = true
	return ck
}

func (ck *Clerk) generateCommandID() string {
	commandID := strconv.FormatInt(ck.clerkID, 10) + "+" + strconv.Itoa(ck.commandNumber)
	ck.clerkID++
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
	for {
		var reply GetReply
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
				continue
			}
			ck.leaderID = i
			if len(reply.Err) > 0 {
				log.Println(reply.Err)
				continue
			}
			res = reply.Value
			ck.debug("Client receives Get reply from server %d\n", i)
			break
		} else {
			i = (i + 1) % len(ck.servers)
			ck.debug("Get RPC times out, client retries with server %d\n", i)
		}
		time.Sleep(10 * time.Millisecond)
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
	for {
		var reply PutAppendReply
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader {
				i = (i + 1) % len(ck.servers)
				continue
			}
			ck.leaderID = i
			if len(reply.Err) > 0 {
				log.Println(reply.Err)
				continue
			}
			ck.debug("Client receives PutAppend reply from server %d\n", i)
			break
		} else {
			i = (i + 1) % len(ck.servers)
			ck.debug("PutAppend RPC times out, client retries with server %d\n", i)
		}
		time.Sleep(10 * time.Millisecond)
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
