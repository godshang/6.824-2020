package kvraft

import (
	"../labrpc"
	"fmt"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"

const ChangeLeaderInterval = time.Millisecond * 20

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId      int
	clientId      int64
	lastRequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) log(format string, a ...interface{}) {
	if Debug > 0 {
		r := fmt.Sprintf(format, a...)
		s := fmt.Sprintf("clientId=%d, leaderId=%d, lastRequestId=%d", ck.clientId, ck.leaderId, ck.lastRequestId)
		log.Printf("- clerk - %s - %s\n", s, r)
	}
	return
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
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
	ck.log("Get, key = %s ", key)
	requestId := ck.lastRequestId + 1

	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		RequestId: requestId,
	}
	leaderId := ck.leaderId

	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}

		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			ck.lastRequestId = requestId
			return reply.Value
		case ErrNoKey:
			ck.leaderId = leaderId
			ck.lastRequestId = requestId
			return ""
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			continue
		}
	}
	return ""
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
	ck.log("PutAppend, key = %s, value = %s, op = %s", key, value, op)
	requestId := ck.lastRequestId + 1
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		RequestId: requestId,
	}
	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.lastRequestId = requestId
			return
		case ErrNoKey:
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
