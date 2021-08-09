package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

const WaitCmdTimeOut = time.Millisecond * 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
	Method    string
}

type Notification struct {
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stopCh               chan struct{}
	db                   map[string]string
	notifyCh             map[int]chan Notification
	lastAppliedRequestId map[int64]int64
	appliedRaftLogIndex  int
}

func (kv *KVServer) log(format string, a ...interface{}) {
	if Debug > 0 {
		r := fmt.Sprintf(format, a...)
		s := fmt.Sprintf("me=%d", kv.me)
		log.Printf("- kvserver - %s - %s\n", s, r)
	}
	return
}

func (kv *KVServer) shouldTakeSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.lastAppliedRequestId)
	appliedRaftLogIndex := kv.appliedRaftLogIndex
	kv.mu.Unlock()

	kv.rf.ReplaceLogWithSnapshot(appliedRaftLogIndex, w.Bytes())
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot != nil {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		if d.Decode(&kv.db) != nil || d.Decode(&kv.lastAppliedRequestId) != nil {
			kv.log("kvserver %d failed to install snapshot", kv.me)
		}
	}
}

func (kv *KVServer) dataGet(key string) (val string, err Err) {
	if v, ok := kv.db[key]; ok {
		val = v
		err = OK
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Method:    "Get",
	}

	reply.Err = kv.waitCmd(op)
	if reply.Err == OK {
		kv.mu.Lock()
		value, err := kv.dataGet(args.Key)
		kv.mu.Unlock()
		reply.Value = value
		reply.Err = err
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Key:       args.Key,
		Value:     args.Value,
		Method:    args.Op,
	}
	reply.Err = kv.waitCmd(op)
}

func (kv *KVServer) waitCmd(op Op) Err {

	index, term, isLeader := kv.rf.Start(op)
	kv.log("start cmd, index = %d, term = %d, isLeader = %t, op = %+v", index, term, isLeader, op)
	if !isLeader {
		return ErrWrongLeader
	}

	if kv.shouldTakeSnapshot() {
		kv.takeSnapshot()
	}

	kv.mu.Lock()
	if _, ok := kv.notifyCh[index]; !ok {
		kv.notifyCh[index] = make(chan Notification, 1)
	}
	ch := kv.notifyCh[index]
	kv.mu.Unlock()

	res := OK
	t := time.NewTimer(WaitCmdTimeOut)
	select {
	case notify := <-ch:
		if notify.ClientId != op.ClientId || notify.RequestId != op.RequestId {
			res = ErrWrongLeader
		}
	case <-t.C:
		kv.mu.Lock()
		if !kv.isDuplicated(op.ClientId, op.RequestId) {
			res = ErrTimeOut
		}
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
	return Err(res)
}

func (kv *KVServer) isDuplicated(clientId int64, requestId int64) bool {
	appliedRequestId, ok := kv.lastAppliedRequestId[clientId]
	if !ok || requestId > appliedRequestId {
		return false
	}
	return true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) waitApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			kv.log("stop ch")
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				switch msg.Command.(string) {
				case "InstallSnapshot":
					kv.installSnapshot(msg.CommandData)
				}
				continue
			}

			kv.log("apply cmd: %+v", msg)
			op := msg.Command.(Op)

			kv.mu.Lock()
			if kv.isDuplicated(op.ClientId, op.RequestId) {
				kv.mu.Unlock()
				continue
			}

			switch op.Method {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			case "Get":
			}
			kv.lastAppliedRequestId[op.ClientId] = op.RequestId
			kv.appliedRaftLogIndex = msg.CommandIndex

			if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
				notify := Notification{
					ClientId:  op.ClientId,
					RequestId: op.RequestId,
				}
				ch <- notify
			}
			kv.mu.Unlock()
		}
	}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.lastAppliedRequestId = make(map[int64]int64)
	kv.stopCh = make(chan struct{})
	kv.notifyCh = make(map[int]chan Notification)

	snapshot := persister.ReadSnapshot()
	kv.installSnapshot(snapshot)

	go kv.waitApplyCh()

	return kv
}
