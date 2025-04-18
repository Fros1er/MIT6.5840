package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	version rpc.Tversion
	value   string
}

type KVServer struct {
	mu   sync.Mutex
	_map map[string]*Value
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu:   sync.Mutex{},
		_map: make(map[string]*Value),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv._map[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = v.value
	reply.Version = v.version
	reply.Err = rpc.OK
}

// Put Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//if args.Key == "l" {
	//	DPrintf("[Server] Put key l version %v value %v", args.Version, args.Value)
	//}
	v, ok := kv._map[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv._map[args.Key] = &Value{1, args.Value}
		//DPrintf("[Server] Put key %v version %v, value %v", args.Key, 0, args.Value)
		reply.Err = rpc.OK
		return
	}
	if v.version != args.Version {
		//DPrintf("[Server] Put key %v ErrVersion v %v arg %v, value %v", args.Key, v.version, args.Version, args.Value)
		reply.Err = rpc.ErrVersion
		return
	}
	//DPrintf("[Server] Put key %v version %v, value %v", args.Key, v.version, args.Value)
	v.version += 1
	v.value = args.Value
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
