package kvpaxos

import "net"
//import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0 

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// The "Value" for each Paxos instance is an Op struct
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key string
	Value string
	ClientID int64
	ReqNo int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kvstore map[string]string		// The actual key-value store
	tempStore map[string]string		// Temporary store used to track inconsistencies against the paxos log
	lastChecked int					// Last verified sequence from paxos log
	Seq int							// Paxos sequence number currently being used 
	reqNos map[int64]int			// Record of latest completed request from each client
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	DPrintf("%v Getting value at %v\n", kv.me, args.Key)
	
	kv.updateKVStore()									// verify local kvstore store against paxos log
	
	// ensure at-most-once semantics
	if (kv.reqNos[args.ClientID] >= args.ReqNo) {		// if request already completed, just return value, don't create another paxos instance
		if val, exists := kv.kvstore[args.Key]; exists == false {
			reply.Err = "ErrNoKey"
		} else {
			reply.Value = val
		}
		DPrintf("%v Already done: Get value at %v reqNo %v\n", kv.me, args.Key, args.ReqNo)
		return nil
	}
	
	kv.Seq = kv.px.Max() + 1							// use the next available sequence number after the max seen by paxos server
	
	for {												// keep trying to make a paxos log entry; ClientID and ReqNo together uniquely id the request
		kv.px.Start(kv.Seq, Op{"Get", args.Key, "", args.ClientID, args.ReqNo})
		if status, value := kv.getStatus(kv.Seq); status == paxos.Decided && value.ClientID == args.ClientID && value.ReqNo == args.ReqNo {
			break
		}
		kv.Seq++										
		kv.updateKVStore()
	}	
		
	kv.reqNos[args.ClientID] = args.ReqNo				// update record of requests
	
	if val, exists := kv.kvstore[args.Key]; exists == false {
		reply.Err = "ErrNoKey"
	} else {
		reply.Value = val
	}

	DPrintf("%v Got %v at %v (SeqNo:%v)\n", kv.me, reply.Value, args.Key, kv.Seq)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	// ensure at-most-once semantics
	for i := kv.px.Min(); i <= kv.px.Max(); i++ {		// check all log entries to update record of requests
		if status, value := kv.getStatus(i); status == paxos.Decided && kv.reqNos[value.ClientID] < value.ReqNo {
			DPrintf("%v Status(%d)=%+v; reqNos: %v\n", kv.me, i, value, kv.reqNos)
			kv.reqNos[value.ClientID] = value.ReqNo
		} else {
			DPrintf("%v Status(%d)=%+v\n", kv.me, i, value)
		}
	}
		
	if (kv.reqNos[args.ClientID] >= args.ReqNo) {		// if request already done, do not repeat else will cause wrong value to be stored		
		DPrintf("%v Already done: %v %v at %v reqNo %v\n", kv.me, args.Op, args.Value, args.Key, args.ReqNo)
		return nil
	}
	
	kv.Seq = kv.px.Max() + 1
	for {												// make paxos log entry
		kv.px.Start(kv.Seq, Op{args.Op, args.Key, args.Value, args.ClientID, args.ReqNo})
		if status, value := kv.getStatus(kv.Seq); status == paxos.Decided && value.ClientID == args.ClientID && value.ReqNo == args.ReqNo {
			break
		}
		kv.Seq++
	}
	
	_, ok := kv.tempStore[args.Key]
	if (!ok) {											// create a snapshot of value stored if local kvstore is going to deviate from log unchecked
		kv.tempStore[args.Key] = kv.kvstore[args.Key]
		DPrintf("%v: Tempstore: %v %v\n", kv.me, kv.tempStore, args)
	}
	
	if (args.Op == "Put") {
		DPrintf("%v Putting %v at %v (SeqNo:%v)\n", kv.me, args.Value, args.Key, kv.Seq)
		kv.kvstore[args.Key] = args.Value
	} else {
		DPrintf("%v Appending %v at %v (SeqNo:%v)\n", kv.me, args.Value, args.Key, kv.Seq)
		kv.kvstore[args.Key] = kv.kvstore[args.Key] + args.Value
	}
	
	kv.reqNos[args.ClientID] = args.ReqNo				// update record of requests
	
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.kvstore = make(map[string]string)
	kv.tempStore = make(map[string]string)
	kv.lastChecked = -1
	kv.Seq = -1
	kv.reqNos = make(map[int64]int)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						DPrintf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				DPrintf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

// Get the status of a paxos instance after waiting till its decided or forgotten
// Do not return if Pending
func (kv *KVPaxos) getStatus(seq int) (paxos.Fate, Op) {
		value := Op{"", "", "", 0, 0}
		to := 10 * time.Millisecond
		status, v := kv.px.Status(seq)
		for status == paxos.Pending {
			time.Sleep(to)
			if to < 10 * time.Second {
			  to *= 2
			}
			status, v = kv.px.Status(seq)
		}
		if v != nil {
			value = v.(Op)
		}
		return status, value
}

// Compare the local kvstore to the paxos log
// Tempstore is updated according to the paxos log and the result is compared with kvstore
// If any mismatch is found, kvstore is updated
func (kv *KVPaxos) updateKVStore() {

	DPrintf("%v: Tempstore before: %v lastChecked: %v kvstore before: %v\n", kv.me, kv.tempStore, kv.lastChecked, kv.kvstore)
	for i := kv.lastChecked + 1; i <= kv.px.Max(); i++ {
		if status, value := kv.getStatus(i); status == paxos.Decided {
			if kv.reqNos[value.ClientID] < value.ReqNo {				// update record of requests
				kv.reqNos[value.ClientID] = value.ReqNo
			}
			
			_, ok := kv.tempStore[value.Key]
			if (!ok) {
				kv.tempStore[value.Key] = kv.kvstore[value.Key]
			}
			if value.Operation == "Put" {
				kv.tempStore[value.Key] = value.Value
			} else if value.Operation == "Append" {
				kv.tempStore[value.Key] = kv.tempStore[value.Key] + value.Value
			}
		}
		kv.lastChecked++
		kv.px.Done(i)													// This server will no longer require this paxos instance, therefore it is forgotten
	}
	
	DPrintf("%v: Tempstore after %v\n:", kv.me, kv.tempStore)
	for key, value := range(kv.tempStore) {
		if kv.kvstore[key] != value {
			DPrintf("%v Mismatch Tempstore[%v]: %v, KVStore[%v]: %v\n", kv.me, key, value, key, kv.kvstore[key])
			kv.kvstore[key] = value
		}
		delete(kv.tempStore, key)										// delete the tempStore to reduce memory usage
	}
}
