package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currentView viewservice.View
	kvstore map[string]string
	seqNos map[int64]int
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
//fmt.Printf("%v Getting value at %v\n", pb.me, args.Key)	
	switch {
	case args.Forwarded && pb.currentView.Backup != pb.me:
		pb.mu.Unlock()
		return fmt.Errorf("Forwarded Get to non-backup %v\n", pb.me)
	case !args.Forwarded && pb.currentView.Primary != pb.me:
		pb.mu.Unlock()
		return fmt.Errorf("%v is not primary!\n", pb.me)		
	case !args.Forwarded && pb.currentView.Primary == pb.me && pb.currentView.Backup != "":
		var rep GetReply
		ok := call(pb.currentView.Backup, "PBServer.Get", &GetArgs{args.Key, true}, &rep)
		if (!ok || rep.Err != "") {
			pb.mu.Unlock()
			return fmt.Errorf("Forwarded Get to %v failed!\n", pb.currentView.Backup)
		}
	}
	
	
	value, ok := pb.kvstore[args.Key]
	if (!ok) {
		reply.Err = Err("Key " + args.Key + " not found on server " + pb.me + "\n")
		reply.Value = ""
	} else {
		reply.Err = ""
		reply.Value = value
	}
	
	pb.mu.Unlock()
	if (reply.Err != "") {
		return fmt.Errorf(string(reply.Err))
	}
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	
//fmt.Printf("%v Trying to append %v at %v (SeqNo:%v) Forwarded = %v\n", pb.me, args.Value, args.Key, args.SeqNo, args.Forwarded)
	switch {
	case args.Forwarded && pb.currentView.Backup != pb.me:
		pb.mu.Unlock()
		return fmt.Errorf("Forwarded Put to non-backup %v\n", pb.me)
	case !args.Forwarded && pb.currentView.Primary != pb.me:
		pb.mu.Unlock()
		return fmt.Errorf("%v is not primary!\n", pb.me)		
	case !args.Forwarded && pb.currentView.Primary == pb.me && pb.currentView.Backup != "":
		paargs := &PutAppendArgs{args.Key, args.Value, args.Op, true, args.ClientID, args.SeqNo}
		var pareply PutAppendReply
		ok := call(pb.currentView.Backup, "PBServer.PutAppend", paargs, &pareply)
		if (!ok || pareply.Err != "") {
			pb.mu.Unlock()
			return fmt.Errorf("Forwarded Put of k:%v, v:%v to %v failed!\n", args.Key, args.Value, pb.currentView.Backup)
		}
	}
	
	if (pb.seqNos[args.ClientID] == args.SeqNo) {
		pb.mu.Unlock()
		return nil
	}
	
	if (args.Op == "Put") {
		if (pb.seqNos[args.ClientID] < args.SeqNo) {
//fmt.Printf("%v Putting %v at %v\n", pb.me, args.Value, args.Key)
			pb.kvstore[args.Key] = args.Value
			pb.seqNos[args.ClientID] = args.SeqNo
		}
	} else {
		value, ok := pb.kvstore[args.Key]
		if (!ok) {
			value = ""
		}
		if (pb.seqNos[args.ClientID] < args.SeqNo) {
//fmt.Printf("%v Appending %v at %v (SeqNo:%v) Forwarded = %v\n", pb.me, args.Value, args.Key, args.SeqNo, args.Forwarded)
			pb.kvstore[args.Key] = value + args.Value
			pb.seqNos[args.ClientID] = args.SeqNo
		}
	}
	
	pb.mu.Unlock()

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
//log.Printf("Ping from %v\n", pb.me)	
	view, err := pb.vs.Ping(pb.currentView.Viewnum)
	if (err == nil && view.Viewnum != pb.currentView.Viewnum) {
//log.Printf("%v UpdateView from %v to %v\n", pb.me, pb.currentView, view)
		pb.currentView = view
		if (view.Primary == pb.me && view.Backup != "") {
			var reply CopyStateReply
//log.Printf("%v Copied state for UpdateView %v\n", pb.me, pb.currentView)
			call(pb.currentView.Backup, "PBServer.CopyState", &CopyStateArgs{pb.kvstore, pb.seqNos}, &reply)
		}
	}
	
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currentView = viewservice.View{0, "", ""}
	pb.kvstore = make(map[string]string)
	pb.seqNos = make(map[int64]int)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}


func (pb *PBServer) CopyState(args *CopyStateArgs, reply *CopyStateReply) error {
	pb.mu.Lock()
	
	view, ok := pb.vs.Get()
	for ; ok == false || view.Viewnum == 0; view, ok = pb.vs.Get() {};
	if (pb.currentView.Viewnum != view.Viewnum) {
//log.Printf("%v Updated view from %v to %v\n", pb.me, pb.currentView, view)
		pb.currentView = view
	}
	
	if pb.currentView.Backup != pb.me {
		pb.mu.Unlock()
		return fmt.Errorf("%v is not Backup\n", pb.me)
	}
	for key, value := range args.Kvstore {
		pb.kvstore[key] = value
	}
	for key, value := range args.SeqNos {
		pb.seqNos[key] = value
	}
	
	pb.mu.Unlock()
	return nil 
}

func (pb *PBServer) UpdateView() {
	view, ok := pb.vs.Get()
	for ; ok == false || view.Viewnum == 0; view, ok = pb.vs.Get() {};
//log.Printf("%v Updated view from %v to %v\n", pb.me, pb.currentView, view)
	pb.currentView = view
}
