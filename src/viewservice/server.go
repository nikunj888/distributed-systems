package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	currentView View
	pendingView View
	primaryAckd bool
	backupInitialized bool
	recentPings map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	
	vs.recentPings[args.Me] = time.Now()
	if (args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum && !vs.primaryAckd) {
		vs.primaryAckd = true
		//log.Printf("Primary has acked view %d;\tPending viewnum = %v\n", vs.currentView.Viewnum, vs.pendingView)
		if (vs.pendingView.Viewnum > vs.currentView.Viewnum) {
			vs.currentView = vs.pendingView
			vs.pendingView = View{0, "", ""}
			vs.primaryAckd = false		
			vs.backupInitialized = false	
		}
	}
	
	if (args.Me == vs.currentView.Backup && args.Viewnum == vs.currentView.Viewnum) {
		vs.backupInitialized = true
	}
	
	if (vs.pendingView.Viewnum <= vs.currentView.Viewnum) {
		vs.pendingView = vs.currentView
	}
	
	if (vs.currentView.Primary == "") {
		if (args.Viewnum == 0) {
			vs.currentView.Viewnum = 1
			vs.currentView.Primary = args.Me
			reply.View = vs.currentView
			vs.pendingView = View{0, "", ""}
			vs.primaryAckd = false
			//log.Printf("Replying to ping %v with %v\n", args, reply)
			vs.mu.Unlock()
			return nil
		}
	} else if (vs.currentView.Backup == "" && args.Me != vs.currentView.Primary) {
		if (args.Viewnum == 0) {
			vs.pendingView.Viewnum = vs.currentView.Viewnum + 1
			vs.pendingView.Backup = args.Me
		}
	} 
	
	if (args.Viewnum != 0) {
		if (vs.pendingView.Viewnum == vs.currentView.Viewnum) {
			vs.pendingView = View{0, "", ""}
		}
		//log.Printf("Server %v is alive\n", args.Me)
	} else if (args.Me == vs.currentView.Primary && vs.currentView.Backup != "" && vs.backupInitialized) {		// if primary died and restarted, promote backup
		vs.pendingView.Primary = vs.currentView.Backup
		vs.pendingView.Backup = ""
		vs.pendingView.Viewnum = vs.currentView.Viewnum + 1
		//log.Printf("Promoted backup %v to primary\n", vs.pendingView.Primary);
		for server, recentPing := range vs.recentPings {
			if server != vs.pendingView.Primary && time.Since(recentPing) < DeadPings * PingInterval {
				vs.pendingView.Backup = server
				//log.Printf("Promoted idle %v to backup\n", vs.pendingView.Backup);
				break
			}
		}		
	}
	
	if (vs.primaryAckd && vs.pendingView.Viewnum > vs.currentView.Viewnum) {
		vs.currentView = vs.pendingView
		vs.pendingView = View{0, "", ""}
		vs.primaryAckd = false
		vs.backupInitialized = false
	} else if (vs.pendingView.Viewnum > vs.currentView.Viewnum) {
		//log.Printf("ViewServer Ping(%v) from %v has no effect yet since previous view not acked by primary", args.Me, args.Viewnum)
	}
	
	reply.View = vs.currentView
	//log.Printf("Replying to ping %v with %v\n", args, reply)
	//log.Printf("currentView = %v acked = %v\n pendingView = %#v", vs.currentView, vs.primaryAckd, vs.pendingView);
		
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	
	reply.View = vs.currentView
		
	vs.mu.Unlock()
	
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	
	primaryDead := false
	backupDead := false
	if (vs.currentView.Primary != "" && time.Since(vs.recentPings[vs.currentView.Primary]) > DeadPings * PingInterval) {
		primaryDead = true
		//log.Printf("Primary %v is dead\n", vs.currentView.Primary);
	}
	if (vs.currentView.Backup != "" && time.Since(vs.recentPings[vs.currentView.Backup]) > DeadPings * PingInterval) {
		backupDead = true
		//log.Printf("Backup %v is dead\n", vs.currentView.Backup);
	}
	

	if (vs.pendingView.Viewnum <= vs.currentView.Viewnum) {
		vs.pendingView = vs.currentView
	}
	
	if (primaryDead && !backupDead && vs.currentView.Backup != "" && vs.backupInitialized) {		// promote backup
		vs.pendingView.Primary = vs.currentView.Backup
		vs.pendingView.Backup = ""
		vs.pendingView.Viewnum = vs.currentView.Viewnum + 1
		//log.Printf("Promoted backup %v to primary\n", vs.pendingView.Primary);
		for server, recentPing := range vs.recentPings {
			if server != vs.pendingView.Primary && time.Since(recentPing) < DeadPings * PingInterval {
				vs.pendingView.Backup = server
				//log.Printf("Promoted idle %v to backup\n", vs.pendingView.Backup);
				break
			}
		}
	} else if (!primaryDead && backupDead) {
		//log.Printf("Removed backup %v\n", vs.currentView.Backup);
		vs.pendingView.Backup = ""
		vs.pendingView.Viewnum = vs.currentView.Viewnum + 1
		for server, recentPing := range vs.recentPings {
			if server != vs.pendingView.Primary && time.Since(recentPing) < DeadPings * PingInterval {
				vs.pendingView.Backup = server
				//log.Printf("Promoted idle %v to backup\n", vs.pendingView.Backup);
				break
			}
		}
	} else if (primaryDead && backupDead) {
		vs.pendingView.Primary = ""
		vs.pendingView.Backup = ""
		vs.pendingView.Viewnum = vs.currentView.Viewnum + 1
	}
	
	if (vs.primaryAckd && vs.pendingView.Viewnum > vs.currentView.Viewnum) {
		vs.currentView = vs.pendingView
		vs.pendingView = View{0, "", ""}
		vs.primaryAckd = false
		vs.backupInitialized = false
	} else if (vs.pendingView.Viewnum > vs.currentView.Viewnum) {
		//log.Printf("Tick has no effect yet since previous view not acked by primary; pendingView = %v\n", vs.pendingView)
	}
	
	vs.mu.Unlock()
		
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{0, "", ""}
	vs.pendingView = View{0, "", ""}
	vs.primaryAckd = false
	vs.backupInitialized = false
	vs.recentPings = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
