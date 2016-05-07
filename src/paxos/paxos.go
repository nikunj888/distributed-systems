package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently to disk, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Instance struct {
	status Fate	
	n_p int					// highest prepare see
	n_a int					// highest accept seen
	v_a interface{}			// highest value accepted
	driver bool				// keeps track of driveDecision threads started
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	instances map[int]*Instance	// map of sequence number to paxos instance
	maxInstance int
	minInstance int
	done	[]int				// highest instance done by each peer
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//fmt.Printf("%v: Start(%d, %v)\n", px.peers[px.me], seq, v)
	if seq >= px.minInstance {
		go func(seq int, v interface{}) {		// start paxos proposal on new thread
			px.Propose(seq, v)
		} (seq, v)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	if seq > px.done[px.me] {
		px.done[px.me] = seq
	}
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxInstance
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.maxInstance
	for i:= range(px.done) {
		if px.done[i] < min {
			min = px.done[i]					// find the minimum values done among all servers
		}
	}
	
	for i := px.minInstance; i <= min; i++ {	// delete log entries up to min
		delete(px.instances, i)
		//fmt.Println(px.peers[px.me], "###Deleting", i)
	}
	
	px.minInstance = min + 1
	//fmt.Println(px.peers[px.me], "Done:", px.done, "Min:", px.minInstance)
	
	return px.minInstance
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, exists := px.instances[seq]
	if exists {									// if log entry exists, return it
		//fmt.Printf("%v: Status(%d) = %+v\n", px.peers[px.me], seq, instance)
		return instance.status, instance.v_a
	} else if seq < px.minInstance {			// if seq < min, the entry has been forgotten
		//fmt.Printf("%v: Status(%d) = %+v\n", px.peers[px.me], seq, "Forgotten")
		return Forgotten, nil
	} else if seq <= px.maxInstance {			// if not forgotten, yet less than max, this server has not heard of seq though others have probably decided on it
		px.instances[seq] = &Instance{Pending, 0, 0, nil, false}
		go px.driveDecision(seq, nil)			// start a new paxos agreement to learn the decision
		px.instances[seq].driver = true
	}
	//fmt.Printf("%v: Status(%d) = %+v\n", px.peers[px.me], seq, "Pending")
	
	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.instances = make(map[int]*Instance)
	px.maxInstance = -1			// highest instance seen
	px.minInstance = -1			// smallest instance remembered
	px.done = make([]int, len(peers))
	for i := range(px.done) {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							//fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					//fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}

type PrepareArgs struct {
	Seq int
	N int
}

type PrepareReply struct {
	Seq int
	Ok bool
	N int
	N_a int
	V_a interface{}
	Done int
}

type AcceptArgs struct {
	Seq int
	N int
	V interface{}
}

type AcceptReply struct {
	Seq int
	Ok bool
	N int
	Done int
}

type DecidedArgs struct {
	Seq int
	N int
	V interface{}
}

type DecidedReply struct {
	Seq int
	N int
	Ok bool
	Done int
}

// Proposer function
func (px* Paxos) Propose(seq int, v interface{}) {
	//fmt.Printf("%v: Propose(%d, %v)\n", px.peers[px.me], seq, v)
	n := 1					// attempt number of paxos agreement on the specified instance
	
	px.mu.Lock()
	if val, exists := px.instances[seq]; exists == true {
		n = val.n_p + 1		// if instance already exists, start with higher attempt number
	}
	px.mu.Unlock()
	
	// Paxos proposal algorithm
	for status, _ := px.Status(seq); status != Decided; status, _ = px.Status(seq) {	// while instance not decided
		numPrepareOKs , _ , highestV_a := px.sendPrepares(seq, n)						// send prepare messages to all servers
		v_a := v
		if !px.isMajority(numPrepareOKs) {												// if majority don't acknowledge
			n++																			// try with higher n
			continue
		} else {																		// else send accept messages to all servers
			if highestV_a != nil {
				v_a = highestV_a														// if replying server has already accepted a value, then use accepted value instead of proposed value
			}
			numAcceptOKs := px.sendAccepts(seq, n, v_a)
			if !px.isMajority(numAcceptOKs) {
				n++
				continue				
			} else {																	// if majority acknowledge
				px.sendDecided(seq, n, v_a)												// inform all servers of decision
			}
		}
	}
}

// Send prepare messages to all servers
func (px* Paxos) sendPrepares(seq int, n int) (numPrepareOKs int, highestN_a int, highestV_a interface{}) {

	for i, peer := range px.peers {
	
		var reply PrepareReply
		//fmt.Printf("%v: Sending prepare(Seq:%d, N:%d) to %v\n", px.peers[px.me], seq, n, peer)
		if i == px.me {
			px.Prepare(&PrepareArgs{seq, n}, &reply)
		} else {
			call(peer, "Paxos.Prepare", &PrepareArgs{seq, n}, &reply)
		}
		
		//fmt.Printf("%v: Prepare_reply(%+v) from %v\n", px.peers[px.me], reply, peer)
		if reply.Ok == true {
			px.done[i] = reply.Done			// piggybacked done value from replying server
			numPrepareOKs++
			if reply.N_a > highestN_a {		// if any server has accepted a higher numbered attempt, use the corresponding accepted value
				highestN_a = reply.N_a
				highestV_a = reply.V_a
			}
		}
	}
	
	return numPrepareOKs, highestN_a, highestV_a
}

// Send accept messages to all servers
func (px* Paxos) sendAccepts(seq int, n int, v_a interface{}) (numAcceptOKs int) {

	for i, peer := range px.peers {
	
		var reply AcceptReply
		//fmt.Printf("%v: Sending accept(Seq:%d, N:%d) to %v\n", px.peers[px.me], seq, n, peer)
		if i == px.me {
			px.Accept(&AcceptArgs{seq, n, v_a}, &reply)
		} else {
			call(peer, "Paxos.Accept", &AcceptArgs{seq, n, v_a}, &reply)
		}
		
		//fmt.Printf("%v: Accept_reply(%+v) from %v\n", px.peers[px.me], reply, peer)
		if reply.Ok == true {
			px.done[i] = reply.Done			// piggybacked done value from replying server
			numAcceptOKs++
		}
	}
	
	return numAcceptOKs
}

// send decision to all servers
func (px* Paxos) sendDecided(seq int, n int, v_a interface{}) (bool) {

	for i, peer := range px.peers {
	
		var reply DecidedReply
		//fmt.Printf("%v: Sending decided(Seq:%d, N:%d) to %v\n", px.peers[px.me], seq, n, peer)
		if i == px.me {
			px.Decided(&DecidedArgs{seq, n, v_a}, &reply)
		} else {
			call(peer, "Paxos.Decided", &DecidedArgs{seq, n, v_a}, &reply)
		}
		
		//fmt.Printf("%v: Decided_reply(%+v) from %v\n", px.peers[px.me], reply, peer)
		if reply.Ok == true {
			px.done[i] = reply.Done			// piggybacked done value from replying server
		} 
	}
	
	return true
}

// Acceptor's prepare handler
func (px* Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()
	
	if args.Seq < px.minInstance {										// Forgotten instance
		reply.Ok = false
		return fmt.Errorf("Prepare(%v): Sequence number too low; minInstance = %d", args, px.minInstance)
	}
	
	if _, exists := px.instances[args.Seq]; exists == false {			// if server is hearing of instance number for first time
		px.instances[args.Seq] = &Instance{Pending, 0, 0, nil, false}	// create new entry
		if px.maxInstance < args.Seq {
			px.maxInstance = args.Seq
		}
		if px.minInstance == -1 {
			px.minInstance = 0
		}
	}
	
	//fmt.Printf("%v: Prepare(%+v)\n", px.peers[px.me], args)
	if args.N > px.instances[args.Seq].n_p {
		px.instances[args.Seq].n_p = args.N
		reply.Seq = args.Seq
		reply.Ok = true
		reply.N = args.N
		reply.N_a = px.instances[args.Seq].n_a
		reply.V_a = px.instances[args.Seq].v_a
	} else {
		reply.Seq = args.Seq
		reply.Ok = false
		reply.N = args.N;
		reply.N_a = px.instances[args.Seq].n_a
		reply.V_a = px.instances[args.Seq].v_a
		return fmt.Errorf("Prepare(%v): Error n < n_p", args)
	}
	reply.Done = px.done[px.me]
	
	if !px.instances[args.Seq].driver && px.instances[args.Seq].status != Decided {		// start a thread that ensures decision is reached for this instance
		go px.driveDecision(args.Seq, nil)
		px.instances[args.Seq].driver = true
	}
	
	return nil
} 

//Acceptor's accept handler
func (px* Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()
	
	//fmt.Printf("%v: Accept(%+v)\n", px.peers[px.me], args)
	if _, exists := px.instances[args.Seq]; exists == false && args.Seq >= px.minInstance {	// if server is hearing of instance number for first time
		px.instances[args.Seq] = &Instance{Pending, 0, 0, nil, false}
		if px.maxInstance < args.Seq {
			px.maxInstance = args.Seq
		}
		if px.minInstance == -1 {
			px.minInstance = 0
		}
	}
	
	if args.Seq >= px.minInstance && args.N >= px.instances[args.Seq].n_p {
		px.instances[args.Seq].n_p = args.N
		px.instances[args.Seq].n_a = args.N
		px.instances[args.Seq].v_a = args.V
		reply.Seq = args.Seq
		reply.Ok = true
		reply.N = args.N
	} else {
		reply.Seq = args.Seq
		reply.Ok = false
		return fmt.Errorf("Accept(%v): Sequence number too low or n < n_p", args)
	}
	reply.Done = px.done[px.me]
	
	if !px.instances[args.Seq].driver && px.instances[args.Seq].status != Decided {		// start a thread that ensures decision is reached for this instance
		go px.driveDecision(args.Seq, args.V)
		px.instances[args.Seq].driver = true
	}
	
	return nil
}

//Decided handler
func (px* Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {

	px.mu.Lock()
	defer px.mu.Unlock()
	
	if  _, exists := px.instances[args.Seq]; exists == false && args.Seq >= px.minInstance {
		px.instances[args.Seq] = &Instance{Pending, 0, 0, nil, false}
		if px.maxInstance < args.Seq {
			px.maxInstance = args.Seq
		}
		if px.minInstance == -1 {
			px.minInstance = 0
		}
	}
	
	if args.Seq >= px.minInstance {
		//fmt.Printf("%v: Decided(%+v)\n", px.peers[px.me], args)
		px.instances[args.Seq].status = Decided			// mark the log entry as decided
		px.instances[args.Seq].v_a = args.V
		reply.Seq = args.Seq
		reply.Done = px.done[px.me]
		reply.N = args.N
		reply.Ok = true
	} else {
		reply.Ok = false
		reply.Seq = args.Seq
		reply.N = args.N
		return fmt.Errorf("Decided(%v): Sequence number too low", args)
	}
	
	if !px.instances[args.Seq].driver && px.instances[args.Seq].status != Decided {		// start a thread that ensures decision is reached for this instance
		go px.driveDecision(args.Seq, args.V)
		px.instances[args.Seq].driver = true
	}
	
	return nil
}

//Find majority count
func (px* Paxos) isMajority(count int) bool {
	if (float64(count) > float64(len(px.peers)) / 2.0) {
		return true
	}
	return false
}

// Attempt to drive the server to decision on a paxos instance by starting a new proposal periodically
func (px* Paxos) driveDecision(seq int, value interface{}) {
	to := time.Second
	for {
		time.Sleep(to)
		status, _ := px.Status(seq)
		if status != Pending {
			//fmt.Printf("%v: Seq %v decided\n", px.peers[px.me], seq) 
			return 
		} else {
			//fmt.Printf("%v: Seq %v driving decision after waiting %v sec\n", px.peers[px.me], seq, to) 
			px.Propose(seq, value)
		}
			
		if to < 10 * time.Second {
			to *= 2
		}
	}
}
