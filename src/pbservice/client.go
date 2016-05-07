package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
//import "log"

import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	view viewservice.View
	id int64
	seqNo int
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.view = viewservice.View{0, "", ""}
	ck.id = nrand()
	ck.seqNo = 0
	
	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	if (ck.view.Viewnum == 0) {
		ck.UpdateView()
	}
	
	args := &GetArgs{key, false}
	var reply GetReply
	for ok := call(ck.view.Primary, "PBServer.Get", args, &reply); ok == false; ok = call(ck.view.Primary, "PBServer.Get", args, &reply) {
		time.Sleep(viewservice.PingInterval)
		ck.UpdateView()
	}
	
	if reply.Err != "" {
		fmt.Println(reply.Err)
	}
	
	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// Your code here.
	if (ck.view.Viewnum == 0) {
		ck.UpdateView()
	}
	
	ck.seqNo++
//fmt.Printf("Clerk (%v) attempting append k:%v, v:%v (SeqNo:%v)\n", ck.id, key, value, ck.seqNo)
	args := &PutAppendArgs{key, value, op, false, ck.id, ck.seqNo}
	var reply PutAppendReply
	for ok := call(ck.view.Primary, "PBServer.PutAppend", args, &reply); ok == false; ok = call(ck.view.Primary, "PBServer.PutAppend", args, &reply) {
		time.Sleep(viewservice.PingInterval)
		ck.UpdateView()
	}
	
	if reply.Err != "" {
		fmt.Println(reply.Err)
	}
//fmt.Printf("Clerk (%v) completed append k:%v, v:%v (SeqNo:%v)\n", ck.id, key, value, ck.seqNo)
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) UpdateView() {
	view, ok := ck.vs.Get()
	for ; ok == false || view.Viewnum == 0; view, ok = ck.vs.Get() {};
	ck.view = view
}
