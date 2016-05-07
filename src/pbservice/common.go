package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op string
	Forwarded bool
	ClientID int64
	SeqNo int

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Forwarded bool
}

type GetReply struct {
	Err   Err
	Value string
}

type CopyStateArgs struct {
	Kvstore map[string]string
	SeqNos map[int64]int
}

type CopyStateReply struct {
	Err Err
}
// Your RPC definitions here.
