package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type WorkType int

const (
	None WorkType = iota
	WMap
	WReduce
	WWait
	WDone
)

type GetIdArgs struct {
}

type GetIdReply struct {
	Id      int
	NReduce int
}

type GetWorkArgs struct {
	Id int
}

type GetWorkReply struct {
	Work WorkType
	Data string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
