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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Register struct {
	ID string
}

type RegisterReply struct {
	WorkerID string
}

type TaskQuery struct{
	NodeID string
}

type TaskQueryReply struct{
	OK   bool
	TaskID string
	TaskType int
	Files []string
}

type SubmitTask struct{
	NodeID string
	TaskID string
	FileName string
}

type SubmitTaskReply struct{
	OK bool
}

type HeartBeat struct{
	TimeStamp int
	WorkDone bool
	WorkerID string
}

type HeartBeatReply struct{
	WorkTimeOut bool  // 工作是否超时10s
	Files 	[]string
	Workas  int
	TaskID  string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
