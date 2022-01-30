package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "github.com/google/uuid"


type taskState int

const(
	TaskIdle = iota
	TaskMapping
	TaskReducing
)


type task struct{
	taskID string
	taskType int
	files []string
}

type node struct{
	mu  sync.Mutex
	nodeID string
	task   task
	cancelFunc *time.Timer
}

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	nodeMap map[string]*node
	reducefiles []string
	reducerCount int
	maxReducerCount int
	idleTask        map[string]*task
	assignedTask    map[string]*task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) TaskQuery(args *TaskQuery, reply *TaskQueryReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node := c.nodeMap[args.NodeID]
	t,err := c.allocateTask()
	if err!=nil{
		reply.OK=false
		return err
	}else{
		reply.OK = true
		reply.TaskType = t.taskType
		reply.Files = t.files
		reply.TaskID = t.taskID

		node.task = *t
		node.cancelFunc = time.AfterFunc(10*time.Second,func(){
			node.mu.Lock()
			defer node.mu.Unlock()
			// 10秒钟未完成任务，取消，重新分配一个备用的
			log.Println("执行任务超时")
			c.mu.Lock()
			if node.task.taskType == TaskReducing{
				c.reducerCount --
			}
			c.idleTask[node.task.taskID] = &node.task
			delete(c.assignedTask,node.task.taskID)
			c.mu.Unlock()
			//node.task = task{}
		})
		if t.taskType == TaskReducing{
			c.reducerCount ++
		}
		return nil
	}
}

func (c *Coordinator)SubmitTask(args *SubmitTask, reply *SubmitTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node := c.nodeMap[args.NodeID]
	log.Println("提交任务args",args)
	node.mu.Lock()
	log.Println("node 状态",node)

	if node.task.taskType == TaskMapping{
		c.reducefiles = append(c.reducefiles,args.FileName)
	}else if node.task.taskType == TaskReducing{
		c.reducerCount --
	}
	delete(c.assignedTask,node.task.taskID)
	node.cancelFunc.Stop()
	node.task = task{}
	node.mu.Unlock()
	log.Println("submit task",c.reducefiles)
	reply.OK = true
	return nil
}

func (c *Coordinator)allocateTask() (*task,error){
	var files []string
	if len(c.idleTask)>0{
		var t *task
		for k,v :=range c.idleTask{
			delete(c.idleTask,k)
			c.assignedTask[k] =v
			t = v
			return t,nil
		}
	}
	if len(c.reducefiles)>0{
		if c.reducerCount >= c.maxReducerCount{
			return nil, errors.New("超过最大允许分配的reducer，等待reducer完成任务")
		}
		files ,c.reducefiles =splitFilesToTask(c.reducefiles)
		t := &task{
			taskID:   uuid.NewString(),
			taskType: TaskReducing,
			files:    files,
		}
		c.assignedTask[t.taskID] = t
		return t,nil
	}
	if len(c.assignedTask)>0{
		return nil, errors.New("等待mapper工作完成")
	}
	return nil, errors.New("没有可分配的任务")
}

func splitFilesToTask(files []string)([]string,[]string){
	const fileAllocatMaxSize = 64*1024*1024
	var filesize  int64= 0
	for i,file :=range files{
		fi,err:=os.Stat(file)
		if err ==nil {
			filesize+=fi.Size()
			if filesize>fileAllocatMaxSize{
				return files[0:i],files[i:]
			}
		}
	}
	return files,[]string{}
}

func (c *Coordinator) Register(args *Register, reply *RegisterReply) error {
	reply.WorkerID = uuid.NewString()
	c.mu.Lock()
	c.nodeMap[reply.WorkerID] = &node{
		nodeID: reply.WorkerID,
		task:   task{},
	}
	c.mu.Unlock()
	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
			nodeMap:         make(map[string]*node),
			reducefiles:     []string{},
			maxReducerCount: nReduce,
			idleTask:        make(map[string]*task),
			assignedTask: 	 make(map[string]*task),
		}
	var taskfiles []string
	for len(files)>0{
		taskfiles, files = splitFilesToTask(files)
		t := task{
			taskID:   uuid.NewString(),
			taskType: TaskMapping,
			files:    taskfiles,
		}
		c.idleTask[t.taskID] = &t
	}
	// Your code here.


	c.server()
	return &c
}
