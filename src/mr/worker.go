package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	conn ,err := connectToCoordinator()
	if err!=nil{
		panic("connect to coordinator failed")
	}
	defer conn.Close()

	regarg := Register{}
	regply := RegisterReply{}
	err = conn.Call("Coordinator.Register",&regarg,&regply)
	if err!=nil{
		log.Println("register worker failed",err)
		return
	}else{
		log.Println("注册成功",regply.WorkerID)
	}

	ticker := time.NewTicker(1*time.Second)
	defer ticker.Stop()
	for{
		select {
		case <-ticker.C:
			arg := TaskQuery{NodeID: regply.WorkerID}
			reply := TaskQueryReply{}
			err := conn.Call("Coordinator.TaskQuery",&arg,&reply)
			log.Println("返回任务",reply)
			if err!=nil{
				fmt.Println(err)
			}else{
				//ticker.Stop()
				if reply.TaskType == TaskMapping{
					intermediate := []KeyValue{}
					for _, filename := range reply.Files {
						file, err := os.Open(filename)
						if err != nil {
							log.Fatalf("cannot open %v", filename)
						}
						content, err := ioutil.ReadAll(file)
						if err != nil {
							log.Fatalf("cannot read %v", filename)
						}
						file.Close()
						kva := mapf(filename, string(content))
						intermediate = append(intermediate, kva...)
					}
					sort.Sort(ByKey(intermediate))

					oname := "map-res-"+reply.TaskID
					ofile, _ := os.Create(oname)
					i := 0
					for i < len(intermediate) {
						// this is the correct format for each line of Reduce output.
						fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, intermediate[i].Value)
						i ++
					}
					ofile.Close()

					submitTask := SubmitTask{
						TaskID: reply.TaskID,
						FileName: oname,
						NodeID: regply.WorkerID,
					}
					rpy := SubmitTaskReply{}
					err := conn.Call("Coordinator.SubmitTask",&submitTask,&rpy)
					if err!=nil{
						log.Println(err)
					}
				}

				if reply.TaskType == TaskReducing{
					//intermediate := []KeyValue{}
					for _, filename := range reply.Files {
						ofile, _ := os.Open(filename)
						content, err := ioutil.ReadAll(ofile)
						if err != nil {
							log.Fatalf("cannot read %v", filename)
						}
						print(string(content))
						//output := reducef(intermediate[i].Key, values)

						//kva := mapf(filename, )
						//
						//oname := "mr-out-0"
						//i := 0
						//for i < len(intermediate) {
						//	j := i + 1
						//	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						//		j++
						//	}
						//	values := []string{}
						//	for k := i; k < j; k++ {
						//		values = append(values, intermediate[k].Value)
						//	}
						//	output := reducef(intermediate[i].Key, values)
						//
						//	// this is the correct format for each line of Reduce output.
						//	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
						//
						//	i = j
						//}

						ofile.Close()
					}
				}
				//ticker.Reset(time.Second)
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func connectToCoordinator()(*rpc.Client, error){
	sockname := coordinatorSock()
	return rpc.DialHTTP("unix", sockname)
}

func CallRegister() {

	// declare an argument structure.
	args := Register{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
