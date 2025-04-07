package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := GetIdArgs{}
	reply := GetIdReply{}
	if !call("Coordinator.GetId", &args, &reply) {
		return
	}
	id := reply.Id
	nReduce := reply.NReduce
	fmt.Printf("Worker %d started, nReduce %d\n", id, nReduce)

	for {
		args := GetWorkArgs{}
		args.Id = id
		reply := GetWorkReply{}
		if !call("Coordinator.GetWork", &args, &reply) {
			return
		}
		//fmt.Printf("Worker %d, Received %v, Data %s\n", id, reply.Work, reply.Data)
		switch reply.Work {
		case None:
			panic("work is none!")
		case WMap:
			filename := reply.Data
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

			files := make([]*os.File, nReduce)

			for i := 0; i < nReduce; i++ {
				file, err := os.OpenFile(fmt.Sprintf("mr-intermediate-%d-%d", id, i), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
				if err != nil {
					panic(err)
				}
				files[i] = file
			}
			for _, kv := range kva {
				fmt.Fprintf(files[ihash(kv.Key)%nReduce], "%s %s\n", kv.Key, kv.Value)
			}

			for _, file := range files {
				file.Close()
			}
			break
		case WReduce:
			var data struct {
				Workers []int `json:"workers"`
				Bucket  int   `json:"bucket"`
			}
			if err := json.Unmarshal([]byte(reply.Data), &data); err != nil {
				panic(err)
			}

			var kva []KeyValue
			for _, i := range data.Workers {
				file, err := os.Open(fmt.Sprintf("mr-intermediate-%d-%d", i, data.Bucket))
				if err != nil {
					panic(err)
				}
				for {
					var kv KeyValue
					_, err := fmt.Fscanf(file, "%s %s\n", &kv.Key, &kv.Value)
					if err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(kva))

			file, _ := os.Create(fmt.Sprint("mr-out-", data.Bucket))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

				i = j
			}
			file.Close()

			break
		case WWait:
			time.Sleep(500 * time.Millisecond)
			break
		case WDone:
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
