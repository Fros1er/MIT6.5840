package mr

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Queue struct {
	queue []string
}

func (q *Queue) Enqueue(data string) {
	q.queue = append(q.queue, data)
}

func (q *Queue) Dequeue() string {
	res := q.queue[0]
	q.queue = q.queue[1:]
	return res
}
func (q *Queue) Empty() bool {
	return len(q.queue) == 0
}

type Coordinator struct {
	nReduce  int
	workerId atomic.Int32
	//completionMap map[string]chan bool
	completionMap    map[int]chan bool
	completedWorkers map[int]struct{}
	//distributionMap map[int]string // worker Id -> map task
	queue Queue
	stage int // 0: map not done 1: map done 2: all done
	mtx   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) waitForMapTask(mapTask string, id int, done chan bool) {
	select {
	case <-done:
		return
	case <-time.After(10 * time.Second):
		c.mtx.Lock()
		c.queue.Enqueue(mapTask)
		delete(c.completionMap, id)
		c.mtx.Unlock()
	}
}

func (c *Coordinator) GetId(args *GetIdArgs, reply *GetIdReply) error {
	reply.Id = int(c.workerId.Add(1)) - 1
	reply.NReduce = c.nReduce
	println("GetId: ", reply.Id, reply.NReduce)
	return nil
}

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	reply.Work = None

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.stage == 2 {
		if len(c.completionMap) != 0 {
			reply.Work = WWait
		} else {
			reply.Work = WDone
		}
		return nil
	}

	if val, ok := c.completionMap[args.Id]; ok {
		val <- true
		delete(c.completionMap, args.Id)
		c.completedWorkers[args.Id] = struct{}{}
	}

	if !c.queue.Empty() {
		if c.stage == 0 {
			reply.Work = WMap
		} else if c.stage == 1 {
			reply.Work = WReduce
		} else {
			panic("should not happen")
		}

		reply.Data = c.queue.Dequeue()
		channel := make(chan bool)
		c.completionMap[args.Id] = channel
		go c.waitForMapTask(reply.Data, args.Id, channel)
		return nil
	}

	// queue is empty, wait for uncompleted tasks
	if len(c.completionMap) != 0 {
		reply.Work = WWait
		return nil
	}

	c.stage += 1
	if c.stage == 1 {
		var completedWorkers []int
		for id := range c.completedWorkers {
			completedWorkers = append(completedWorkers, id)
		}
		for i := 0; i < c.nReduce; i++ {
			task := struct {
				Workers []int `json:"workers"`
				Bucket  int   `json:"bucket"`
			}{
				completedWorkers,
				i,
			}
			data, err := json.Marshal(task)
			if err != nil {
				panic(err)
			}
			c.queue.Enqueue(string(data))
		}
		reply.Work = WWait
		return nil
	} else if c.stage == 2 {
		if len(c.completionMap) != 0 {
			reply.Work = WWait
		} else {
			reply.Work = WDone
		}
		return nil
	}

	panic("should not reach here")
}

// start a thread that listens for RPCs from worker.go
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.stage == 2
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:          nReduce,
		workerId:         atomic.Int32{},
		completionMap:    make(map[int]chan bool),
		completedWorkers: make(map[int]struct{}),
		//completionMap:   make(map[string]chan bool),
		//distributionMap: make(map[int]string),
		queue: Queue{files},
	}
	println(files, nReduce)

	c.server()
	return &c
}
