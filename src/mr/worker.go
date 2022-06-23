package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"
import "time"
import "strings"

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

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()

	// uncomment to send the Example RPC to the master.
	//CallExample()
	

}

func (w *worker) run() {
	// if reqTask conn fail, worker exit
	for {
			t := w.GetTask()
			if  !t.IsExist{
				fmt.Printf("There are no idle tasks here! task_type: %s, work_id: %v,task_id: %v\n",t.Task_type,w.id,t.Task_id)
				time.Sleep(time.Millisecond*500)
				continue
			}
			switch  t.Task_type {
				case "MapTask":
					w.MapTask(t)
				case "ReduceTask":
					w.ReduceTask(t)
				default:
					fmt.Printf("task type error,exit \n")
					os.Exit(1)
			}
		}
}


//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)
	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func (w *worker) GetTask() Task{
	// declare an argument structure.
	args := TaskArgs{}
	args.Work_id = w.id
	reply := TaskReply{}
	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		fmt.Printf("worker get task fail,exit")
		os.Exit(1)
	}

	// send the RPC request, wait for the reply.
	if reply.Task == nil{
		fmt.Printf("worker get task fail %v\n",reply.Single)
		tmp := Task{}
		if reply.Single == 100{
			tmp.IsExist = false
		}
		if reply.Single == 500{
			os.Exit(1)
		}
		return tmp
	}
	fmt.Printf("GetTask success task_type: %s, work_id: %v,task_id: %v\n",(*reply.Task).Task_type,w.id,(*reply.Task).Task_id)
	return *reply.Task
}
// Map task
func (w *worker) MapTask(t Task){
	fmt.Printf("do MapTask work_id: %v, task_id: %v\n",w.id,t.Task_id)
	filename := t.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		w.reportTask(t, false, nil)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		w.reportTask(t, false, nil)
	}
	file.Close()
	kva := w.mapf(filename, string(content))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := fmt.Sprintf("mr-%d-%d", t.Task_id, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}

		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	fmt.Printf("MapTask finish work_id: %v, task_id: %v\n",w.id,t.Task_id)
	w.reportTask(t, true, nil)
}
// Reduce task
func (w *worker) ReduceTask(t Task){
	fmt.Printf("do ReduceTask work_id: %v, task_id: %v\n",w.id,t.Task_id)
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		fileName := fmt.Sprintf("mr-%d-%d", idx, t.Task_id)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	
	if err := ioutil.WriteFile(fmt.Sprintf("mr-out-%d", t.Task_id), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Task = &t
	args.WorkerId = w.id
	args.Task_type = t.Task_type
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		fmt.Printf("report task fail:%+v", args)
	}
	fmt.Printf("report task success work_id :%v, task_id: %v \n",w.id,t.Task_id)
}

func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkerId
	fmt.Printf("register success work_id: %v\n", w.id)
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
