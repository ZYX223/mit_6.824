package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "fmt"

// 任务状态值
const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)
// master 状态
const (
	Master_Init = 0
	Q1_Not_Empty_MapTask_Not_Done = 1
	Q1_Empty_MapTask_Not_Done = 2
	Q3_Not_Empty_MapTask_Done = 3
	Q3_Empty_ReduceTask_Not_Done = 4
	Q3_Empty_ReduceTask_Done = 5
	Task_error = -1
)
// 时间周期
const (
	MaxTaskRunTime   = time.Second * 5 
	ScheduleInterval = time.Millisecond * 500
)

// task 类
type Task struct {
	Task_type string // map  or reduce
	Filename string // 文件名
	Task_id int	// 任务编号
	BeginTime time.Time // 开始时间
	Status int // 状态
	Worker_id int
	NMaps int // map task 任务数
	NReduce int // reduce task 任务数
	IsExist bool // 是否存在
} 

type Master struct {
	// Your definitions here.
	// 任务队列
	MapTaskReady_queue []Task // Q1
	TaskRunning_queue []Task // Q2
	ReduceTaskReady_queue []Task // Q3
	// 任务完成列表
	MapTaskFin_list [] int 
	ReduceTaskFin_list [] int 
	// 队列锁
	mutex sync.Mutex
	mu sync.Mutex
	// worker 编号
	workerSeq int
	// matser 状态
	status int
	// task 相关
	nMap int
	nReduce int
	nMap_Fin int
	nReduce_Fin int
}

func (m* Master) lock(){
	m.mutex.Lock()
}

func (m* Master) unlock(){
	m.mutex.Unlock()
}

func (m* Master) Pop(q *[]Task, q_type string) Task{
	m.lock()
	arrayLen := len(*q)
	if arrayLen == 0{
		m.unlock()
		fmt.Printf("Pop master get task fail queue is empty \n")
		error_task:= Task{}
		return error_task
	}
	ret := (*q)[arrayLen-1]
	*q = (*q)[:arrayLen-1]
	// master status 转换
	if len((*q)) == 0 &&  (q_type == "Q1" || q_type == "Q3"){
		if m.status == 1{
			m.status = 2
		}else if m.status == 3{
			m.status = 4
		}
	}
	m.unlock()
	return ret
}

func (m* Master) Push(q *[]Task,t Task, q_type string){
	m.lock()
	*q = append(*q,t)
	m.unlock()
}
func (m* Master) Remove(q *[]Task,t Task, q_type string){
	m.lock()
	flag := 0
	for index,task:= range *q{
		if (task.Task_id == t.Task_id){
			*q = append((*q)[:index], (*q)[index+1:]...) 
			fmt.Printf("Running queue remove Finish task_id: %v, Worker_id: %v \n",task.Task_id,task.Worker_id)
			flag = 1
			break
		}
	}
	if (flag == 0){
		fmt.Printf("Running queue remove Fail task_id: %v, Worker_id: %v \n",t.Task_id,t.Worker_id)
	}
	m.unlock()
}


func (m *Master) initMapTask(files []string, nReduce int) {
	for index,file:= range files{
		task := Task{
			Task_type : "MapTask",
			Filename : file,
			Task_id : index,
			Status : 0,
			Worker_id :-1,
			NMaps : m.nMap,
			NReduce : nReduce,
			IsExist : true,
		}
		m.Push(&m.MapTaskReady_queue,task,"Q1")
	}
	m.status = 1
	fmt.Printf("initMapTask success %v\n",len(m.MapTaskReady_queue))
}

func (m *Master) initReduceTask() {
	for i:=0;i< m.nReduce;i++{
		task := Task{
			Task_type : "ReduceTask",
			Task_id : i,
			Status : 0,
			Worker_id :-1,
			NMaps : m.nMap,
			NReduce : m.nReduce,
			IsExist : true,
		}
		m.Push(&m.ReduceTaskReady_queue,task,"Q2")
	}
	fmt.Printf("initReduceTask success %v\n",len(m.ReduceTaskReady_queue))
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 100
	return nil
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	// Q1_Not_Empty_MapTask_Not_Done = 1
	// Q1_Empty_MapTask_Not_Done = 2
	// Q3_Not_Empty_MapTask_Done = 3
	// Q3_Empty_ReduceTask_Not_Done = 4
	// Q3_Empty_ReduceTask_Done = 5

	// MapTaskReady_queue []Task Q1
	// TaskRunning_queue []Task  Q2
	// ReduceTaskReady_queue []Task Q3

	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("master GetOneTask worker id :%v \n",args.Work_id)
	status:=m.status
	switch status {
	case 1:
		fmt.Printf("GetOneTask case1 \n")
		ret:= m.Pop(&m.MapTaskReady_queue,"Q1")
		ret.BeginTime = time.Now()
		ret.Worker_id = args.Work_id
		reply.Task = &ret
		reply.Single = 200
		m.Push(&m.TaskRunning_queue,ret,"Q2")
	case 2:
		fmt.Printf("GetOneTask case2 \n")
		reply.Single = 100
		return nil
	case 3:
		fmt.Printf("GetOneTask case3 \n")
		ret:= m.Pop(&m.ReduceTaskReady_queue,"Q3")
		ret.BeginTime = time.Now()
		ret.Worker_id = args.Work_id
		reply.Task = &ret
		reply.Single = 200
		m.Push(&m.TaskRunning_queue,ret,"Q2")
	case 4:
		fmt.Printf("GetOneTask case4 \n")
		reply.Single = 100
		return nil
	case 5:
		fmt.Printf("GetOneTask case5 \n")
		reply.Single = 500
		return nil
	}
	return nil
}

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
	fmt.Printf("RegWorker: %v\n",m.workerSeq)
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t:= *args.Task
	fmt.Printf("ReportTask worker_id:%v, task_id: %v, task_type: %s\n",args.WorkerId,t.Task_id,t.Task_type)
	if args.Done{
		t.Status = 3
		// remove
		m.Remove(&m.TaskRunning_queue,t,"Q2")
		switch t.Task_type{
			case "MapTask":
				if (m.MapTaskFin_list[t.Task_id] == 0){
					m.MapTaskFin_list[t.Task_id] = 1
					m.nMap_Fin +=1
				}
				if m.nMap_Fin == m.nMap{
					m.status = 3
					m.initReduceTask()
				}
			case "ReduceTask":
				if (m.ReduceTaskFin_list[t.Task_id] == 0){
					m.ReduceTaskFin_list[t.Task_id] = 1
					m.nReduce_Fin +=1
				}
				if m.nReduce_Fin == m.nReduce{
					m.status = 5
					// set m.Done() true
				}
			default:
				fmt.Printf("task type error \n")
		}
	}else {
		t.Status = 4
		m.status = -1
		// set m.Done() true
	}
	
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 任务心跳检测
func (m *Master) checkTask(){
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Printf("go checkTask() \n")
	tmp_queue := make([]Task,0)
	for _,task:= range m.TaskRunning_queue{
		if time.Now().Sub(task.BeginTime) >MaxTaskRunTime{
			switch task.Task_type {
			case "MapTask":
				fmt.Printf("Get Back MapTask from Running queue Task_id: %v, Worker_id: %v \n",task.Task_id,task.Worker_id)
				m.Push(&m.MapTaskReady_queue,task,"Q1")
				m.status = 1
			case "ReduceTask":
				fmt.Printf("Get Back ReduceTask from Running queue Task_id: %v,Worker_id: %v \n",task.Task_id,task.Worker_id)
				m.Push(&m.ReduceTaskReady_queue,task,"Q3")
				m.status = 3
			default:
				fmt.Printf("checkTask task type error \n")
			}
		}else {
			tmp_queue = append(tmp_queue,task)
		}
	}
	m.TaskRunning_queue = tmp_queue
}

func (m *Master) tickSchedule() {
	// 按说应该是每个 task 一个 timer，此处简单处理
	for !m.Done() {
		fmt.Printf("master status is %v \n",m.status)
		// Goroutine 协程
		go m.checkTask()
		time.Sleep(ScheduleInterval)
	}
}


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	
	
	// Your code here.
	return m.status == -1 || m.status == 5
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.status = 0
	m.nMap = len(files)
	m.nReduce = nReduce

	m.MapTaskFin_list = make([] int, m.nMap)
	m.ReduceTaskFin_list = make([] int,m.nReduce)

	// Your code here.
	m.initMapTask(files,nReduce)
	go m.tickSchedule()
	m.server()
	return &m
}
