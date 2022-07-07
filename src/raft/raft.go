package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync/atomic"
import "../labrpc"
import "time"
import "fmt"
import "sync"
// import "bytes"
// import "../labgob"


// rf 状态值
const (
	Follower = 1
	Candidate = 2
	Leader = 3
) 

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	peers_num int
	// vote 相关
	term	  int				  // currentTerm
	isleader  bool
	state     int				  // rf 状态值 follower/candidate/leader
	voteNums  int                 // 选票数量
	voteFor   int                 // 投票给谁
	// Timer 相关
	electionTime time.Time	  	  // 超时选举时间
	heartBeat	 time.Duration	  // Leader 心跳间隔
	voteTime time.Time			  // Vote 到期时间戳
	// log 相关
	logEntires   []LogEntry		  // 本地log列表,首个索引为1
	// Volatile state on all servers
	commitIndex int				  // 最近被提交的日志索引
	lastApplied int				  // 最近被应用到状态机的日志索引
	// Volatile state on leaders
	nextIndex []int				  // 对于每个server,leader 将要发送的log index
	matchIndex []int			  // 对于每个server, match到的最近log index
	// applyCh
	applyCh	  chan ApplyMsg
	applyCond *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.term
	isleader := rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.term
	isLeader := rf.state == Leader
	if !isLeader{
		return index, term, false
	}
	// Your code here (2B).
	index = len(rf.logEntires) +1
	log:=LogEntry{
		Term :term,
		Index: index,
		Command :command,
	}
	rf.logEntires = append(rf.logEntires,log)
	// leader 发送日志包
	fmt.Printf("[%v] start append log %v\n",rf.me,index)
	rf.appendEntiresSend(false)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func (rf *Raft) Ticker(){
	for !rf.killed(){
		switch rf.state{
		case Follower:
			if time.Now().After(rf.electionTime) {
				fmt.Printf("%v ElectionTimeOut \n",rf.me)
				rf.leaderElection()
			}
		case Candidate:
			if time.Now().After(rf.voteTime) {
				fmt.Printf("%v VoteTimeOut \n",rf.me)
				rf.leaderElection()
			}
		case Leader:
			// 不含Log 的心跳包
			rf.appendEntiresSend(true)
			time.Sleep(rf.heartBeat)
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	time.Sleep(time.Millisecond*50)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peers_num = len(peers)

	// Your initialization code here (2A, 2B, 2C).
	rf.heartBeat = 50 *time.Millisecond
	rf.voteFor = -1
	rf.state = Follower
	// log相关初始化
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int,len(rf.peers))	
	rf.matchIndex = make([]int,len(rf.peers))
	// applyCh
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// 设置 Election_TimeOut
	rf.resetElectionTimer()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Ticker()
	go rf.Applier()
	return rf
}
func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	//fmt.Printf("[%v]: rf.applyCond.Broadcast() \n", rf.me)
}

func (rf *Raft) Applier(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed(){
		if rf.commitIndex >rf.lastApplied && rf.getLastLog().Index > rf.lastApplied{
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.getLog(rf.lastApplied).Command,
				CommandIndex:  rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}else{
			rf.applyCond.Wait()
		}
	}
}