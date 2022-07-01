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
import "math/rand"
import "sync"
// import "bytes"
// import "../labgob"


// rf 状态值
const (
	Follower = 1
	Candidate = 2
	Leader = 3
) 
// voteReply 状态值
const (
	VoteSuccess = 1 // 投票成功
	HasVoted = 2    // 已投票不能重复投
	TermLower = 3	// 任期过低拒绝投票 (退化为Follower)
	LogLower = 4	// 日志完整性过低拒绝投票 (不退化)
)
// AppendReply 状态值
const (
	Success = 1
	
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
	// vote 相关
	term	  int
	isleader  bool
	state     int				  // rf 状态值 follower/candidate/leader
	
	voteNums  int                 // 选票数量
	voteFor   int                 // 
	// Timer 相关
	electionTime time.Time	  	  // 超时选举时间
	heartBeat	 time.Duration	  // Leader 心跳间隔
	voteTime time.Time
	
	peers_num int
	
	// log 相关
	lastLogIndex int
	lastLogTerm int

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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term  int
	CandidateId	int
	LastLogIndex int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int
	VoteState int // 投票状态
}

type AppendEntriesArgs struct{
	Term  int
	LeaderId  int
	PreLogIndex	int
	PreLogTerm int
}
type AppendEntriesReply struct{
	Term int
	AppendEntriesState int
}

type VoteFinArgs struct{
	Term  int
	LeaderId  int
}
type VoteFinReply struct{
	Term  int
	IsRecv bool
}
// tools function
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(200 + rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}
func (rf *Raft) resetVoteTimer() {
	t := time.Now()
	VoteTimeout := time.Duration(rand.Intn(150)) * time.Millisecond
	rf.voteTime = t.Add(VoteTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.term || rf.term == 0 {
		rf.state = Follower
		rf.term = term
		rf.voteFor = -1
		fmt.Printf("[%d]: set term %v\n", rf.me, rf.term)
		//rf.persist()
	}
}

//
// example RequestVote RPC handler.
//

//	VoteSuccess = 1  投票成功
//	HasVoted = 2     已投票不能重复投
//	TermLower = 3	 任期过低拒绝投票 (退化为Follower)
//	LogLower = 4	 日志完整性过低拒绝投票 (不退化)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%v has recv RequestVote from %v \n",rf.me,args.CandidateId)
	
	// 请求者任期低
	if args.Term < rf.term{
		reply.Term = rf.term
		reply.VoteState = TermLower
		return
	} 
	// 日志完整性过低拒绝投票
	if args.LastLogIndex <rf.lastLogIndex || args.LastLogTerm < rf.lastLogTerm{
		reply.Term = rf.term
		reply.VoteState = LogLower
		return
	}
	// 已投票
	if rf.voteFor != -1 && rf.voteFor != rf.me{
		reply.Term = rf.term
		reply.VoteState = HasVoted
		return
	}
	
	// 投票成功
	reply.VoteState = VoteSuccess
	// rf term 更新
	if args.Term > rf.term{
		rf.setNewTerm(args.Term)
	}
	rf.voteFor = args.CandidateId
	reply.Term = rf.term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("%v has recv AppendEntries from %v \n",rf.me,args.LeaderId)
	// Leader 任期低
	if args.Term < rf.term{
		fmt.Printf("%v has recv AppendEntries from %v, and TermLower Follower term is %v, Leader term is %v! \n",rf.me,args.LeaderId,rf.term,args.Term)
		reply.AppendEntriesState = TermLower
		return
	}
	rf.resetElectionTimer()
	reply.AppendEntriesState = Success
	if rf.state == Candidate{
		rf.state = Follower
		fmt.Printf("%v has recv AppendEntries from %v  Candidate=>Follower \n",rf.me,args.LeaderId)
	}
	// 可重新投票
	rf.voteFor = -1
	// 更新 term
	if args.Term > rf.term{
		rf.setNewTerm(args.Term)
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).

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


func (rf *Raft) candidateSendVote(server int, args *RequestVoteArgs){
	reply:= RequestVoteReply{}
	if ok := rf.sendRequestVote(server,args,&reply); !ok {
		return
	}
	switch reply.VoteState{
	case TermLower:
		fmt.Printf("Server %v TermLower from Server %v \n",rf.me,server)
		rf.setNewTerm(reply.Term)
		return
	case LogLower:
		fmt.Printf("Server %v LogLower from Server %v\n",rf.me,server)
		return
	case HasVoted:
		fmt.Printf("Server %v HasVoted Request is %v\n",server,rf.me)
		return
	case VoteSuccess:
		fmt.Printf("Server %v VoteSuccess to %v\n",server,rf.me)
		rf.voteNums +=1
		// BecomeLeader
		if rf.voteNums > rf.peers_num/2 && rf.state == Candidate{
			fmt.Printf("Server %v has been Leader \n",rf.me)
			rf.state = Leader
		}
		return
	}
}

func (rf *Raft) leaderSendAppend(server int, args *AppendEntriesArgs){
	reply:= AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server,args,&reply); !ok {
		return
	}
	switch reply.AppendEntriesState{
	case TermLower:
		fmt.Printf("Leader %v AppendEntries TermLower \n",rf.me)
		rf.setNewTerm(reply.Term)
	case Success:
		fmt.Printf("Leader %v AppendEntries Success to %v \n",rf.me,server)
	}
}


func (rf *Raft) leaderElection(){
	rf.term +=1
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.voteNums =1
	args:= RequestVoteArgs{
		Term : rf.term,
		CandidateId: rf.me,
	}
	for server:=0;server<len(rf.peers);server++{
		if server == rf.me{
			continue
		}
		go rf.candidateSendVote(server,&args)
	}
	rf.resetVoteTimer()
}


func (rf *Raft) appendEntiresSend(){
	rf.resetElectionTimer()
	rf.state = Leader
	args:= AppendEntriesArgs{
		Term : rf.term,
		LeaderId: rf.me,
	}
	for server:=0;server<len(rf.peers);server++{
		if server == rf.me{
			continue
		}
		go rf.leaderSendAppend(server,&args)
	} 
}


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
			rf.appendEntiresSend()
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
	// 设置 Election_TimeOut
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Ticker()

	return rf
}
