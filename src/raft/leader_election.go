package raft


import "fmt"

// voteReply 状态值
const (
	VoteSuccess = 1 // 投票成功
	HasVoted = 2    // 已投票不能重复投
	TermLower = 3	// 任期过低拒绝投票 (退化为Follower)
	LogLower = 4	// 日志完整性过低拒绝投票 (不退化)
)

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
	if args.LastLogTerm < rf.getLastLog().Term || (args.LastLogTerm == rf.getLastLog().Term && args.LastLogIndex <rf.getLastLog().Index){
		reply.VoteState = LogLower
		return
	}
	// 已投票
	if rf.voteFor != -1 && rf.voteFor != rf.me{
		reply.Term = rf.term
		reply.VoteState = HasVoted
		return
	}
	// Follower重置选举超时，避免多次选举
	rf.resetElectionTimer()
	// 投票成功
	reply.VoteState = VoteSuccess
	// rf term 更新
	if args.Term > rf.term{
		rf.setNewTerm(args.Term)
	}
	rf.voteFor = args.CandidateId
	reply.Term = rf.term
	rf.persist()
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


func (rf *Raft) leaderElection(){
	rf.term +=1
	rf.state = Candidate
	rf.voteFor = rf.me
	// Persister
	rf.persist()
	rf.voteNums =1
	args:= RequestVoteArgs{
		Term : rf.term,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm: rf.getLastLog().Term,
	}
	for server:=0;server<len(rf.peers);server++{
		if server == rf.me{
			continue
		}
		go rf.candidateSendVote(server,&args)
	}
	rf.resetVoteTimer()
}

func (rf *Raft) candidateSendVote(server int, args *RequestVoteArgs){
	reply:= RequestVoteReply{}
	if ok := rf.sendRequestVote(server,args,&reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		if rf.voteNums > rf.peers_num/2 && rf.state == Candidate && rf.term == args.Term{
			fmt.Printf("Server %v has been Leader \n",rf.me)
			rf.state = Leader
			lastLogIndex := rf.getLastLog().Index
			for i, _ := range rf.peers {
				rf.nextIndex[i] = lastLogIndex+1
				rf.matchIndex[i] = 0
			}
			rf.appendEntiresSend(true)
		}
		return
	}
}