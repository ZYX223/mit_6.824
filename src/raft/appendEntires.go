package raft


import "fmt"

// AppendReply 状态值
const (
	Success = 1
	
)
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct{
	Term  int
	LeaderId  int
	PreLogIndex	int
	PreLogTerm int
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type AppendEntriesReply struct{
	Term int
	AppendEntriesState int
}

//
// example RequestVote RPC handler.
//
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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