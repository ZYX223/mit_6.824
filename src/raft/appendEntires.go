package raft


import "fmt"

// AppendReply 状态值
const (
	Success = 2
	LogRep_Fail = 4
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
	LogEntires   []LogEntry
	LeaderCommit int
	Server int
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

// Success Follower 成功match PreLogIndex,PreLogTerm
// TermLower Term < rf.term


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	//fmt.Printf("%v has recv AppendEntries from %v \n",rf.me,args.LeaderId)
	reply.Term = rf.term
	// Leader 任期低
	if args.Term < rf.term{
		//fmt.Printf("%v has recv AppendEntries from %v, and TermLower Follower term is %v, Leader term is %v! \n",rf.me,args.LeaderId,rf.term,args.Term)
		reply.AppendEntriesState = TermLower
		return
	}
	rf.resetElectionTimer()
	// 更新 commitIndex
	if args.LeaderCommit >rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit,rf.getLastLog().Index)
		rf.apply()
		//fmt.Printf("[%v] try to apply log %v to client \n",rf.me,rf.commitIndex)
	}
	
	// preIndex 未匹配到  Fail
	preIndex := args.PreLogIndex
	preTerm :=  args.PreLogTerm			
	if preIndex > rf.getLastLog().Index || rf.getLog(preIndex).Term !=preTerm{
		reply.AppendEntriesState = LogRep_Fail
		fmt.Printf("[%v] LogRep_Fail serverid: [%v] preIndex: %v, rf.getLastLog().Index: %v, preTerm: %v \n",rf.me,args.Server,preIndex,
		rf.getLastLog().Index,preTerm)
		return
	}
	
	reply.AppendEntriesState = Success
	// 成功匹配到 进行log复制
	rf.LogReplicate(preIndex,&args.LogEntires)
	//fmt.Printf("new LogEntires index is %v, term is %v \n",rf.logEntires[0].Index,rf.logEntires[0].Term)
	
	if rf.state == Candidate{
		rf.state = Follower
		//fmt.Printf("%v has recv AppendEntries from %v  Candidate=>Follower \n",rf.me,args.LeaderId)
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
// 函数入口
func (rf *Raft) appendEntiresSend(heartBeat bool){
	
	rf.resetElectionTimer()
	rf.state = Leader
	
	for server:=0;server<len(rf.peers);server++{
		if server == rf.me{
			continue
		}
		args:= AppendEntriesArgs{
			Term : rf.term,
			LeaderId: rf.me,
			LeaderCommit: rf.commitIndex,
			Server: server,
		}
		// 初始化 next
		if rf.nextIndex[server] <= 0{
			rf.nextIndex[server] = 1
		}
		if rf.getLastLog().Index >= rf.nextIndex[server] || heartBeat{
			preLog:= rf.getLog(rf.nextIndex[server]-1)
			args.PreLogIndex = preLog.Index
			args.PreLogTerm = preLog.Term
			args.LogEntires = rf.getNextEntires(preLog.Index+1)
			go rf.leaderSendAppend(server,&args)
		}
	}
}
func (rf *Raft) leaderSendAppend(server int, args *AppendEntriesArgs){
	reply:= AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server,args,&reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch reply.AppendEntriesState{
	case TermLower:
		fmt.Printf("Leader %v AppendEntries TermLower \n",rf.me)
		rf.setNewTerm(reply.Term)
	case Success:
		fmt.Printf("Leader %v AppendEntries LogRep_Success to %v\n",rf.me,server)
		// 更新 nextIndex matchIndex
		match:= args.PreLogIndex + len(args.LogEntires)
		next:= match+1
		rf.matchIndex[server] = match
		rf.nextIndex[server] = next
	case LogRep_Fail:
		fmt.Printf("Leader %v AppendEntries LogRep_Fail to %v, nextIndex: %v\n",rf.me,server,rf.nextIndex[server])
		// 减小 nextIndex值 待优化
		if rf.nextIndex[server] >1{
			rf.nextIndex[server]--
		}
	}
	rf.leaderCommit()
	
}

func (rf *Raft) leaderCommit(){
	for logIndex:= rf.commitIndex+1;logIndex <= rf.getLastLog().Index;logIndex++{
		if rf.getLog(logIndex).Term != rf.term{
			continue
		}
		count:=1
		for server:=0;server<len(rf.peers);server++{
			if server == rf.me{
				continue
			}
			if rf.matchIndex[server] >=logIndex{
				count++
			}
			if count > len(rf.peers)/2{
				rf.commitIndex = logIndex
				// Apply
				fmt.Printf("leader %v try to apply logIndex %v to client \n",rf.me,logIndex)
				rf.apply()
				break
			}
		}
	}
}