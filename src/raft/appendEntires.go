package raft


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
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type AppendEntriesReply struct{
	Term int
	AppendEntriesState int
	ReIndex int
	ReTerm int
}

//
// example RequestVote RPC handler.
//


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	DPrintf("%v has recv AppendEntries from %v \n",rf.me,args.LeaderId)
	reply.Term = rf.term
	// term 失效
	if args.Term > rf.term{
		rf.setNewTerm(args.Term)
		DPrintf("[%v] term is vaild \n",rf.me)
		reply.AppendEntriesState = -1
		return
	}

	reply.ReIndex = 0
	reply.ReTerm = 0

	// Leader 任期低
	if args.Term < rf.term{
		DPrintf("%v has recv AppendEntries from %v, and TermLower Follower term is %v, Leader term is %v! \n",rf.me,args.LeaderId,rf.term,args.Term)
		reply.AppendEntriesState = TermLower
		return
	}
	rf.resetElectionTimer()
	if rf.state == Candidate{
		rf.state = Follower
	}
	// 处理 log 一致性
	preIndex := args.PreLogIndex
	preTerm :=  args.PreLogTerm		
	
	if preIndex > rf.getLastLog().Index{
		reply.AppendEntriesState = LogRep_Fail
		DPrintf("[%v] LogRep_Fail preIndex: %v, rf.getLastLog().Index: %v, preTerm: %v \n",rf.me,preIndex,
		rf.getLastLog().Index,preTerm)
		reply.ReIndex = rf.getLastLog().Index
		return 
	}
	if rf.getLog(preIndex).Term !=preTerm{
		reply.AppendEntriesState = LogRep_Fail
		DPrintf("[%v] LogRep_Fail preIndex: %v, rf.getLastLog().Index: %v, preTerm: %v \n",rf.me,preIndex,
		rf.getLastLog().Index,preTerm)
		log:= rf.findPreTermLog(preIndex)
		reply.ReIndex = log.Index
		reply.ReTerm = log.Term
		return
	}
	reply.AppendEntriesState = Success
	// 成功匹配到 进行log复制
	// rf.LogReplicate(preIndex,&args.LogEntires)
	// rf.persist()
	
	for idx, entry := range args.LogEntires {
		// append entries rpc 3
		if entry.Index <= rf.getLastLog().Index && rf.getLog(entry.Index).Term != entry.Term {
			rf.logEntires = rf.logEntires[:entry.Index-1]
			DPrintf("[%v] index: %v \n",rf.me,entry.Index)
			rf.persist()
		}
		// append entries rpc 4
		if entry.Index > rf.getLastLog().Index {
			rf.logEntires = append(rf.logEntires,args.LogEntires[idx:]...)
			DPrintf("[%d]: follower append [%v]", rf.me, args.LogEntires[idx:])
			rf.persist()
			break
		}
	}


	// 更新 commitIndex
	if args.LeaderCommit >rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit,rf.getLastLog().Index)
		rf.apply()
	}

	// 可重新投票
	rf.voteFor = -1
	
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
	lastLog := rf.getLastLog()
	for server:=0;server<len(rf.peers);server++{
		if server == rf.me{
			continue
		}
		args:= AppendEntriesArgs{
			Term : rf.term,
			LeaderId: rf.me,
			LeaderCommit: rf.commitIndex,
		}
		
		nextIndex := rf.nextIndex[server]
		if nextIndex <= 0 {
			nextIndex = 1
		}
		if lastLog.Index+1 < nextIndex {
			nextIndex = lastLog.Index
		}
		if lastLog.Index >= rf.nextIndex[server] || heartBeat{
			preLog:= rf.getLog(nextIndex-1)
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
		DPrintf("Leader %v AppendEntries TermLower \n",rf.me)
		rf.setNewTerm(reply.Term)
	case Success:
		DPrintf("Leader %v AppendEntries LogRep_Success to %v\n",rf.me,server)
		// 更新 nextIndex matchIndex
		match:= args.PreLogIndex + len(args.LogEntires)
		next:= match+1
		rf.matchIndex[server] = match
		rf.nextIndex[server] = next
	case LogRep_Fail:
		DPrintf("Leader %v AppendEntries LogRep_Fail to %v, nextIndex: %v\n",rf.me,server,rf.nextIndex[server])
		if reply.ReTerm == 0{
			
			rf.nextIndex[server] = reply.ReIndex +1
		}else{
			lastLogindex := rf.findLastLogInTerm(reply.ReTerm)
			DPrintf("[%v]: lastLogindex %v\n", rf.me, lastLogindex)
			if lastLogindex > 0 {
				rf.nextIndex[server] = lastLogindex +1
			} else {
				DPrintf("[%v] reply.ReIndex: %v\n",rf.me,reply.ReIndex)
				rf.nextIndex[server] = reply.ReIndex +1
			}
		}
	default:
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
	}
	rf.leaderCommit()
	
}

func (rf *Raft) leaderCommit(){
	if rf.state != Leader {
		return
	}
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
				DPrintf("leader %v try to apply logIndex %v to client \n",rf.me,logIndex)
				rf.apply()
				break
			}
		}
	}
}