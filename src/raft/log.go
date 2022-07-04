package raft


// log entry
type  LogEntry struct{
	Term int
	Index int
	Command interface{}
}

func (rf *Raft) getNextEntires(logindex int) [] LogEntry{
	index:= logindex-1
	return rf.logEntires[index:]
}

func (rf *Raft) LogReplicate(PreIndex int, LogEntires *[]LogEntry){
	index:=PreIndex-1
	preEntires:= rf.logEntires[:index+1]
	nxetEntires:= *LogEntires
	preEntires = append(preEntires, nxetEntires...)
	rf.logEntires = preEntires
	return
}

func (rf *Raft) getLastLog() LogEntry{
	
	return rf.logEntires[len(rf.logEntires)-1]
}

func (rf *Raft) getLog(logindex int) LogEntry{
	index:= logindex-1
	return rf.logEntires[index]
}