package raft


// log entry
type  LogEntry struct{
	Term int
	Command interface{}
}

func (rf *Raft) getNextEntires(index int) [] LogEntry{
	return rf.logEntires[index:]
}

func (rf *Raft) LogReplicate(PreIndex int, LogEntires *[]LogEntry){
	preEntires:= rf.logEntires[:PreIndex+1]
	nxetEntires:= *LogEntires
	preEntires = append(preEntires, nxetEntires...)
	rf.logEntires = preEntires
	return
}