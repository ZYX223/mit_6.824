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
	return rf.getLog(len(rf.logEntires))
}

func (rf *Raft) getLog(logindex int) LogEntry{
	index:= logindex-1
	if index < 0{
		entry:= LogEntry{
			Term:-1,
			Index: 0,
		}
		return entry
	}
	return rf.logEntires[index]
}