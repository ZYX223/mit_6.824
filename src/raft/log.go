package raft

import (
	"fmt"
	"strings"
)

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
	rf.logEntires = rf.logEntires[:index+1]
	rf.logEntires = append(rf.logEntires,*LogEntires...)
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

func (rf *Raft)findPreTermLog(curIndex int) LogEntry{
	curTerm:= rf.getLog(curIndex).Term
	for index:= curIndex; index>=1;index--{
		log:= rf.getLog(index)
		if log.Term != curTerm{
			return log
		}
	}
	return rf.getLog(0)
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for index := rf.getLastLog().Index; index > 0; index-- {
		term := rf.getLog(index).Term
		if term == x {
			return index
		} else if term < x {
			break
		}
	}
	return -1
}

func (e *LogEntry) String() string {
	return fmt.Sprint(e.Term)
}


func (rf *Raft) Log2String() string {
	nums := []string{}
	for _, entry := range rf.logEntires {
		nums = append(nums,  fmt.Sprintf("%4d", entry.Term))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}