package raft

import "time"
import "fmt"
import "math/rand"


// Timer function
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150 + rand.Intn(200)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}
func (rf *Raft) resetVoteTimer() {
	t := time.Now()
	VoteTimeout := time.Duration(50+ rand.Intn(150)) * time.Millisecond
	rf.voteTime = t.Add(VoteTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.term || rf.term == 0 {
		rf.state = Follower
		rf.term = term
		rf.voteFor = -1
		fmt.Printf("[%d]: set term %v\n", rf.me, rf.term)
		rf.persist()
	}
}