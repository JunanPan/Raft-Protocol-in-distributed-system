// raft.go
// =======
package raft

//
// API
// ===
// This is an outline of the API that my raft implementation exposes.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

const HEART_BEAT_INTERVAL = 100 * time.Millisecond

const RAND_RANGE = 100
const RAND_OFFSET = 300

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = false

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// logEntry struct
// ===============
//
// define a struct to hold information about each log entry.
type logEntry struct {
	Term    int
	Command interface{}
}

// getLastLogIndex
// ===============
//
// every time when I call it, it has already locked, so no need to write lock here
// start from 1
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log)
}

// getLastLogTerm
// ==============
//
// return the term of the last log entry
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux           sync.Mutex // Lock to protect shared access to this peer's state
	PutCommandMux sync.Mutex
	peers         []*rpc.ClientEnd // RPC end points of all peers
	me            int              // this peer's index into peers[]
	logger        *log.Logger

	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	role          Role
	applyCh       chan ApplyCommand
	lastHeartbeat time.Time // update when receiving heartbeat from leader or voting other candidates
	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []logEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int
	matchIndex []int
}

// AppendEntriesArgs
// =================
//
// struct for AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

// AppendEntriesReply
// ==================
//
// struct for AppendEntries RPC reply
type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// GetState
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	var me int
	var term int
	var is_leader bool
	me = rf.me
	term = rf.currentTerm
	is_leader = rf.role == Leader
	return me, term, is_leader
}

// RequestVoteArgs
// ===============
//
// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// ================
//
// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote
// ===========
//
// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//handle requestVote RPC
	rf.mux.Lock()
	defer rf.mux.Unlock()
	reply.Term = rf.currentTerm
	// Reject if the term is outdated
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Update current term and reset votedFor if a newer term is seen
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Check log completeness
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	// Check the conditions to decide whether to grant the vote or not
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.role = Follower
	} else {
		reply.VoteGranted = false
	}
	return
}

// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a server.
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply.
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while.
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries
// =============
//
// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	reply.Term = rf.currentTerm

	// Reject if the term is outdated
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.role != Follower {
			rf.role = Follower
			rf.votedFor = -1
		}
	}

	rf.lastHeartbeat = time.Now()
	//Check log consistency first, no matter it is heartbeat or real log
	if args.PrevLogIndex > rf.getLastLogIndex() || (args.PrevLogIndex > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
		reply.Success = false
		return
	}

	// if it is a heartbeat, return immediately
	if len(args.Entries) == 0 {
		// Update commitIndex for heartbeat
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		}
		reply.Success = true
		return
	}

	temp_log := rf.log[:args.PrevLogIndex]
	temp_log = append(temp_log, args.Entries...)
	rf.log = temp_log
	reply.Success = true
	return

}

// sendAppendEntries
// =================
//
// send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// syncLogToPeer
// ============
//
// used by leader to sync log to other peers
func (rf *Raft) syncLogToPeer(peer int) {
	for {
		rf.mux.Lock()
		if rf.role != Leader {
			rf.mux.Unlock()
			return
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		entries := rf.log[prevLogIndex:]
		index := rf.getLastLogIndex()
		prevLogTerm := 0
		if prevLogIndex == 0 {
			prevLogTerm = -1
		} else {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}
		term := rf.currentTerm
		me := rf.me
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mux.Unlock()

		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, args, reply) { //if success, update commitIndex

			if reply.Term > term { //if term is outdated, convert to follower
				rf.mux.Lock()
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.mux.Unlock()
				return
			}
			if reply.Success {
				rf.mux.Lock()
				rf.nextIndex[peer] = index + 1
				rf.matchIndex[peer] = index
				for N := index; N > rf.commitIndex; N-- {
					count := 1
					for j := range rf.peers {
						if j != me && rf.matchIndex[j] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 && rf.log[N-1].Term == rf.currentTerm {
						rf.commitIndex = N
						break
					}
				}
				rf.mux.Unlock()
				return
			} else {
				rf.mux.Lock()
				rf.nextIndex[peer]--
				rf.mux.Unlock()
			}
		} else {
			//probably is disconnected, just return
			return
		}
	}
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false.
//
// Otherwise start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term.
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.PutCommandMux.Lock()
	defer rf.PutCommandMux.Unlock()
	// If this server is not the leader, return false.
	rf.mux.Lock()
	if rf.role != Leader {
		rf.mux.Unlock()
		return -1, -1, false
	}
	entry := logEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	me := rf.me
	rf.mux.Unlock()

	//trigger the log replication process to other followers
	for peer, _ := range rf.peers {
		if peer == me {
			continue
		}
		go rf.syncLogToPeer(peer)
	}

	rf.mux.Lock()
	index := rf.getLastLogIndex()
	term := rf.currentTerm
	role := rf.role
	rf.mux.Unlock()
	return index, term, role == Leader
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
func (rf *Raft) Stop() {
	// nothing here
}

// sendHeartbeats
// ==============
//
// send heartbeats to all peers
func (rf *Raft) sendHeartbeats() {
	for {
		rf.mux.Lock()
		if rf.role != Leader { // stop sending heartbeats if not leader
			rf.mux.Unlock()
			return
		}
		term := rf.currentTerm
		leaderId := rf.me
		prevLogIndex := rf.getLastLogIndex()
		preLogTerm := rf.getLastLogTerm()
		commitIndex := rf.commitIndex
		peers := rf.peers
		rf.mux.Unlock()

		// send heartbeats to all peers
		for i, _ := range peers {
			if i == leaderId {
				continue
			}
			go func(peer int, term int) {
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  preLogTerm,
					Entries:      []logEntry{}, // Empty log entries for heartbeat
					LeaderCommit: commitIndex,
				}
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(peer, args, reply) {
					if reply.Term > term {
						rf.mux.Lock()
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.mux.Unlock()
						return
					}
					if !reply.Success { //prevIndex is not matched
						// sync current log entries to the peer
						go rf.syncLogToPeer(peer)
					}
				}
			}(i, term)

		}
		// Wait for the heartbeat interval before sending the next round of heartbeats
		time.Sleep(HEART_BEAT_INTERVAL)
	}
}

// startElection
// =============
//
// start election and send RequestVote RPCs to all other servers
func (rf *Raft) startElection() {
	rf.mux.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	votesReceived := 1 // vote for itself
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	term := rf.currentTerm
	me := rf.me
	peers := rf.peers
	rf.lastHeartbeat = time.Now()
	rf.mux.Unlock()
	// send RequestVote RPCs to all other servers
	for i, _ := range peers {
		if i != me {
			go func(me int, peer int, term int, lastLogIdx int, lastLogTm int) {
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  me,
					LastLogIndex: lastLogIdx,
					LastLogTerm:  lastLogTm,
				}
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mux.Lock()
					currentTerm := rf.currentTerm
					role := rf.role
					rf.mux.Unlock()
					if reply.VoteGranted && reply.Term == currentTerm {
						rf.mux.Lock()
						votesReceived++
						curVoteReceived := votesReceived
						rf.mux.Unlock()
						if role == Candidate && curVoteReceived > len(peers)/2 {
							rf.mux.Lock()
							rf.role = Leader
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := range rf.peers {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
							rf.mux.Unlock()
							// Start sending heartbeats to other servers.
							go rf.sendHeartbeats()
						}
					} else if reply.Term > currentTerm {
						rf.mux.Lock()
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.mux.Unlock()
					}

				}
			}(me, i, term, lastLogIndex, lastLogTerm)
		}
	}
}

// electionTimeoutRoutine
// ======================
//
// set a random timer to start election
func (rf *Raft) electionTimeoutRoutine() {
	for {
		timeoutDuration := time.Duration(rand.Intn(RAND_RANGE)+RAND_OFFSET) * time.Millisecond // 300ms ~ 400ms
		select {
		case <-time.After(timeoutDuration):
			rf.mux.Lock()
			elapsed := time.Since(rf.lastHeartbeat)
			currentRole := rf.role
			rf.mux.Unlock()
			if currentRole != Leader && elapsed >= timeoutDuration {
				rf.startElection()
			}

		}
	}
}

// applyChRoutine
// ==============
//
// check lastApplied and commitIndex, if there is any new log entry to apply, apply it
func (rf *Raft) applyChRoutine() {
	for {
		//put command into arr first, avoid holding the lock for too long
		commandsToApply := make([]ApplyCommand, 0)
		rf.mux.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied-1]
			commandsToApply = append(commandsToApply, ApplyCommand{
				Index:   rf.lastApplied,
				Command: entry.Command,
			})
		}
		rf.mux.Unlock()

		for _, cmd := range commandsToApply {
			rf.applyCh <- cmd
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// NewPeer
// ====
//
// The service or tester wants to create a Raft server.
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// initialize raft struct
	rf.mux = sync.Mutex{}
	rf.PutCommandMux = sync.Mutex{}
	rf.applyCh = applyCh
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]logEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	/*
		Modify NewPeer() to create a background goroutine that will kick off leader election periodically
		by sending out RequestVote RPCs when it hasn’t heard from another peer for a while. */

	go rf.electionTimeoutRoutine()
	go rf.applyChRoutine()
	return rf
}
