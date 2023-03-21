// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	_ "github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomElectionTimeout is a random number between
	// [electionTimeout, 2 * electionTimeout - 1].
	// It gets reset when raft changes its state to follower or candidate.
	randomElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// Ticks since it reached 2 * electionTimeout when leadTransferee is not zero.
	transferElapsed int
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		log.Panic("Validate config", err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	hardState, confState, _ := raft.RaftLog.storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == raft.id {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}
	// Initial state is follower.
	raft.becomeFollower(0, None)
	// Give different nodes different election timeouts to avoid centralized elections in the same time period.
	raft.randomElectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)
	// Read `term`, `vote`, `committed` from raft hard state which is persisted.
	raft.Term, raft.Vote, raft.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()
	if c.Applied > 0 {
		raft.RaftLog.updateApplied(c.Applied)
	}
	return raft
}

// String return raft's term, id and state.
func (r *Raft) String() string {
	var state string
	switch r.State {
	case StateLeader:
		state = "L"
	case StateCandidate:
		state = "C"
	case StateFollower:
		state = "F"
	}
	return fmt.Sprintf("[TE:%d] [ID:%s-%d]", r.Term, state, r.id)
}

// softState returns soft state.
func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Term,
		RaftState: r.State,
	}
}

// hardState returns hard state.
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if (m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse) && m.Term == 0 {

		log.Panicf("term should be set when sending %s", m.MsgType)

	} else {

		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}

	if m.To == r.id {
		log.Error("message should not be self-addressed when sending %s", m.MsgType)
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an AppendEntries RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		if err == ErrCompacted {
			// The log entry need to be sent exists in the snapshot.
			r.sendSnapshot(to)
			return false
		}
		// This should not happen.
		log.Panicf("Get log term %s", err.Error())
	}
	var entries []*pb.Entry
	n := len(r.RaftLog.entries)
	for i := r.RaftLog.toSliceIndex(prevLogIndex + 1); i < n; i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}

	r.send(msg)

	log.Debugf("%s send AEA: {to=%d, term=%d, index=%d, logTerm=%d, commit=%d, len=%d}",
		r, msg.To, msg.Term, msg.LogTerm, msg.Index, msg.Commit, len(entries))
	return true
}

// sendAppendResponse sends a response for AppendEntries
// RPC to the given peer.
// The param term is not None, which means the prev log
// entry is in conflict.
func (r *Raft) sendAppendResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   index,
		LogTerm: term,
		Reject:  reject,
	}
	r.send(msg)

	log.Debugf("%s send AER: {to=%d, term=%d, index=%d, logTerm=%d, accept=%v}",
		r, msg.To, msg.Term, msg.LogTerm, msg.Index, !msg.Reject)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.send(msg)
	log.Debugf("%s send HBA: {to=%d}", r, msg.To)
}

// sendHeartBeatResponse sends a response for HeartBeat RPC to
// the given peer.
func (r *Raft) sendHeartBeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.send(msg)
	log.Debugf("%s send HBR: {to=%d, accept=%v}", r, msg.To, !reject)
}

// sendRequestVote sends a RequestVote RPC with index and term of
// the last log entry to the given peer.
func (r *Raft) sendRequestVote(to, index, term uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
	}
	r.send(msg)
	log.Debugf("%s send RVA: {to=%d, term=%d, logTerm=%d, index=%d}",
		r, msg.To, msg.Term, msg.LogTerm, msg.Index)
}

// sendRequestVoteResponse send a response for RequestVote RPC to
// the given peer.
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.send(msg)
	log.Debugf("%s send RVR: {to=%d, term=%d, vote=%v}", r, msg.To, msg.Term, !reject)
}

// sendSnapshot sends a snapshot RPC to the given peer.
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.send(msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	log.Debugf("%s send ISA: {to=%d, term=%d, meta(term=%d, index=%d)}",
		r, to, r.Term, snapshot.Metadata.Term, snapshot.Metadata.Index)
}

// sendTransferLeader sends transfer leader request from non-leader node to leader.
func (r *Raft) sendTransferLeader() {

	msg := pb.Message{
		To:      r.Lead,
		MsgType: pb.MessageType_MsgTransferLeader,
	}

	r.send(msg)
	log.Infof("Send leader transfer from %d to %d", r.id, r.Lead)
}

// sendTimeout sends from the leader to the leadership transfer target
// to let the leadership transfer target timeout immediately and start
// a new election.
func (r *Raft) sendTimeout(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
	}
	r.send(msg)
	log.Debugf("%s send TMO: {to=%d}, transfer leadership to: %d", r, to, to)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
		if r.leadTransferee != None {
			r.tickTransfer()
		}
	}
}

// tickElection advances `r.electionElapsed` and determines
// whether to initiate a round of prevote.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		log.Infof("%s election timeout, start a new election", r)
		r.electionElapsed = 0
		_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// tickHeartbeat advances `r.heartbeatElapsed` and determines
// whether to broadcast a round of heartbeat.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		log.Debugf("%s heartbeat timeout, broadcast beat", r)
		r.heartbeatElapsed = 0
		_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// tickTransfer advanced `r.transferElapsed` and determines
// whether to transfer the leadership.
func (r *Raft) tickTransfer() {
	r.transferElapsed++
	// The leadership transfer has not been completed after two rounds of elections.
	// The target node may hang up and be turn leadTransferee into none.
	if r.transferElapsed >= 2*r.electionTimeout {
		r.transferElapsed = 0
		r.leadTransferee = None
	}
}

// becomeFollower transform this peer's state to Follower.
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	log.Infof("%s state: %v -> %v, cur leader: %v", r, r.State, StateFollower, lead)
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate.
func (r *Raft) becomeCandidate() {
	log.Infof("%s state: %v -> %v, prev leader: %v", r, r.State, StateCandidate, r.Lead)
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.votes = make(map[uint64]bool)
	// Vote for itself.
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader.
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Infof("%s state: %s -> %s, prev leader: %d", r, r.State, StateLeader, r.Lead)
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastIndex + 2
			r.Prs[peer].Match = lastIndex + 1
		} else {
			r.Prs[peer].Next = lastIndex + 1
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term: r.Term, Index: lastIndex + 1,
	})
	r.broadcastAppendEntries()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled.
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	if m.Term > r.Term {
		r.leadTransferee = None
		r.becomeFollower(m.Term, None)
	}
	var err error
	switch r.State {
	case StateFollower:
		err = r.stepFollower(m)
	case StateCandidate:
		err = r.stepCandidate(m)
	case StateLeader:
		err = r.stepLeader(m)
	}
	return err
}

// stepFollower is used by follower to step message.
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // local msg used to call follower or candidate to start an election
		r.handleElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		r.sendTransferLeader()
	case pb.MessageType_MsgTimeoutNow:
		r.handleElection()
	}
	return nil
}

// stepCandidate is used by candidate to step message.
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			log.Debugf("%s receive AEA from leader: %d, become follower", r, m.From)
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			log.Debugf("%s receive HBA from leader: %d, become follower", r, m.From)
			r.becomeFollower(m.Term, m.Term)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		r.sendTransferLeader()
	case pb.MessageType_MsgTimeoutNow:
		r.handleElection()

	}
	return nil
}

// stepLeader is used by leader to step message.
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	// ignore
	case pb.MessageType_MsgHup:
	// local msg used to call leader to broadcast heartbeat
	case pb.MessageType_MsgBeat:
		r.broadcastHeartBeat()
	// local msg used to call leader append data to log entries
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			log.Infof("%s process propose: %s", r, m.Entries)
			r.appendEntries(m.Entries)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	// response msg from other nodes for AppendEntries RPC
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	// vote msg from candidate
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	// ignore
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	// heartbeat msg from another leader
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	// request msg to let it transfer leadership
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	// ignore
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

// broadcastHeartBeat is used by leader to broadcast HeartBeat
// RPC to all nodes.
func (r *Raft) broadcastHeartBeat() {
	log.Debugf("%s start to broadcast heartbeat", r)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// broadcastAppendEntries is used by leader to broadcast AppendEntries
// RPC to all nodes.
func (r *Raft) broadcastAppendEntries() {
	log.Debugf("%s start to broadcast entries", r)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// appendEntries is used by leader to append entries to log entries.
func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = entry.Index
		}
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.broadcastAppendEntries()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// handleElection is used by follower or candidate to start an election.
func (r *Raft) handleElection() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.Prs) == 1 {
		log.Infof("%s standalone mode, become leader at %d directly", r, r.Term)
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer, lastIndex, lastLogTerm)
	}
}

// resetElectionTimeout reset `electionElapsed` to zero and
// `randomElectionTimeout` to a new random value.
func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// handleAppendEntries handles AppendEntries RPC request.
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Debugf("%s receive AEA: {from=%d, term=%d, logTerm=%d, index=%d, len=%d, entries=%s}",
		r, m.From, m.Term, m.LogTerm, m.Index, len(m.Entries), m.Entries)

	// 5.1 Reply false if term < currentTerm.
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}

	// For all roles.
	r.resetElectionTimeout()
	r.Lead = m.From

	// 5.2 Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm.
	l := r.RaftLog
	lastIndex := l.LastIndex()
	if m.Index > lastIndex { // need older log entries
		r.sendAppendResponse(m.From, true, None, lastIndex+1)
		return
	}

	// RAFT optimization: Check the difference of prev log entry with leader's.
	if m.Index >= l.FirstIndex() {
		logTerm, err := l.Term(m.Index)
		if err != nil {
			log.Panic("Get log term", err.Error())
		}
		// The term of prev log entry is different.
		if logTerm != m.LogTerm {
			// Find the first index of log entry whose term is conflict with leader
			// in range of entries [0, m.Index].
			idx := sort.Search(l.toSliceIndex(m.Index+1), func(i int) bool {
				return l.entries[i].Term == logTerm
			})
			conflictIndex := l.toEntryIndex(idx)
			r.sendAppendResponse(m.From, true, logTerm, conflictIndex)
			return
		}
	}

	// Append leader's new log entries.
	for i, entry := range m.Entries {
		if entry.Index < l.FirstIndex() {
			continue
		}
		if entry.Index <= l.LastIndex() {
			logTerm, err := l.Term(entry.Index)
			if err != nil {
				log.Panic("Get log term", err.Error())
			}
			// 5.3 If an existing entry conflicts with a new one
			// (same index but different terms),
			// update the existing entry and delete all entries
			// that follow it.
			if logTerm != entry.Term {
				idx := l.toSliceIndex(entry.Index)
				l.entries[idx] = *entry
				l.entries = l.entries[:idx+1]
				l.updateStable(min(l.stabled, entry.Index-1))
			}
		} else {
			// 5.4 Append any new entries not already in the log.
			n := len(m.Entries)
			for j := i; j < n; j++ {
				l.entries = append(l.entries, *m.Entries[j])
			}
			break
		}
	}
	// 5.5 If tryCommit > commitIndex,
	// set commitIndex = min(tryCommit, lastIndex).
	if m.Commit > l.committed {
		l.updateCommit(min(m.Commit, m.Index+uint64(len(m.Entries))))
	}
	r.sendAppendResponse(m.From, false, None, l.LastIndex())
}

// handleAppendEntriesResponse handles AppendEntries RPC response.
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	// Handle the conflict term in the prev log entry.
	if m.Reject {
		conflictIndex := m.Index
		if conflictIndex == None {
			return
		}
		if m.LogTerm != None {
			logTerm := m.LogTerm
			l := r.RaftLog
			idx := sort.Search(len(l.entries), func(i int) bool {
				return l.entries[i].Term > logTerm
			})
			if idx > 0 && l.entries[idx-1].Term == logTerm {
				conflictIndex = l.toEntryIndex(idx)
			}
		}
		r.Prs[m.From].Next = conflictIndex
		// Send AppendEntriesRPC RPC immediately.
		r.sendAppend(m.From)
		return
	}

	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.tryCommit()
		if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
			r.sendTimeout(m.From)
			r.leadTransferee = None
		}
	}
}

// tryCommit advances commit index if necessary and broadcasts
// AppendEntries RPC to notify followers to advance commit index.
func (r *Raft) tryCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	toCommit := match[(len(r.Prs)-1)/2]
	if toCommit > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(toCommit)
		if err != nil {
			log.Panic("Get log term", err.Error())
		}
		if logTerm == r.Term {
			r.RaftLog.updateCommit(toCommit)
			log.Infof("%s advance commit index to: %d", r, toCommit)
			r.broadcastAppendEntries()
		}
	}
}

// handleRequestVote handles RequestVote RPC request.
func (r *Raft) handleRequestVote(m pb.Message) {
	log.Debugf("%s receive RVA: {from=%d, term=%d, logTerm=%d, index=%d}",
		r, m.From, m.Term, m.LogTerm, m.Index)
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	if lastLogTerm > m.LogTerm || lastLogTerm == m.LogTerm && lastIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.resetElectionTimeout()
	r.sendRequestVoteResponse(m.From, false)
}

// handleRequestVoteResponse handles RequestVote RPC response.
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	log.Debugf("%s receive RVR: {from=%v, term=%d, vote=%v}", r, m.From, m.Term, !m.Reject)
	if m.Term != None && m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	grant := 0
	votes := len(r.votes)
	quorum := len(r.Prs) / 2
	for _, vote := range r.votes {
		if vote {
			grant++
		}
	}
	if grant > quorum {
		log.Debugf("%s receive quorum vote: %d, become leader at term: %d", r, grant, r.Term)
		r.becomeLeader()
	} else if votes-grant > quorum {
		r.becomeFollower(r.Term, None)
	}
}

// handleHeartbeat handles HeartBeat RPC request.
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	log.Debugf("%s receive HBA: {from=%v, term=%d}", r, m.From, m.Term)
	if m.Term != None && m.Term < r.Term {
		r.sendHeartBeatResponse(m.From, true)
	}
	r.Lead = m.From
	r.resetElectionTimeout()
	r.sendHeartBeatResponse(m.From, false)
}

// handleSnapshot handles InstallSnapshot RPC request.
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	log.Debugf("%s receive ISA: {from: %d, meta(term=%d, index=%d)}", r, m.From, meta.Term, meta.Index)
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false, None, r.RaftLog.committed)
		return
	}
	r.becomeFollower(max(r.Term, m.Term), m.From)
	first := meta.Index + 1
	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	// The update order is important.
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.updateOffset(first)
	r.RaftLog.updateStable(meta.Index)
	r.RaftLog.updateCommit(meta.Index)
	r.RaftLog.updateApplied(meta.Index)
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

// addNode adds a new node to raft group.
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = None

	if _, exist := r.Prs[id]; exist {
		log.Warnf("Add an existing node, id: %d", id)
		return
	}

	r.Prs[id] = &Progress{
		Next:  1,
		Match: 0,
	}

}

// removeNode removes a node from raft group.
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, exist := r.Prs[id]; !exist {
		log.Warnf("Delete an non-existing node, id: %d", id)
		return
	}

	delete(r.Prs, id)
	r.PendingConfIndex = None

	// enable the
	if r.State == StateLeader {
		r.tryCommit()
	}
}

// handleTransferLeader handles TransferLeader RPC request
func (r *Raft) handleTransferLeader(m pb.Message) {

	if r.State != StateLeader || m.From == r.id || (r.leadTransferee != None && m.From == r.leadTransferee) {
		return
	}

	log.Infof("Receive leader transfer from %d to %d", r.id, m.From)

	// peer node from other group
	if _, exist := r.Prs[m.From]; !exist {
		return
	}

	log.Infof("Start leader transfer from %d to %d", r.id, m.From)

	r.leadTransferee = m.From
	r.transferElapsed = 0

	// check the index
	if r.Prs[m.From].Next == r.Prs[r.id].Next {
		r.sendTimeout(m.From)
	} else {
		r.sendAppend(m.From)
	}

}
