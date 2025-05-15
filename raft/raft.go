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

	// logd "github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

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

	active map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// 初始化RaffLog
	hardState, confState, _ := c.Storage.InitialState()
	log := newLog(c.Storage)
	log.committed = max(log.committed, hardState.Commit)
	log.applied = max(log.applied, c.Applied)

	// 初始化Prs
	var prs map[uint64]*Progress
	if c.peers != nil {
		prs = make(map[uint64]*Progress, len(c.peers))
		for _, id := range c.peers {
			prs[id] = &Progress{}
		}
	} else {
		prs = make(map[uint64]*Progress, len(confState.Nodes))
		for _, id := range confState.Nodes {
			prs[id] = &Progress{}
		}
	}

	return &Raft{
		id:               c.ID,
		State:            StateFollower,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          log,
		Prs:              prs,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  randElectionTimeout(c.ElectionTick, 2*c.ElectionTick),
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
}

/*===================================== Start：状态判定 ====================================*/
func (r *Raft) getSoftState() SoftState {
	return SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

/*===================================== End：状态判定 ====================================*/

/*===================================== Start：发起动作 ====================================*/
func (r *Raft) startElection() {
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

func (r *Raft) startBeat() {
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgBeat,
	})
}

func (r *Raft) startPropose() {
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
}

func (r *Raft) startTimeoutNow() {
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
	})
}

/*===================================== End：发起动作 ====================================*/

/*===================================== Start：发送消息 ====================================*/
// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	if r.State != StateLeader {
		return false
	}
	index := r.Prs[to].Next - 1
	term, err := r.RaftLog.Term(index)

	if err == ErrCompacted {
		r.sendSnapshot(to)
	} else {
		entries, _ := r.RaftLog.Entries(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			Term:    r.Term,
			From:    r.id,
			To:      to,
			Commit:  r.RaftLog.committed,
			Index:   index,
			LogTerm: term,
			Entries: entries,
		})
	}
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	if r.RaftLog.pendingSnapshot == nil {
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return
		}
		r.RaftLog.pendingSnapshot = &snapshot
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Term:     r.Term,
		From:     r.id,
		To:       to,
		Index:    r.RaftLog.LastIndex(),
		Snapshot: r.RaftLog.pendingSnapshot,
	})
}

func (r *Raft) sendAllAppend() {
	r.heartbeatElapsed = 0
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	r.heartbeatElapsed = 0
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			From:    r.id,
			To:      id,
		})
	}
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		From:    r.id,
		To:      to,
	})
}

// send request vote to all peers
func (r *Raft) sendRequestVote() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
			LogTerm: lastTerm,
			Index:   lastIndex,
			From:    r.id,
			To:      id,
		})
	}
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Reject:  reject,
	})
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
		Commit:  r.RaftLog.committed,
	})
}

/*===================================== End：发送消息 ====================================*/

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.electionElapsed++
	r.RaftLog.maybeCompact()
	// logd.Infof("r.id: %d, r.Term: %d, r.Lead: %d, r.election: %d, tick: %d, r.heartbeat: %d, r.heartelpased: %d, FirstIndex: %d, LastIndex: %d, Commited: %d, Applied: %d", r.id, r.Term, r.Lead, r.electionTimeout, r.electionElapsed, r.heartbeatTimeout, r.heartbeatElapsed, r.RaftLog.FirstIndex(), r.RaftLog.LastIndex(), r.RaftLog.committed, r.RaftLog.applied)
	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout {
			r.startTimeoutNow()
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			r.startTimeoutNow()
		}
	case StateLeader:
		// 若有一半以上的节点返回了心跳响应，则可以认为该节点继续当选Leader
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			activeNum := len(r.active)
			// 自动退选
			if activeNum*2 <= len(r.Prs) {
				r.becomeFollower(r.Term, None)
				r.RaftLog.pendingSnapshot = nil
				return
			}
			r.active = make(map[uint64]bool)
			r.active[r.id] = true
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.startBeat()
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = lead
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term += 1
	r.Vote = r.id
	r.Lead = None
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	// 向自己投票
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

	// 只有一个节点，自动当选
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.active = make(map[uint64]bool)
	r.active[r.id] = true

	// 初始化所有节点的进度
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		} else {
			r.Prs[id].Match = 0
		}
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
	}

	r.startPropose()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// logd.Infof("r.id: %d, r.Term: %d, r.Lead: %d, r.tick: %d, Step %s, FirstIndex: %d, LastIndex: %d, Commited: %d, Stabled: %d Applied: %d", r.id, r.Term, r.Lead, r.electionElapsed, m.MsgType, r.RaftLog.FirstIndex(), r.RaftLog.LastIndex(), r.RaftLog.committed, r.RaftLog.stabled, r.RaftLog.applied)
	// logd.Infof("Step %s, From: %d, To: %d, Term: %d, Index: %d, LogTerm: %d, Commit: %d", m.MsgType, m.From, m.To, m.Term, m.Index, m.LogTerm, m.Commit)
	switch r.State {
	case StateFollower:
		r.FollowerStep(m)
	case StateCandidate:
		r.CandidateStep(m)
	case StateLeader:
		r.LeaderStep(m)
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.sendRequestVote()
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
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return nil
}

func (r *Raft) CandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.sendRequestVote()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return nil
}

func (r *Raft) LeaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.sendHeartbeat()
	case pb.MessageType_MsgPropose:
		return r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term || r.State == StateLeader && m.Term == r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	if r.State == StateLeader {
		r.RaftLog.pendingSnapshot = nil
	}
	r.becomeFollower(m.Term, m.From)
	reject := !r.RaftLog.Append(m.Index, m.LogTerm, m.Entries)
	if !reject {
		if len(m.Entries) > 0 {
			r.RaftLog.committed = max(r.RaftLog.committed, min(m.Commit, m.Entries[len(m.Entries)-1].Index))
		} else {
			r.RaftLog.committed = max(r.RaftLog.committed, min(m.Commit, m.Index))
		}
	}
	r.sendAppendResponse(m.From, reject)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	// 存在更高的term，转为follower，比如有节点被隔离太久，term较大，这时候需要重新选举来同步term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.RaftLog.pendingSnapshot = nil
		return
	}
	r.active[m.From] = true
	if m.Reject {
		r.Prs[m.From].Next = m.Commit + 1
		r.Prs[m.From].Match = m.Commit
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = r.Prs[m.From].Next - 1
		// 差分数组求commit的Index位置
		if term, _ := r.RaftLog.Term(m.Index); term == r.Term && m.Index > r.RaftLog.committed {
			arr := make([]uint64, r.Prs[r.id].Next-r.RaftLog.committed)
			for _, p := range r.Prs {
				if p.Match >= r.RaftLog.committed {
					arr[p.Match-r.RaftLog.committed]++
				}
			}
			for i, n := len(arr)-1, 0; i > 0; i-- {
				n += int(arr[i])
				if n > len(r.Prs)/2 {
					// 找到第一个大于一半的index，更新commit，发送Append
					r.RaftLog.committed = r.RaftLog.committed + uint64(i)
					r.sendAllAppend()
					break
				}
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term || r.State == StateLeader && m.Term == r.Term {
		r.sendHeartbeatResponse(m.From)
		return
	}
	if r.State == StateLeader {
		r.RaftLog.pendingSnapshot = nil
	}
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	// 存在更高的term，转为follower，比如有节点被隔离太久，term较大，这时候需要重新选举来同步term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.RaftLog.pendingSnapshot = nil
		return
	}
	r.active[m.From] = true
	if r.RaftLog.committed > m.Commit {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.RaftLog.pendingSnapshot = nil
	}
	reject := true
	if m.Term < r.Term || r.State != StateFollower && m.Term == r.Term {
		reject = true
	} else if r.Vote == m.From {
		reject = false
	} else if r.Vote == None {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		if m.LogTerm > lastTerm || m.LogTerm == lastTerm && m.Index >= lastIndex {
			reject = false
			r.Vote = m.From
		}
	}

	r.sendRequestVoteResponse(m.From, reject)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	// 存在更高的term，转为follower，比如有任期更高的candidate或leader
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.RaftLog.pendingSnapshot = nil
		return
	}
	if _, exists := r.votes[m.From]; exists {
		return
	}
	r.votes[m.From] = !m.Reject
	// 获得过半同意便当选，获得过半拒绝便退选
	if len(r.votes)*2 <= len(r.Prs) {
		return
	}
	argNum := 0
	denNum := 0
	for _, v := range r.votes {
		if v {
			argNum++
		} else {
			denNum++
		}
	}
	if argNum*2 > len(r.Prs) {
		r.becomeLeader()
	}
	if denNum*2 > len(r.Prs) {
		r.becomeFollower(m.Term, None)
	}
}

func (r *Raft) handlePropose(m pb.Message) error {
	// 如果正在迁移，直接返回错误
	if r.leadTransferee != None {
		return ErrProposalDropped
	}

	// 补全entry信息，构建新的entry
	NextIndex := r.Prs[r.id].Next
	for i, entry := range m.Entries {
		entry.Index = NextIndex + uint64(i)
		entry.Term = r.Term
		entry.EntryType = pb.EntryType_EntryNormal
	}

	// 将新的entry写入raft log
	lastIndex := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(lastIndex)
	r.RaftLog.Append(lastIndex, term, m.Entries)
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}

	// 将新的entry同步到其他节点
	r.sendAllAppend()
	return nil
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	if m.Term < r.Term || r.State == StateLeader && m.Term == r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	// log.Infof("handleSnapshot, r.Id: %d, r.Term: %d, m.Term: %d, m.From: %d, m.Snapshot.Metadata.Index: %d, r.RaftLog.committed: %d",r.id, r.Term, m.Term, m.From, m.Snapshot.Metadata.Index, r.RaftLog.committed)
	if r.State == StateLeader {
		r.RaftLog.pendingSnapshot = nil
	}
	r.becomeFollower(m.Term, m.From)
	reject := false
	if m.Snapshot == nil || m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		reject = true
	} else {
		if m.Snapshot.Metadata.ConfState != nil {
			r.Prs = make(map[uint64]*Progress, len(m.Snapshot.Metadata.ConfState.Nodes))
			for _, id := range m.Snapshot.Metadata.ConfState.Nodes {
				r.Prs[id] = &Progress{}
			}
		}
		r.RaftLog.ApplySnapshot(m.Snapshot)
		if r.RaftLog.LastIndex() < m.Index {
			reject = true
		}
	}
	r.sendAppendResponse(m.From, reject)
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	r.electionTimeout = randElectionTimeout(10, 20)
	r.startElection()
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
