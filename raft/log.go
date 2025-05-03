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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	entries := make([]pb.Entry, 1)
	firstIndex, _ := storage.FirstIndex()
	term, _ := storage.Term(firstIndex - 1)
	entries[0].Index = firstIndex - 1
	entries[0].Term = term
	lastIndex, _ := storage.LastIndex()
	ents, _ := storage.Entries(firstIndex, lastIndex+1)
	entries = append(entries, ents...)
	// log.Infof("newLog: firstIndex %d, lastIndex %d, len: %d", firstIndex, lastIndex, len(entries))
	return &RaftLog{
		storage:   storage,
		stabled:   lastIndex,
		entries:   entries,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if FirstIndex, _ := l.storage.FirstIndex(); l.FirstIndex() < FirstIndex {
		l.entries = l.entries[FirstIndex-l.entries[0].Index:]
		l.pendingSnapshot = nil
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	offset := l.entries[0].Index
	// if l.stabled + 1 - offset > uint64(len(l.entries)) {
	// 	log.Infof("unstableEntries: stabled %d, offset %d, len %d", l.stabled, offset, len(l.entries))
	// }
	return l.entries[l.stabled+1-offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return l.entries
	}
	offset := l.entries[0].Index
	lo := l.applied + 1 - offset
	hi := l.committed + 1 - offset
	return l.entries[lo:hi]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.entries[0].Index + 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) Append(preIndex, preTerm uint64, entries []*pb.Entry) bool {
	if term, err := l.Term(preIndex); err != nil || term != preTerm {
		return false
	}
	offset := l.entries[0].Index
	for len(entries) > 0 {
		firstIndex := entries[0].Index
		if firstIndex > l.LastIndex() || l.entries[firstIndex-offset].Term != entries[0].Term {
			break
		}
		entries = entries[1:]
	}
	if len(entries) == 0 {
		return true
	}
	l.entries = l.entries[:entries[0].Index-offset]
	l.stabled = min(l.stabled, l.LastIndex())
	for len(entries) > 0 {
		l.entries = append(l.entries, *entries[0])
		entries = entries[1:]
	}
	return true
}

func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	offset := l.entries[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, l.LastIndex())
	}
	if len(l.entries) == 1 && lo < hi {
		return nil, ErrUnavailable
	}

	ents := make([]*pb.Entry, hi-lo)
	for i := lo; i < hi; i++ {
		ents[i-lo] = &l.entries[i-offset]
	}
	return ents, nil
}

func (l *RaftLog) ApplySnapshot(snap *pb.Snapshot) {
	l.pendingSnapshot = snap

	l.entries = make([]pb.Entry, 1)
	l.entries[0].Index = snap.Metadata.Index
	l.entries[0].Term = snap.Metadata.Term

	l.applied = snap.Metadata.Index
	l.committed = snap.Metadata.Index
	l.stabled = snap.Metadata.Index

	// log.Infof("apply snapshot: index %d, term %d", snap.Metadata.Index, snap.Metadata.Term)
}
