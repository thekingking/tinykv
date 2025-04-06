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

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, _ := storage.InitialState()
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	storageEntries, _ := storage.Entries(firstIndex, lastIndex + 1)
	entries := make([]pb.Entry, 1)
	entries[0] = pb.Entry{}
	entries = append(entries, storageEntries...)
	return &RaftLog {
		storage: storage,
		applied: 0,
		committed: hardState.Commit,
		stabled: lastIndex,
		entries: entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries[l.stabled + 1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.entries[l.applied + 1 : l.committed + 1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}
	return 0, nil
}

func (l *RaftLog) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	offset := first - l.entries[0].Index
	l.storage.(*MemoryStorage).Append(l.unstableEntries())
	l.stabled = offset - 1
	l.entries = append(l.entries[:offset], entries...)

	return nil
}

func (l *RaftLog) Stable(commit uint64) {
	
}

func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	ents := make([]*pb.Entry, hi - lo)
	firstIndex := l.entries[0].Index
	for i := lo; i < hi; i++ {
		ents[i - lo] = &l.entries[i - firstIndex]
	}
	return ents, nil
}

