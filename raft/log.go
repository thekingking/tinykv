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
	// "github.com/pingcap-incubator/tinykv/log"
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
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
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
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return l.entries
	}
	firstIndex := l.entries[0].Index
	offset := l.stabled - firstIndex + 1
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return l.entries
	}
	firstIndex := l.entries[0].Index
	lo := l.applied + 1 - firstIndex
	hi := l.committed + 1 - firstIndex
	return l.entries[lo:hi]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		lastIndex, _ := l.storage.LastIndex()
		return lastIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) == 0 || i < l.entries[0].Index || i > l.LastIndex() {
		return 0, nil
	}
	return l.entries[i-l.entries[0].Index].Term, nil
}

func (l *RaftLog) Append(preIndex, preTerm uint64, entries []*pb.Entry) bool {
	if term, _ := l.Term(preIndex); term != preTerm {
		return false
	}
	if len(entries) == 0 {
		return true
	}
	if len(l.entries) == 0 || preIndex+1 < l.entries[0].Index {
		l.entries = make([]pb.Entry, 0)
		l.stabled = preIndex
	} else {
		offset := int(preIndex - l.entries[0].Index + 1)
		old_entries := l.entries[offset:]
		l.entries = l.entries[:offset]
		for len(entries) > 0 && len(old_entries) > 0 {
			if entries[0].Term != old_entries[0].Term {
				break
			}
			l.entries = append(l.entries, old_entries[0])
			old_entries = old_entries[1:]
			entries = entries[1:]
		}
		if len(entries) == 0 {
			l.entries = append(l.entries, old_entries...)
		}
		if len(l.entries) == 0 {
			l.stabled = preIndex
		} else {
			l.stabled = min(l.stabled, l.entries[len(l.entries)-1].Index)
		}
	}
	for len(entries) > 0 {
		l.entries = append(l.entries, *entries[0])
		entries = entries[1:]
	}
	return true
}

func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	ents := make([]*pb.Entry, hi-lo)
	firstIndex := l.entries[0].Index
	for i := lo; i < hi; i++ {
		ents[i-lo] = &l.entries[i-firstIndex]
	}
	return ents, nil
}
