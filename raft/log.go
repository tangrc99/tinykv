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

	// Your Data Here (2A).
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	raftLog := &RaftLog{
		storage:   storage,
		committed: 0,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
		offset:    firstIndex,
	}
	return raftLog
}

// updateOffset advances first index.0
func (l *RaftLog) updateOffset(firstIndex uint64) {
	if firstIndex < l.offset {
		log.Panicf("updateOffset: newFirstIndex(%d) < offset(%d)", firstIndex, l.offset)
	}
	l.offset = firstIndex
}

// updateStable advances stable index.
func (l *RaftLog) updateStable(stableIndex uint64) {
	lastIndex := l.LastIndex()
	if stableIndex > lastIndex {
		log.Panicf("updateStable: stableIndex(%d) > lastIndex(%d)", stableIndex, lastIndex)

	}
	l.stabled = stableIndex
}

// updateCommit advances committed index.
func (l *RaftLog) updateCommit(commitIndex uint64) {
	lastIndex := l.LastIndex()
	if commitIndex < l.committed || commitIndex > lastIndex {
		log.Panicf("updateCommit: commitIndex(%d) is out of range [committed(%d), lastIndex(%d)]",
			commitIndex, l.committed, lastIndex)
	}
	l.committed = commitIndex
}

// updateApplied advances applied index.
func (l *RaftLog) updateApplied(applyIndex uint64) {
	if applyIndex == 0 {
		return
	}
	if applyIndex < l.applied || applyIndex > l.committed {
		log.Panicf("updateApplied: applyIndex(%d) is out of range [applied(%d), committed(%d)]",
			applyIndex, l.applied, l.committed)
	}
	l.applied = applyIndex
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries.
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndex, _ := l.storage.FirstIndex()
	// If older entries in storage module has been compacted into snapshot,
	// then `storage.FirstIndex()` must be bigger than `l.FirstIndex`.
	if firstIndex > l.offset {
		if len(l.entries) > 0 {
			// Delete entries that have been compressed into snapshots but still exist in memory.
			entries := l.entries[l.toSliceIndex(firstIndex):]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.offset = firstIndex
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries.
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.offset+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries.
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.offset+1 : l.committed-l.offset+1]
	}
	return nil
}

// FirstIndex returns the first index of RaftLog.entries.
func (l *RaftLog) FirstIndex() uint64 {
	return l.offset
}

// LastIndex return the last index of RaftLog.entries.
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// Get last index in memory.
	var lastIndexInMemory uint64
	if len(l.entries) > 0 {
		lastIndexInMemory = l.entries[len(l.entries)-1].Index
	} else {
		lastIndexInMemory, _ = l.storage.LastIndex()
	}
	// Get last index in snapshot.
	var lastIndexInSnapshot uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		lastIndexInSnapshot = l.pendingSnapshot.Metadata.Index
	}
	return max(lastIndexInMemory, lastIndexInSnapshot)
}

// Term return the term of the entry in the given index.
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// If the entry in the given index is in memory.
	if len(l.entries) > 0 && i >= l.offset {
		return l.entries[i-l.offset].Term, nil
	}
	// If the entry in the given index is in snapshot.
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

// toSliceIndex return the log offset in corresponding to the log index.
func (l *RaftLog) toSliceIndex(logIndex uint64) int {
	idx := int(logIndex - l.offset)
	if idx < 0 {
		log.Panicf("toSliceIndex: logIndex(%d) < offset(%d)", logIndex, l.offset)
	}
	return idx
}

// toEntryIndex returns the index of log entry corresponding to the offset.
func (l *RaftLog) toEntryIndex(offset int) uint64 {
	return uint64(offset) + l.offset
}
