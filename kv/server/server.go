package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}

	// check the region
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	defer reader.Close()

	// check the lock
	tx := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := tx.GetLock(req.GetKey())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}

	if lock != nil && lock.IsLockedFor(req.GetKey(), req.Version, resp) {
		return resp, nil
	}

	value, err := tx.GetValue(req.GetKey())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}

	resp.Value = value
	if value == nil {
		resp.NotFound = true
	}

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).

	resp := &kvrpcpb.PrewriteResponse{}

	// check the region
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	defer reader.Close()

	tx := mvcc.NewMvccTxn(reader, req.StartVersion)

	// 这里需要先获取所有的锁
	for _, mt := range req.Mutations {

		// 如果有 key 正在被使用，则无法完成全部加锁
		write, ts, err := tx.MostRecentWrite(mt.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if write != nil && ts >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mt.Key,
					Primary:    req.PrimaryLock,
				}})
			// 必须要知道目前有多少键被占用，方便重试
			continue
		}

		// 尝试获取锁
		lock, err := tx.GetLock(mt.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if lock != nil && lock.Ts <= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mt.Key)})
			continue
		}
		// 写入事务中

		lk := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}

		switch mt.Op {
		case kvrpcpb.Op_Put:
			tx.PutValue(mt.Key, mt.Value)
			lk.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			tx.DeleteValue(mt.Key)
			lk.Kind = mvcc.WriteKindDelete
		default:
			return nil, nil
		}

		tx.PutLock(mt.Key, lk)
	}

	// 如果获取锁失败，不写入
	if len(resp.Errors) > 0 {
		return resp, nil
	}

	// 提交事务并且返回
	if err := server.storage.Write(req.Context, tx.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).

	// check status
	resp := &kvrpcpb.CommitResponse{}

	// check the region
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	defer reader.Close()

	// commit using the same version as prewrite
	tx := mvcc.NewMvccTxn(reader, req.StartVersion)

	// latch
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// check is tx committed or rollback by checking key lock
	for _, key := range req.Keys {
		lock, err := tx.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
			}
			return resp, err
		}

		// fail because of lock and discard tx
		if lock != nil && lock.Ts != req.StartVersion { //&& lock.Ts+lock.Ttl >= req.CommitVersion {

			resp.Error = &kvrpcpb.KeyError{
				Locked:    lock.Info(key),
				Retryable: "true",
			}

			return resp, nil
		}

		if lock == nil {
			// check if write already exists
			write, ts, err := tx.CurrentWrite(key)
			if err != nil {
				if regionErr, ok := err.(*raft_storage.RegionError); ok {
					resp.RegionError = regionErr.RequestErr
				}
				return resp, err
			}

			if write == nil {
				continue
			}

			// tx is already rollback
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "false",
				}
				return resp, nil
			}
			_ = ts
			continue
		}

		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		}

		// add write op
		tx.PutWrite(key, req.CommitVersion, write)
		tx.DeleteLock(key)
	}

	// write to db
	if err := server.storage.Write(req.Context, tx.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).

	// readonly tx, all read op should be with the same tp
	resp := &kvrpcpb.ScanResponse{}
	// check the region
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	defer reader.Close()

	tx := mvcc.NewMvccTxn(reader, req.Version)
	iter := mvcc.NewScanner(req.StartKey, tx)
	defer iter.Close()

	pairs := make([]*kvrpcpb.KvPair, 0, req.Limit)

	for i := uint32(0); i < req.Limit; {

		key, value, err := iter.Next()
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		// finished
		if key == nil {
			break
		}

		// check lock
		lock, err := tx.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if lock != nil && req.Version >= lock.Ts {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					}},
				Key: key,
			})
			i++
			continue
		}
		// the key is deleted
		if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
			i++
		}
	}

	// 只有成功才能返回
	resp.Pairs = pairs

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).

	// check status
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	// check the region
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	defer reader.Close()

	tx := mvcc.NewMvccTxn(reader, req.LockTs)

	lock, err := tx.GetLock(req.PrimaryKey)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, err
	}
	if lock != nil {

		// if the lock is not expired
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl > mvcc.PhysicalTime(req.CurrentTs) {
			resp.LockTtl = lock.Ttl
			return resp, nil
		} else {

			// lock is expired, rollback
			tx.DeleteValue(req.PrimaryKey)
			tx.DeleteLock(req.PrimaryKey)
			tx.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})

			if err := server.storage.Write(req.Context, tx.Writes()); err != nil {
				if regionErr, ok := err.(*raft_storage.RegionError); ok {
					resp.RegionError = regionErr.RequestErr
					return resp, nil
				}
				return nil, err
			}

			resp.Action = kvrpcpb.Action_TTLExpireRollback
			return resp, nil
		}
	}

	write, ts, err := tx.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if write == nil {

		// if write not exists, rollback
		tx.DeleteValue(req.PrimaryKey)
		tx.DeleteLock(req.PrimaryKey)
		tx.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})

		if err := server.storage.Write(req.Context, tx.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback

		return resp, nil
	}

	// already rollback, no action
	if write.Kind == mvcc.WriteKindRollback {
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	// the action is committed
	resp.CommitVersion = ts

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).

	// check status
	resp := &kvrpcpb.BatchRollbackResponse{}
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// latch
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		// check write
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				txn.DeleteValue(key)
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
		}

		// check lock
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})

		if lock == nil || lock.Ts != req.StartVersion {
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

// KvResolveLock is used to commit a transaction (CommitVersion != 0) or rollback a transaction (CommitVersion == 0).
// After KvCheckTxnStatus is already called.
func (server *Server) KvResolveLock(c context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).

	// check status
	resp := &kvrpcpb.ResolveLockResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	tx := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Get all locks of this transaction
	pairs, err := mvcc.AllLocksForTxn(tx)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	// no actions to do
	if len(pairs) == 0 {
		return resp, nil
	}

	keys := make([][]byte, 0, len(pairs))
	for _, pair := range pairs {
		keys = append(keys, pair.Key)
	}

	if req.CommitVersion == 0 {
		// rollback
		rresp, err := server.KvBatchRollback(c, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		resp.Error = rresp.Error
		resp.RegionError = rresp.RegionError
		return resp, nil
	}

	// commit
	cresp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
		Context:       req.Context,
		StartVersion:  req.StartVersion,
		Keys:          keys,
		CommitVersion: req.CommitVersion,
	})
	resp.Error = cresp.Error
	resp.RegionError = cresp.RegionError
	return resp, err

}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
