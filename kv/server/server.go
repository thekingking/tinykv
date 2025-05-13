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
	reader, _ := server.storage.Reader(req.Context)
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// Check if the key is locked.
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= req.Version {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: lock.Info(req.Key),
			},
		}, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.GetResponse{
		Value: value,
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// create a new transaction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, mutation := range req.Mutations {
		// 检查是否有大于当前事务的write
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && ts >= req.StartVersion {
			return &kvrpcpb.PrewriteResponse{
				Errors: []*kvrpcpb.KeyError{
					{
						Conflict: &kvrpcpb.WriteConflict{
							StartTs:    req.StartVersion,
							ConflictTs: ts,
							Key:        mutation.Key,
							Primary:    req.PrimaryLock,
						},
					},
				},
			}, nil
		}

		// 检查是否有锁
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			return &kvrpcpb.PrewriteResponse{
				Errors: []*kvrpcpb.KeyError{
					{
						Locked: lock.Info(mutation.Key),
					},
				},
			}, nil
		}

		txn.PutValue(mutation.Key, mutation.Value)
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
	}
	server.storage.Write(req.Context, txn.Writes())

	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.CommitVersion)

	for _, key := range req.Keys {
		write, ts, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil && ts >= req.CommitVersion {
			if ts == req.CommitVersion {
				if write.Kind == mvcc.WriteKindRollback {
					return &kvrpcpb.CommitResponse{
						Error: &kvrpcpb.KeyError{
							Retryable: "retry",
						},
					}, nil
				}
				return &kvrpcpb.CommitResponse{}, nil
			}
			return &kvrpcpb.CommitResponse {
				Error: &kvrpcpb.KeyError{
					Abort: "abort",
				},
			}, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			return &kvrpcpb.CommitResponse{}, nil
		}

		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "retry",
				},
			}, nil
		}

		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	server.storage.Write(req.Context, txn.Writes())
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)

	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if err != nil {
			return resp, err
		}
		if key == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	// 创建事务
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// 检查是否已经commit了
	write, _, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if write != nil && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = write.StartTS
		return resp, nil
	}
	
	// 检查是否有锁，没有则回滚
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if lock == nil {
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:   mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}
	
	// 有则检查是否超时，超时则回滚
	curTs := mvcc.PhysicalTime(req.CurrentTs)
	lockTs := mvcc.PhysicalTime(lock.Ts)
	if curTs >= lockTs+lock.Ttl {
		txn.DeleteValue(req.PrimaryKey)
		txn.DeleteLock(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:   mvcc.WriteKindRollback,
		})
		server.storage.Write(req.Context, txn.Writes())
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}

	// Check if the request is valid
	if req == nil || len(req.Keys) == 0 {
		return resp, nil
	}

	// Create a new transaction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)


	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && write.StartTS == req.StartVersion {
			if write.Kind == mvcc.WriteKindRollback {
				return resp, nil
			}
			resp.Error = &kvrpcpb.KeyError{Abort: "abort"}
			return resp, nil
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
		}
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	server.storage.Write(req.Context, txn.Writes())
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	// Create a new transaction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	kls, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return resp, err
	}

	for _, kl := range kls {
		key := kl.Key
		lock := kl.Lock
		if req.CommitVersion == 0 {
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			txn.PutWrite(key, lock.Ts, &mvcc.Write{
				StartTS: lock.Ts,
				Kind:    mvcc.WriteKindRollback,
			})
		} else {
			txn.DeleteLock(key)
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
				StartTS: lock.Ts,
				Kind:    lock.Kind,
			})
		}
	}

	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return resp, err
	}
	return resp, nil
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
