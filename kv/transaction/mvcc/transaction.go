package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfWrite,
			Key:   EncodeKey(key, ts),
			Value: write.ToBytes(),
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	lockBytes, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil || lockBytes == nil {
		return nil, err
	}
	return ParseLock(lockBytes)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfLock,
			Key:   key,
			Value: lock.ToBytes(),
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfLock,
			Key: key,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	// Seek to the first key >= EncodeKey(key, txn.StartTS)
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key, txn.StartTS))
	if !iter.Valid() {
		return nil, nil
	}

	// Check if the key is the one we are looking for
	item := iter.Item()
	keyBytes := item.Key()
	userKey := DecodeUserKey(keyBytes)
	if !bytes.Equal(key, userKey) {
		return nil, nil
	}

	// Check if the write is valid for this transaction
	writeBytes, err := item.Value()
	if err != nil {
		return nil, err
	}
	write, err := ParseWrite(writeBytes)
	if err != nil || write == nil || write.Kind != WriteKindPut {
		return nil, err
	}

	// search the value in the default CF
	startTs := write.StartTS
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, startTs))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfDefault,
			Key:   EncodeKey(key, txn.StartTS),
			Value: value,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Cf:  engine_util.CfDefault,
			Key: EncodeKey(key, txn.StartTS),
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	// get the iterator for the write CF
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for iter.Seek(EncodeKey(key, TsMax)); iter.Valid(); iter.Next() {
		item := iter.Item()
		keyBytes := item.Key()
		userKey := DecodeUserKey(keyBytes)
		commitTs := decodeTimestamp(keyBytes)
		if !bytes.Equal(key, userKey) || commitTs < txn.StartTS {
			break
		}

		writeBytes, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(writeBytes)
		if err != nil || write == nil {
			return nil, 0, err
		}

		if write.StartTS == txn.StartTS {
			return write, commitTs, nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	// get the iterator for the write CF
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	// Seek the most recent write with the given key
	iter.Seek(EncodeKey(key, TsMax))
	if !iter.Valid() {
		return nil, 0, nil
	}

	// Check if the key is the one we are looking for
	item := iter.Item()
	keyBytes := item.Key()
	userKey := DecodeUserKey(keyBytes)
	if !bytes.Equal(key, userKey) {
		return nil, 0, nil
	}

	// Check if the write is valid for this transaction
	writeBytes, err := item.Value()
	if err != nil {
		return nil, 0, err
	}
	write, err := ParseWrite(writeBytes)
	if err != nil || write == nil {
		return nil, 0, err
	}

	return write, decodeTimestamp(keyBytes), nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
