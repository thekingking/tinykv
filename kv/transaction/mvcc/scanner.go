package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	// The iterator used to scan the keys.
	iter engine_util.DBIterator

	txn *MvccTxn

	key []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(startKey)
	return &Scanner{
		iter: iter,
		txn:  txn,
		key:  startKey,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	if scan.iter != nil {
		scan.iter.Close()
	}
	scan.iter = nil
	scan.txn = nil
	scan.key = nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.iter == nil {
		return nil, nil, nil
	}
	for ; scan.iter.Valid(); scan.iter.Next() {
		item := scan.iter.Item()
		keyBytes := item.Key()
		userKey := DecodeUserKey(keyBytes)
		if bytes.Equal(scan.key, userKey) {
			continue
		}
		scan.key = userKey
		value, err := scan.txn.GetValue(userKey)
		if err != nil {
			return nil, nil, err
		}
		if value == nil {
			continue
		}
		return userKey, value, nil
	}
	// If the scanner is exhausted, return nil, nil, nil.
	return nil, nil, nil
}
