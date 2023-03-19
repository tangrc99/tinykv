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
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).

	// it is much easier to traverse write cf
	it := txn.Reader.IterCF(engine_util.CfWrite)

	// the key is ordered by timestamp desc, so the first key is available
	it.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		txn:  txn,
		iter: it,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	userKey := DecodeUserKey(scan.iter.Item().Key())
	scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))

	if !scan.iter.Valid() {
		return nil, nil, nil
	}

	// to ensure current key the same as original user key.
	if !bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), userKey) {
		// to find next key
		return scan.Next()
	}

	// check if this key deleted
	writeVal, err := scan.iter.Item().Value()
	if err != nil {
		return nil, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return nil, nil, err
	}

	// if deleted, return nil value
	if write.Kind == WriteKindDelete {
		key := scan.iter.Item().KeyCopy(nil)
		// move next
		for scan.iter.Valid() && bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), userKey) {
			scan.iter.Next()
		}

		return DecodeUserKey(key), nil, nil
	}

	// if not deleted, to find the value

	k := DecodeUserKey(scan.iter.Item().KeyCopy(nil))
	ts := write.StartTS

	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(k, ts))
	if err != nil {
		return nil, nil, err
	}

	for scan.iter.Valid() && bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), userKey) {
		scan.iter.Next()
	}

	return k, value, err
}
