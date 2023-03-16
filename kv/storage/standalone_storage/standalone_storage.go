package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db  *badger.DB
	cfg *config.Config
	// Your Data Here (1).
}

type StandAloneReader struct {
	db  *badger.DB
	txn *badger.Txn
	storage.StorageReader
}

func (r StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {

	return engine_util.GetCF(r.db, cf, key)
}
func (r StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
func (r StandAloneReader) Close() {
	_ = r.txn.Commit()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		db:  engine_util.CreateDB(conf.DBPath, false),
		cfg: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	r := StandAloneReader{
		db:  s.db,
		txn: s.db.NewTransaction(false),
	}

	// Your Code Here (1).
	return r, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.db.NewTransaction(true)
	for _, modify := range batch {
		kcf := engine_util.KeyWithCF(modify.Cf(), modify.Key())

		err := txn.Set(kcf, modify.Value())
		if err != nil {
			txn.Discard()
			return err
		}
	}
	err := txn.Commit()
	if err != nil {
		println(err.Error())
	}
	return err
}
