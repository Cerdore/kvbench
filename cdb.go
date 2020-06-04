package kvbench

import (
	"os"
	"sync"

	"github.com/cerdore/cdb"
)

type cdbStore struct {
	mu    sync.RWMutex
	db    *cdb.DB
	path  string
	fsync bool
}

func NewcdbStore(path string, fsync bool) (Store, error) {
	if path == ":memory:" {
		return nil, errMemoryNotAllowed
	}
	//	db, err := kv.Create(path, &kv.Options{})
	db, err := cdb.New("chen", cdb.DBOpts{DataDir: path, MtSizeLimit: 0})

	if err != nil {
		return nil, err
	}

	return &cdbStore{
		db:    db,
		path:  path,
		fsync: fsync,
	}, nil
}

func (s *cdbStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Close()
	return nil
}
func (s *cdbStore) PSet(keys, values [][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// if err := s.db.BeginTransaction(); err != nil {
	// 	return err
	// }
	// defer s.db.Rollback()
	for i := range keys {
		if err := s.db.Put(keys[i], values[i], false); err != nil {
			return err
		}
	}
	return nil
}

func (s *cdbStore) PGet(keys [][]byte) ([][]byte, []bool, error) {
	var values [][]byte
	var oks []bool
	for i := range keys {
		value, ok, err := s.Get(keys[i])
		if err != nil {
			return nil, nil, err
		}
		values = append(values, value)
		oks = append(oks, ok)
	}
	return values, oks, nil
}

func (s *cdbStore) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Put(key, value, false)
}

func (s *cdbStore) Get(key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, err := s.db.Get(key)
	if err != nil {
		return nil, false, err
	}
	return v, v != nil, nil
}

func (s *cdbStore) Del(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.db.Get(key)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	err = s.db.Delete(key, false)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *cdbStore) Keys(pattern []byte, limit int, withvalues bool) ([][]byte, [][]byte, error) {
	return nil, nil, nil
	/*
		s.mu.RLock()
		defer s.mu.RUnlock()
		spattern := string(pattern)
		min, max := match.Allowable(spattern)
		bmin := []byte(min)
		var keys [][]byte
		var vals [][]byte
		useMax := !(len(spattern) > 0 && spattern[0] == '*')
		s.db.BeginTransaction()
		iter := s.db.NewIterator(nil, nil)
		for ok := iter.Seek(bmin); ok; ok = iter.Next() {
			if limit > -1 && len(keys) >= limit {
				break
			}
			key := iter.Key()
			value := iter.Value()
			skey := string(key)
			if useMax && skey >= max {
				break
			}
			if match.Match(skey, spattern) {
				keys = append(keys, []byte(skey))
				if withvalues {
					vals = append(vals, bcopy(value))
				}
			}
		}
		iter.Release()
		err := iter.Error()
		if err != nil {
			return nil, nil, err
		}
		return keys, vals, nil
	*/
}

func (s *cdbStore) FlushDB() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Close()
	os.RemoveAll(s.path)
	s.db = nil
	db, err := cdb.New(s.path, cdb.DBOpts{DataDir: "", MtSizeLimit: 0}) //kv.Create(s.path, &kv.Options{})
	if err != nil {
		return err
	}
	s.db = db
	return nil
}
