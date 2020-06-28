package newtikv

import (
	"bytes"
	"context"
	//tiConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
)

const DBName = "newtikv"

type TiKV struct {
	client kv.Storage
	conf   *config.Config
}

type Driver struct {
}

func init() {
	store.RegisterDB(Driver{})
}

func (d Driver) Name() string {
	return DBName
}

func (d Driver) Open(conf *config.Config) (store.DB, error) {
	driver := tikv.Driver{}
	s, err := driver.Open(conf.Store.Path)
	if err != nil {
		return nil, err
	}

	//if conf.Store.GCEnable {
	//	if raw, ok := s.(tikv.EtcdBackend); ok {
	//		tikv.NewGCHandlerFunc =
	//		err = raw.StartGCWorker()
	//		if err != nil {
	//			return nil, err
	//		}
	//	}
	//}

	//https://github.com/pingcap/tidb/pull/12095
	//gConfig := tiConfig.GetGlobalConfig()

	return &TiKV{
		client: s,
		conf:   conf,
	}, nil
}

func (t *TiKV) Close() error {
	return t.client.Close()
}

func (t *TiKV) Get(key []byte, option store.Option) ([]byte, error) {
	tx, err := t.client.Begin()
	if err != nil {
		return nil, xerror.ErrGetTimestampFailed
	}

	if option.ReplicaRead {
		snapshot := tx.GetSnapshot()
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.ReadTimeout.Duration)
	defer cancel()
	v, err := tx.Get(ctx, key)
	if kv.IsErrNotFound(err) {
		return nil, xerror.ErrNotExists
	}
	if err != nil {
		return nil, xerror.ErrGetKVFailed
	}
	return v, nil
}

func (t *TiKV) List(start, end []byte, limit int, option store.Option) ([]store.KeyEntry, error) {
	tx, err := t.client.Begin()
	if err != nil {
		return nil, xerror.ErrGetTimestampFailed
	}

	it, err := tx.Iter(kv.Key(start), kv.Key(end))
	if err != nil {
		return nil, xerror.ErrListKVFailed
	}
	defer it.Close()

	if option.KeyOnly {
		tx.SetOption(kv.KeyOnly, true)
	}

	if option.ReplicaRead {
		snapshot := tx.GetSnapshot()
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}

	ret := make([]store.KeyEntry, 0)
	for it.Valid() && limit > 0 {
		ret = append(ret, store.KeyEntry{Key: it.Key(), Entry: it.Value()})
		limit--
		err = it.Next()
		if err != nil {
			return nil, xerror.ErrListKVFailed
		}
	}
	return ret, nil
}

func (t *TiKV) CheckAndPut(key, oldVal, newVal []byte) error {
	tx, err := t.client.Begin()
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.WriteTimeout.Duration)
	defer cancel()
	v, err := tx.Get(ctx, key)
	if kv.IsErrNotFound(err) {
		if len(oldVal) > 0 {
			return xerror.ErrCheckAndSetFailed
		}
	} else if err != nil {
		return xerror.ErrGetKVFailed
	} else {
		if !bytes.Equal(oldVal, v) {
			return xerror.ErrCheckAndSetFailed
		}
	}

	if len(newVal) == 0 {
		err = tx.Delete(key)
	} else {
		err = tx.Set(key, newVal)
	}

	if err != nil {
		return xerror.ErrSetKVFailed
	}

	err = tx.Commit(ctx)
	if err != nil {
		return xerror.ErrCommitKVFailed
	}
	return nil
}

func (t *TiKV) Put(key, val []byte) error {
	tx, err := t.client.Begin()
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	if len(val) == 0 {
		err = tx.Delete(key)
	} else {
		err = tx.Set(key, val)
	}

	if err != nil {
		return xerror.ErrSetKVFailed
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.WriteTimeout.Duration)
	defer cancel()
	err = tx.Commit(ctx)
	if err != nil {
		return xerror.ErrCommitKVFailed
	}
	return nil
}
