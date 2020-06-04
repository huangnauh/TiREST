package tikv

import (
	"bytes"
	"context"
	tikvConfig "github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"time"
)

type TiKV struct {
	client *txnkv.Client
	conf   *config.Config
}

func Open(conf *config.Config) (store.DB, error) {
	tikvConfig := tikvConfig.Default()
	tikvConfig.Txn.TsoSlowThreshold = 100 * time.Millisecond
	client, err := txnkv.NewClient(context.TODO(), conf.Store.PdAddresses, tikvConfig)
	if err != nil {
		return nil, err
	}
	return &TiKV{
		client: client,
		conf:   conf,
	}, nil
}

func (t *TiKV) Close() error {
	return t.client.Close()
}

func (t *TiKV) Get(key []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.ReadTimeout)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return nil, xerror.ErrGetTimestampFailed
	}

	v, err := tx.Get(ctx, key)
	if err == kv.ErrNotExist {
		return nil, xerror.ErrNotExists
	}
	if err != nil {
		return nil, xerror.ErrGetKVFailed
	}
	return v, nil
}

func (t *TiKV) List(start, end []byte, limit int) ([]store.KeyEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.ListTimeout)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return nil, xerror.ErrGetTimestampFailed
	}

	it, err := tx.Iter(ctx, key.Key(start), key.Key(end))
	if err != nil {
		return nil, xerror.ErrListKVFailed
	}
	defer it.Close()

	ret := make([]store.KeyEntry, 0)
	for it.Valid() && limit > 0 {
		ret = append(ret, store.KeyEntry{Key: it.Key(), Entry: it.Value()})
		limit--
		err = it.Next(ctx)
		if err != nil {
			return nil, xerror.ErrListKVFailed
		}
	}
	return ret, nil
}

func (t *TiKV) CheckAndPut(key, oldVal, newVal []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.WriteTimeout)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	v, err := tx.Get(ctx, key)
	if err == kv.ErrNotExist {
		if oldVal != nil {
			return xerror.ErrCheckAndSetFailed
		}
	} else if err != nil {
		return xerror.ErrGetKVFailed
	} else {
		if !bytes.Equal(oldVal, v) {
			return xerror.ErrCheckAndSetFailed
		}
	}

	err = tx.Set(key, newVal)
	if err != nil {
		return xerror.ErrSetKVFailed
	}

	err = tx.Commit(ctx)
	if err != nil {
		return xerror.ErrCommitKVFailed
	}
	return nil
}
