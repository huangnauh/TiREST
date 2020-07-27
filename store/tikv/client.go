package tikv

import (
	"context"
	"github.com/sirupsen/logrus"
	tikvConfig "github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"time"
)

const DBName = "tikv"

type TiKV struct {
	client *txnkv.Client
	conf   *config.Config
	log    *logrus.Entry
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
	tikvConfig := tikvConfig.Default()
	tikvConfig.Txn.TsoSlowThreshold = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), conf.Store.ReadTimeout.Duration)
	defer cancel()
	client, err := txnkv.NewClient(ctx, conf.Store.PdAddresses, tikvConfig)
	if err != nil {
		return nil, err
	}
	return &TiKV{
		client: client,
		conf:   conf,
		log:    logrus.WithFields(logrus.Fields{"worker": DBName}),
	}, nil
}

func (t *TiKV) Close() error {
	return t.client.Close()
}

func (t *TiKV) Get(key []byte, option store.GetOption) (store.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.ReadTimeout.Duration)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return store.NoValue, xerror.ErrGetTimestampFailed
	}

	v, err := tx.Get(ctx, key)
	secondary := false
	if err == kv.ErrNotExist && option.Secondary != nil {
		secondary = true
		v, err = tx.Get(ctx, option.Secondary)
	}

	if err == kv.ErrNotExist {
		return store.NoValue, xerror.ErrNotExists
	}
	if err != nil {
		return store.NoValue, xerror.ErrGetKVFailed
	}
	return store.Value{Secondary: secondary, Value: v}, nil
}

func (t *TiKV) List(start, end []byte, limit int, option store.ListOption) ([]store.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.ListTimeout.Duration)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return nil, xerror.ErrGetTimestampFailed
	}
	if option.KeyOnly {
		tx.SetOption(kv.KeyOnly, true)
	}

	s := key.Key(start)
	e := key.Key(end)

	var it kv.Iterator
	if !option.Reverse {
		it, err = tx.Iter(ctx, s, e)
	} else {
		it, err = tx.IterReverse(ctx, e)
	}
	if err != nil {
		return nil, xerror.ErrListKVFailed
	}
	defer it.Close()

	ret := make([]store.KeyValue, 0)
	for it.Valid() {
		k := it.Key()
		if key.Key(k).Cmp(s) < 0 || key.Key(k).Cmp(e) >= 0 {
			break
		}

		v := it.Value()
		k, v, err = option.Item(k, v)
		if err != nil {
			continue
		}

		ret = append(ret, store.KeyValue{Key: utils.B2S(k), Value: utils.B2S(v)})
		limit--
		if limit <= 0 {
			break
		}
		err = it.Next(ctx)
		if err != nil {
			return nil, xerror.ErrListKVFailed
		}
	}
	return ret, nil
}

func (t *TiKV) CheckAndPut(key, oldVal, newVal []byte, option store.CheckOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.WriteTimeout.Duration)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	existVal, err := tx.Get(ctx, key)
	if err == kv.ErrNotExist {
		existVal = nil
	} else if err != nil {
		return xerror.ErrGetKVFailed
	}

	if option.Check != nil {
		newVal, err = option.Check(oldVal, newVal, existVal)
		if err != nil {
			return err
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
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.WriteTimeout.Duration)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	if len(val) > 0 {
		err = tx.Delete(key)
	} else {
		err = tx.Set(key, val)
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

func (t *TiKV) BatchPut(items []store.KeyEntry) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.BatchPutTimeout.Duration)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	for _, item := range items {
		if len(item.Entry) == 0 {
			err = tx.Delete(item.Key)
		} else {
			err = tx.Set(item.Key, item.Entry)
		}
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

func (t *TiKV) BatchDelete(start, end []byte, limit int) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.BatchDeleteTimeout.Duration)
	defer cancel()
	tx, err := t.client.Begin(ctx)
	if err != nil {
		return nil, 0, xerror.ErrGetTimestampFailed
	}
	tx.SetOption(kv.KeyOnly, true)

	it, err := tx.Iter(ctx, key.Key(start), key.Key(end))
	if err != nil {
		return nil, 0, xerror.ErrListKVFailed
	}
	defer it.Close()

	count := 0
	var lastKey key.Key
	for it.Valid() {
		k := it.Key()
		t.log.Debugf("delete key %s", k)
		err = tx.Delete(k)
		if err != nil {
			t.log.Errorf("delete key %s, err: %s", k, err)
			break
		}
		count++
		lastKey = k
		if limit > 0 && count >= limit {
			break
		}
		err = it.Next(ctx)
		if err != nil {
			t.log.Errorf("next key %s, err: %s", k, err)
			break
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, 0, xerror.ErrCommitKVFailed
	}
	return lastKey, count, nil
}

func (t *TiKV) UnsafeDelete(_, _ []byte) error {
	return nil
}
