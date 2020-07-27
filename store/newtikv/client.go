package newtikv

import (
	"context"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/v4/client"
	tikvConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
	"sync"
)

const DBName = "newtikv"

type Range struct {
	Start []byte
	End   []byte
}

type TiKV struct {
	client     kv.Storage
	store      tikv.Storage
	pdClient   pd.Client
	cancel     context.CancelFunc
	conf       *config.Config
	log        *logrus.Entry
	deleteChan chan Range
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

	//TODO: set config
	cfg := tikvConfig.GetGlobalConfig()
	cfg.Log.Level = conf.Log.Level
	cfg.Log.EnableSlowLog = false
	tikvConfig.StoreGlobalConfig(cfg)
	err := logutil.InitZapLogger(cfg.Log.ToLogConfig())

	s, err := driver.Open(conf.Store.Path)
	if err != nil {
		return nil, err
	}

	t := &TiKV{
		client:     s,
		conf:       conf,
		deleteChan: make(chan Range, 10),
		log:        logrus.WithFields(logrus.Fields{"worker": DBName}),
	}

	if conf.Store.GCEnable {
		if raw, ok := s.(tikv.EtcdBackend); ok {
			tikv.NewGCHandlerFunc = t.NewGCWorker
			err = raw.StartGCWorker()
			if err != nil {
				return nil, err
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	go t.runDelete(ctx)
	//https://github.com/pingcap/tidb/pull/12095
	//gConfig := tiConfig.GetGlobalConfig()

	return t, nil
}

func (t *TiKV) NewGCWorker(store tikv.Storage, pdClient pd.Client) (tikv.GCHandler, error) {
	t.store = store
	t.pdClient = pdClient
	return NewGCWorker(store, pdClient)
}

func (t *TiKV) Close() error {
	t.cancel()
	return t.client.Close()
}

func (t *TiKV) Get(key []byte, option store.GetOption) (store.Value, error) {
	tx, err := t.client.Begin()
	if err != nil {
		return store.NoValue, xerror.ErrGetTimestampFailed
	}
	t.log.Debugf("start ts %d, %s", tx.StartTS(), kv.Key(key))
	if option.ReplicaRead {
		snapshot := tx.GetSnapshot()
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.ReadTimeout.Duration)
	defer cancel()
	v, err := tx.Get(ctx, key)
	secondary := false
	if kv.IsErrNotFound(err) && option.Secondary != nil {
		secondary = true
		v, err = tx.Get(ctx, option.Secondary)
	}

	if kv.IsErrNotFound(err) {
		return store.NoValue, xerror.ErrNotExists
	}
	if err != nil {
		return store.NoValue, xerror.ErrGetKVFailed
	}
	return store.Value{Secondary: secondary, Value: v}, nil
}

func (t *TiKV) List(start, end []byte, limit int, option store.ListOption) ([]store.KeyValue, error) {
	tx, err := t.client.Begin()
	if err != nil {
		t.log.Errorf("client begin failed %s", err)
		return nil, xerror.ErrGetTimestampFailed
	}

	if option.KeyOnly {
		tx.SetOption(kv.KeyOnly, true)
	}

	if option.ReplicaRead {
		snapshot := tx.GetSnapshot()
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}

	s := kv.Key(start)
	e := kv.Key(end)

	var it kv.Iterator
	if !option.Reverse {
		it, err = tx.Iter(s, e)
	} else {
		it, err = tx.IterReverse(e)
	}

	if err != nil {
		t.log.Errorf("iter (%s-%s) failed %s", err, start, end)
		return nil, xerror.ErrListKVFailed
	}

	defer it.Close()

	ret := make([]store.KeyValue, 0)
	for it.Valid() {
		k := it.Key()
		t.log.Debugf("iter key %v", k)
		if kv.Key(k).Cmp(s) < 0 || kv.Key(k).Cmp(e) >= 0 {
			break
		}

		v := it.Value()
		k, v, err = option.Item(k, v)
		if err != nil {
			t.log.Warnf("iter (%s-%s) key %s, err %s", start, end, k, err)
			continue
		}

		ret = append(ret, store.KeyValue{Key: utils.B2S(k), Value: utils.B2S(v)})

		limit--
		if limit <= 0 {
			break
		}
		err = it.Next()
		if err != nil {
			t.log.Errorf("iter next (%s-%s) failed %s", err, start, end)
			return nil, xerror.ErrListKVFailed
		}
	}
	return ret, nil
}

func (t *TiKV) CheckAndPut(key, oldVal, newVal []byte, check store.CheckOption) error {
	tx, err := t.client.Begin()
	if err != nil {
		return xerror.ErrGetTimestampFailed
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.WriteTimeout.Duration)
	defer cancel()
	t.log.Debugf("get key %s", key)
	existVal, err := tx.Get(ctx, key)
	if kv.IsErrNotFound(err) {
		existVal = nil
	} else if err != nil {
		return xerror.ErrGetKVFailed
	}

	if check.Check != nil {
		newVal, err = check.Check(oldVal, newVal, existVal)
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

func (t *TiKV) BatchPut(items []store.KeyEntry) error {
	tx, err := t.client.Begin()
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

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.BatchPutTimeout.Duration)
	defer cancel()
	err = tx.Commit(ctx)
	if err != nil {
		return xerror.ErrCommitKVFailed
	}
	return nil
}

func (t *TiKV) BatchDelete(start, end []byte, limit int) ([]byte, int, error) {
	tx, err := t.client.Begin()
	if err != nil {
		return nil, 0, xerror.ErrGetTimestampFailed
	}

	it, err := tx.Iter(kv.Key(start), kv.Key(end))
	if err != nil {
		return nil, 0, xerror.ErrListKVFailed
	}
	defer it.Close()
	tx.SetOption(kv.KeyOnly, true)
	snapshot := tx.GetSnapshot()
	snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)

	count := 0
	var lastKey kv.Key
	for it.Valid() {
		key := it.Key()
		if count == 0 {
			t.log.Infof("start delete %s, (%s-%s)", key, start, end)
		}
		err = tx.Delete(key)
		if err != nil {
			t.log.Errorf("delete key %s, err: %s", key, err)
			break
		}
		count++
		lastKey = key
		if limit > 0 && count >= limit {
			t.log.Infof("end delete %s, (%s-%s)", key, start, end)
			break
		}
		err = it.Next()
		if err != nil {
			t.log.Errorf("next key %s, err: %s", key, err)
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.conf.Store.BatchDeleteTimeout.Duration)
	defer cancel()
	err = tx.Commit(ctx)
	if err != nil {
		return nil, 0, xerror.ErrCommitKVFailed
	}

	return lastKey, count, nil
}

func (t *TiKV) UnsafeDelete(start, end []byte) error {
	select {
	case t.deleteChan <- Range{start, end}:
		t.log.Infof("accept delete (%s-%s)", start, end)
		return nil
	default:
		t.log.Errorf("reject delete (%s-%s)", start, end)
		return xerror.ErrUnsafeDestroyRangeFailed
	}
}

func (t *TiKV) runDelete(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-t.deleteChan:
			t.doUnsafeDestroyRangeRequest(ctx, d.Start, d.End, 1)
		}
	}
}

func (t *TiKV) getUpStoresForGC(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := t.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, err
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		if store.State != metapb.StoreState_Up {
			continue
		}
		upStores = append(upStores, store)
	}
	return upStores, nil
}

func (t *TiKV) doUnsafeDestroyRangeRequest(ctx context.Context, startKey []byte, endKey []byte, concurrency int) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	t.log.Infof("start unsafe delete (%s-%s)", startKey, endKey)
	stores, err := t.getUpStoresForGC(ctx)
	if err != nil {
		t.log.Errorf("delete ranges: get store list from PD, %s", err)
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})

	var wg sync.WaitGroup
	failed := false
	for _, s := range stores {
		address := s.Address
		storeID := s.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := t.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.UnsafeDestroyRangeTimeout)
			if err != nil {
				failed = true
				t.log.Errorf("unsafe destroy range store %d, err %s", storeID, err)
				return
			}
			if resp == nil || resp.Resp == nil {
				failed = true
				t.log.Errorf("unsafe destroy range returns nil response from store %v", storeID)
				return

			}

			errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
			if len(errStr) > 0 {
				failed = true
				t.log.Errorf("unsafe destroy range failed on store %d: %s", storeID, errStr)
				return
			}
		}()
	}

	wg.Wait()

	if failed {
		t.log.Errorf("unsafe destroy range failed")
		return xerror.ErrUnsafeDestroyRangeFailed
	}

	// Notify all affected regions in the range that UnsafeDestroyRange occurs.
	notifyTask := tikv.NewNotifyDeleteRangeTask(t.store, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		t.log.Errorf("failed notifying regions affected by UnsafeDestroyRange, %s", err)
		return xerror.ErrNotifyDeleteRangeFailed
	}

	return nil
}
