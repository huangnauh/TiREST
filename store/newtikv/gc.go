package newtikv

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/sirupsen/logrus"
	"github.com/huangnauh/tirest/xerror"
)

type GCWorker struct {
	store    tikv.Storage
	pdClient pd.Client
	cancel   context.CancelFunc
	log      *logrus.Entry
}

const (
	gcDefaultLifeTime    = time.Minute * 10
	gcWorkerTickInterval = 60
	gcScanLockLimit      = tikv.ResolvedCacheSize / 2
	GcSavedLockSafePoint = "/tidb/store/gcworker/saved_lock_safe_point"
)

func NewGCWorker(store tikv.Storage, pdClient pd.Client) (tikv.GCHandler, error) {
	worker := &GCWorker{
		store:    store,
		pdClient: pdClient,
		log:      logrus.WithFields(logrus.Fields{"worker": "gc"}),
	}
	return worker, nil
}

func (w *GCWorker) Close() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	w.tick(ctx)
	w.tickLock(ctx)
	go w.run(ctx)
}

func (w *GCWorker) run(ctx context.Context) {
	tickTime := time.Duration(gcWorkerTickInterval + rand.Intn(gcWorkerTickInterval))
	ticker := time.NewTicker(tickTime * time.Second)
	lockTickTime := time.Duration((gcWorkerTickInterval + rand.Intn(gcWorkerTickInterval)) * 60 * 24)
	lockTicker := time.NewTicker(lockTickTime * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case <-lockTicker.C:
			w.tickLock(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *GCWorker) tickLock(ctx context.Context) {
	lastLockSafePoint, err := w.getSafePoint(GcSavedLockSafePoint)
	if err != nil {
		w.log.Errorf("getSafePoint failed %s", err)
		return
	}
	lastSafePoint, err := w.getSafePoint(tikv.GcSavedSafePoint)
	if err != nil {
		w.log.Errorf("getSafePoint failed %s", err)
		return
	}

	if lastLockSafePoint.Add(24 * time.Hour).After(lastSafePoint) {
		return
	}
	safePointValue := oracle.ComposeTS(oracle.GetPhysical(lastSafePoint), 0)
	err = w.putSafePoint(GcSavedLockSafePoint, safePointValue)
	if err != nil {
		w.log.Errorf("putSafePoint failed %s", err)
		return
	}

	go func(safePointValue uint64) {
		//TODO: slow
		err = w.legacyResolveLocks(ctx, safePointValue, 1)
		if err != nil {
			w.log.Errorf("legacyResolveLocks %d failed %s", safePointValue, err)
			return
		}
	}(safePointValue)
}

func (w *GCWorker) tick(ctx context.Context) {
	w.log.Info("gc start")
	metrics.GCWorkerCounter.WithLabelValues("run_job").Inc()
	safePoint, newSafePointValue, err := w.calculateNewSafePoint()
	if err != nil {
		w.log.Errorf("calculateNewSafePoint failed %s", err)
		return
	}

	lastSafePoint, err := w.getSafePoint(tikv.GcSavedSafePoint)
	if err != nil {
		w.log.Errorf("getSafePoint failed %s", err)
		return
	}

	if lastSafePoint.Add(time.Duration(gcWorkerTickInterval) * time.Second).After(safePoint) {
		w.log.Infof("lastSafePoint %s near safePoint %s", lastSafePoint, safePoint)
		return
	}

	err = w.putSafePoint(tikv.GcSavedSafePoint, newSafePointValue)
	if err != nil {
		w.log.Errorf("putSafePoint %d failed %s", newSafePointValue, err)
		metrics.GCJobFailureCounter.WithLabelValues("save_safe_point").Inc()
		return
	}
	err = w.uploadSafePointToPD(ctx, newSafePointValue)
	if err != nil {
		w.log.Errorf("uploadSafePointToPD %d failed %s", newSafePointValue, err)
		metrics.GCJobFailureCounter.WithLabelValues("upload_safe_point").Inc()
		return
	}
}

func (w *GCWorker) getSafePoint(key string) (time.Time, error) {
	value, err := w.store.GetSafePointKV().Get(key)
	if err != nil {
		w.log.Errorf("get safe point failed %s", err)
		return time.Time{}, err
	}

	safePointTS, err := strconv.ParseUint(value, 10, 64)
	safePoint := time.Unix(0, oracle.ExtractPhysical(safePointTS)*1e6)
	return safePoint, nil
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.store.CurrentVersion()
	if err != nil {
		return time.Time{}, err
	}
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	return time.Unix(sec, nsec), nil
}

func (w *GCWorker) calculateNewSafePoint() (time.Time, uint64, error) {
	now, err := w.getOracleTime()
	if err != nil {
		return time.Time{}, 0, err
	}

	safePoint := now.Add(-gcDefaultLifeTime)
	safePointValue := oracle.ComposeTS(oracle.GetPhysical(safePoint), 0)
	safePoint = oracle.GetTimeFromTS(safePointValue)
	return safePoint, safePointValue, nil
}

func (w *GCWorker) putSafePoint(key string, safePointValue uint64) error {
	s := strconv.FormatUint(safePointValue, 10)
	return w.store.GetSafePointKV().Put(key, s)
}

func (w *GCWorker) uploadSafePointToPD(ctx context.Context, safePoint uint64) error {
	newSafePoint, err := w.pdClient.UpdateGCSafePoint(ctx, safePoint)
	if err != nil {
		return err
	}
	if newSafePoint != safePoint {
		w.log.Errorf("PD rejected safe point %d using %d", safePoint, newSafePoint)
		return xerror.ErrGetSafePointFailed
	}
	w.log.Infof("sent safe point %d to PD", safePoint)
	return nil
}

func (w *GCWorker) resolveLocksForRange(ctx context.Context, safePoint uint64, startKey []byte, endKey []byte) (tikv.RangeTaskStat, error) {
	w.log.Infof("start resolve locks safePoint %d range(%s-%s)", safePoint, startKey, endKey)
	req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: safePoint,
		Limit:      gcScanLockLimit,
	})

	var stat tikv.RangeTaskStat
	key := startKey
	bo := tikv.NewBackofferWithVars(ctx, tikv.GcResolveLockMaxBackoff, nil)
retryScanAndResolve:
	for {
		select {
		case <-ctx.Done():
			w.log.Warnf("canceled resolve locks safePoint %d range(%s-%s)", safePoint, startKey, endKey)
			return stat, errors.New("gc job canceled")
		default:
		}

		req.ScanLock().StartKey = key
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			w.log.Errorf("locate region from %s, %s", key, err)
			return stat, err
		}
		resp, err := w.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			w.log.Errorf("scan lock to region %d, send req %s", loc.Region.GetID(), err)
			return stat, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			w.log.Errorf("scan lock to region %d, response %s", loc.Region.GetID(), err)
			return stat, err
		}
		if regionErr != nil {
			w.log.Errorf("scan lock to region %d, response %s", loc.Region.GetID(), regionErr)
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return stat, err
			}
			continue
		}
		if resp.Resp == nil {
			w.log.Errorf("scan lock to region %d, response missing", loc.Region.GetID())
			return stat, tikv.ErrBodyMissing
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		rerr := locksResp.GetError()
		if rerr != nil {
			w.log.Errorf("scan lock to region %d, response %s", loc.Region.GetID(), rerr)
			return stat, errors.New(rerr.String())
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*tikv.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = tikv.NewLock(locksInfo[i])
		}

		for {
			ok, err1 := w.store.GetLockResolver().BatchResolveLocks(bo, locks, loc.Region)
			if err1 != nil {
				w.log.Errorf("resolver lock to region %d, %s", loc.Region.GetID(), err1)
				return stat, err1
			}
			if !ok {
				err = bo.Backoff(tikv.BoTxnLock, errors.New("remain locks"))
				if err != nil {
					w.log.Errorf("resolver lock to region %d, %s", loc.Region.GetID(), err)
					return stat, err
				}
				stillInSame, refreshedLoc, err := w.tryRelocateLocksRegion(bo, locks)
				if err != nil {
					w.log.Errorf("try relocate locks region %d, %s", loc.Region.GetID(), err)
					return stat, err
				}
				if stillInSame {
					loc = refreshedLoc
					continue
				}
				continue retryScanAndResolve
			}
			break
		}

		if len(locks) > 0 {
			w.log.Infof("region %d resolve %d locks done %s", loc.Region.GetID(), len(locks), loc.EndKey)
		}

		if len(locks) < gcScanLockLimit {
			stat.CompletedRegions++
			key = loc.EndKey
		} else {
			w.log.Infof("region %d has more than limit locks", loc.Region.GetID())
			metrics.GCRegionTooManyLocksCounter.Inc()
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
		bo = tikv.NewBackoffer(ctx, tikv.GcResolveLockMaxBackoff)
	}
	return stat, nil
}

func (w *GCWorker) legacyResolveLocks(ctx context.Context, safePoint uint64, concurrency int) error {
	w.log.Infof("start resolve locks safePoint %d concurrency %d", safePoint, concurrency)
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks").Inc()
	startTime := time.Now()
	handler := func(ctx context.Context, r kv.KeyRange) (tikv.RangeTaskStat, error) {
		return w.resolveLocksForRange(ctx, safePoint, r.StartKey, r.EndKey)
	}
	runner := tikv.NewRangeTaskRunner("resolve-locks-runner", w.store, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		w.log.Errorf("run resolve-locks failed, %s", err)
		return err
	}
	w.log.Infof("finish resolve locks %d %d", safePoint, runner.CompletedRegions())
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) tryRelocateLocksRegion(bo *tikv.Backoffer, locks []*tikv.Lock) (stillInSameRegion bool, refreshedLoc *tikv.KeyLocation, err error) {
	if len(locks) == 0 {
		return
	}
	refreshedLoc, err = w.store.GetRegionCache().LocateKey(bo, locks[0].Key)
	if err != nil {
		return
	}
	stillInSameRegion = refreshedLoc.Contains(locks[len(locks)-1].Key)
	return
}
