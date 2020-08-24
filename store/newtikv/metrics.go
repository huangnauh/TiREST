package newtikv

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type Metric struct {
	WaitKVRespDurationHistogram *prometheus.HistogramVec
	WaitPDRespDurationHistogram *prometheus.HistogramVec
	BackOffDurationHistogram    *prometheus.HistogramVec
	PrewriteTimeHistogram       *prometheus.HistogramVec
	CommitTimeHistogram         *prometheus.HistogramVec
	GetCommitTsTimeHistogram    *prometheus.HistogramVec
	CommitBackOffTimeHistogram  *prometheus.HistogramVec
	ResolveLockTimeHistogram    *prometheus.HistogramVec
}

var metric = newMetric()

func (m *Metric) Observe(method string, execDetails *execdetails.StmtExecDetails,
	commitDetails *execdetails.CommitDetails) {
	if execDetails != nil {
		m.WaitKVRespDurationHistogram.WithLabelValues(method).Observe(
			time.Duration(execDetails.WaitKVRespDuration).Seconds())
		m.WaitPDRespDurationHistogram.WithLabelValues(method).Observe(
			time.Duration(execDetails.WaitPDRespDuration).Seconds())
		m.BackOffDurationHistogram.WithLabelValues(method).Observe(
			time.Duration(execDetails.BackoffDuration).Seconds())
	}

	if commitDetails != nil {
		m.PrewriteTimeHistogram.WithLabelValues(method).Observe(
			commitDetails.PrewriteTime.Seconds())
		m.CommitTimeHistogram.WithLabelValues(method).Observe(
			commitDetails.CommitTime.Seconds())
		m.GetCommitTsTimeHistogram.WithLabelValues(method).Observe(
			commitDetails.GetCommitTsTime.Seconds())
		m.CommitBackOffTimeHistogram.WithLabelValues(method).Observe(
			time.Duration(commitDetails.CommitBackoffTime).Seconds())
		m.ResolveLockTimeHistogram.WithLabelValues(method).Observe(
			time.Duration(commitDetails.ResolveLockTime).Seconds())
	}
}

func newMetric() *Metric {
	return &Metric{
		WaitKVRespDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "wait_kv_resp_duration",
				Help:      "Bucketed histogram of tikv wait kv resp duration.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		WaitPDRespDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "wait_pd_resp_duration",
				Help:      "Bucketed histogram of tikv wait pd resp duration.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		BackOffDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "backoff_duration",
				Help:      "Bucketed histogram of tikv backoff duration.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		PrewriteTimeHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "prewrite_time",
				Help:      "Bucketed histogram of prewrite time.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		CommitTimeHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "commit_time",
				Help:      "Bucketed histogram of commit time.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		GetCommitTsTimeHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "get_commit_ts_time",
				Help:      "Bucketed histogram of get commit ts time.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		ResolveLockTimeHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "reslove_lock_time",
				Help:      "Bucketed histogram of reslove lock time.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
		CommitBackOffTimeHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: "tikv",
				Name:      "commit_backoff_time",
				Help:      "Bucketed histogram of commit backoff time.",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
			},
			[]string{"method"},
		),
	}
}

func registerMetrics() {
	prometheus.MustRegister(metric.WaitKVRespDurationHistogram)
	prometheus.MustRegister(metric.WaitPDRespDurationHistogram)
	prometheus.MustRegister(metric.BackOffDurationHistogram)
	prometheus.MustRegister(metric.PrewriteTimeHistogram)
	prometheus.MustRegister(metric.CommitTimeHistogram)
	prometheus.MustRegister(metric.GetCommitTsTimeHistogram)
	prometheus.MustRegister(metric.ResolveLockTimeHistogram)
	prometheus.MustRegister(metric.CommitBackOffTimeHistogram)

	prometheus.MustRegister(metrics.TiKVBackoffHistogram)
	prometheus.MustRegister(metrics.TiKVCoprocessorHistogram)
	prometheus.MustRegister(metrics.TiKVLoadSafepointCounter)
	prometheus.MustRegister(metrics.TiKVLockResolverCounter)
	prometheus.MustRegister(metrics.TiKVRawkvCmdHistogram)
	prometheus.MustRegister(metrics.TiKVRawkvSizeHistogram)
	prometheus.MustRegister(metrics.TiKVRegionCacheCounter)
	prometheus.MustRegister(metrics.TiKVRegionErrorCounter)
	prometheus.MustRegister(metrics.TiKVSecondaryLockCleanupFailureCounter)
	prometheus.MustRegister(metrics.TiKVSendReqHistogram)
	prometheus.MustRegister(metrics.TiKVTxnCmdHistogram)
	prometheus.MustRegister(metrics.TiKVTxnRegionsNumHistogram)
	prometheus.MustRegister(metrics.TiKVTxnWriteKVCountHistogram)
	prometheus.MustRegister(metrics.TiKVTxnWriteSizeHistogram)
	prometheus.MustRegister(metrics.TiKVLocalLatchWaitTimeHistogram)
	prometheus.MustRegister(metrics.TiKVPendingBatchRequests)
	prometheus.MustRegister(metrics.TiKVStatusDuration)
	prometheus.MustRegister(metrics.TiKVStatusCounter)
	prometheus.MustRegister(metrics.TiKVBatchWaitDuration)
	prometheus.MustRegister(metrics.TiKVBatchClientUnavailable)
	prometheus.MustRegister(metrics.TiKVBatchClientWaitEstablish)
	prometheus.MustRegister(metrics.TiKVRangeTaskStats)
	prometheus.MustRegister(metrics.TiKVRangeTaskPushDuration)
	prometheus.MustRegister(metrics.HandleSchemaValidate)
	prometheus.MustRegister(metrics.TiKVTokenWaitDuration)
	prometheus.MustRegister(metrics.TiKVTxnHeartBeatHistogram)
	prometheus.MustRegister(metrics.TiKVPessimisticLockKeysDuration)
	prometheus.MustRegister(metrics.GRPCConnTransientFailureCounter)
	prometheus.MustRegister(metrics.TiKVTTLLifeTimeReachCounter)
	prometheus.MustRegister(metrics.TiKVNoAvailableConnectionCounter)

	prometheus.MustRegister(metrics.TSFutureWaitDuration)

	prometheus.MustRegister(metrics.GCHistogram)
	prometheus.MustRegister(metrics.GCJobFailureCounter)
	prometheus.MustRegister(metrics.GCRegionTooManyLocksCounter)
	prometheus.MustRegister(metrics.GCWorkerCounter)
}

func init() {
	registerMetrics()
}
