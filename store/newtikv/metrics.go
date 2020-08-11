package newtikv

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func registerMetrics() {
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

	prometheus.MustRegister(metrics.GCHistogram)
	prometheus.MustRegister(metrics.GCJobFailureCounter)
	prometheus.MustRegister(metrics.GCRegionTooManyLocksCounter)
	prometheus.MustRegister(metrics.GCWorkerCounter)
}

func init() {
	registerMetrics()
}
