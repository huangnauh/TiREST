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
}

func init() {
	//TODO:
	//metrics.TiKVBackoffHistogram = metric.TiKVBackoffHistogram
	//metrics.TiKVCoprocessorHistogram = metric.TiKVCoprocessorHistogram
	//metrics.TiKVLoadSafepointCounter = metric.TiKVLoadSafepointCounter
	//metrics.TiKVLockResolverCounter = metric.TiKVLockResolverCounter
	//metrics.TiKVRawkvCmdHistogram = metric.TiKVRawkvCmdHistogram
	//metrics.TiKVRawkvSizeHistogram = metric.TiKVRawkvSizeHistogram
	//metrics.TiKVRegionCacheCounter = metric.TiKVRegionCacheCounter
	//metrics.TiKVRegionErrorCounter = metric.TiKVRegionErrorCounter
	//metrics.TiKVSecondaryLockCleanupFailureCounter = metric.TiKVSecondaryLockCleanupFailureCounter
	//metrics.TiKVSendReqHistogram = metric.TiKVSendReqHistogram
	//metrics.TiKVTxnCmdHistogram = metric.TiKVTxnCmdHistogram
	//metrics.TiKVTxnRegionsNumHistogram = metric.TiKVTxnRegionsNumHistogram
	//metrics.TiKVTxnWriteKVCountHistogram = metric.TiKVTxnWriteKVCountHistogram
	//metrics.TiKVTxnWriteSizeHistogram = metric.TiKVTxnWriteSizeHistogram
	//metrics.TiKVLocalLatchWaitTimeHistogram = metric.TiKVLocalLatchWaitTimeHistogram
	//metrics.TiKVPendingBatchRequests = metric.TiKVPendingBatchRequests
	//metrics.TiKVStatusDuration = metric.TiKVStatusDuration
	//metrics.TiKVStatusCounter = metric.TiKVStatusCounter
	//metrics.TiKVBatchWaitDuration = metric.TiKVBatchWaitDuration
	//metrics.TiKVBatchClientUnavailable = metric.TiKVBatchClientUnavailable
	//metrics.TiKVBatchClientWaitEstablish = metric.TiKVBatchClientWaitEstablish
	//metrics.TiKVRangeTaskStats = metric.TiKVRangeTaskStats
	//metrics.TiKVRangeTaskPushDuration = metric.TiKVRangeTaskPushDuration
	//metrics.TiKVTokenWaitDuration = metric.TiKVTokenWaitDuration
	//metrics.TiKVTxnHeartBeatHistogram = metric.TiKVTxnHeartBeatHistogram
	//metrics.TiKVPessimisticLockKeysDuration = metric.TiKVPessimisticLockKeysDuration
	//metrics.GRPCConnTransientFailureCounter = metric.GRPCConnTransientFailureCounter
	//metrics.TiKVTTLLifeTimeReachCounter = metric.TiKVTTLLifeTimeReachCounter
	//metrics.TiKVNoAvailableConnectionCounter = metric.TiKVNoAvailableConnectionCounter
	registerMetrics()
}
