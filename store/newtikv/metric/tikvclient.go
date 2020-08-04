// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.s.upyun.com/platform/tikv-proxy/version"
)

// TiKVClient metrics.
var (
	TiKVTxnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "txn_cmd_duration_seconds",
			Help:      "Bucketed histogram of processing time of txn cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{metrics.LblType})

	TiKVBackoffHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "backoff_seconds",
			Help:      "total backoff seconds of a single backoffer.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{metrics.LblType})

	TiKVSendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of sending request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{metrics.LblType, metrics.LblStore})

	TiKVCoprocessorHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "cop_duration_seconds",
			Help:      "Run duration of a single coprocessor task, includes backoff time.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		})

	TiKVLockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "lock_resolver_actions_total",
			Help:      "Counter of lock resolver actions.",
		}, []string{metrics.LblType})

	TiKVRegionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "region_err_total",
			Help:      "Counter of region errors.",
		}, []string{metrics.LblType})

	TiKVTxnWriteKVCountHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "txn_write_kv_num",
			Help:      "Count of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21), // 1 ~ 1048576
		})

	TiKVTxnWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "txn_write_size_bytes",
			Help:      "Size of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1Byte ~ 500MB
		})

	TiKVRawkvCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "rawkv_cmd_seconds",
			Help:      "Bucketed histogram of processing time of rawkv cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{metrics.LblType})

	TiKVRawkvSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "rawkv_kv_size_bytes",
			Help:      "Size of key/value to put, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 30), // 1Byte ~ 512MB
		}, []string{metrics.LblType})

	TiKVTxnRegionsNumHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "txn_regions_num",
			Help:      "Number of regions in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 25), // 1 ~ 16M
		}, []string{metrics.LblType})

	TiKVLoadSafepointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "load_safepoint_total",
			Help:      "Counter of load safepoint.",
		}, []string{metrics.LblType})

	TiKVSecondaryLockCleanupFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "lock_cleanup_task_total",
			Help:      "failure statistic of secondary lock cleanup task.",
		}, []string{metrics.LblType})

	TiKVRegionCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "region_cache_operations_total",
			Help:      "Counter of region cache.",
		}, []string{metrics.LblType, metrics.LblResult})

	TiKVLocalLatchWaitTimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "local_latch_wait_seconds",
			Help:      "Wait time of a get local latch.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
		})

	// TiKVPendingBatchRequests indicates the number of requests pending in the batch channel.
	TiKVPendingBatchRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "pending_batch_requests",
			Help:      "Pending batch requests",
		}, []string{"store"})

	TiKVStatusDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "kv_status_api_duration",
			Help:      "duration for kv status api.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20), // 0.5ms ~ 262s
		}, []string{"store"})

	TiKVStatusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "kv_status_api_count",
			Help:      "Counter of access kv status api.",
		}, []string{metrics.LblResult})

	TiKVBatchWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "batch_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "batch wait duration",
		})

	TiKVBatchClientUnavailable = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "batch_client_unavailable_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:      "batch client unavailable",
		})
	TiKVBatchClientWaitEstablish = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "batch_client_wait_connection_establish",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
			Help:      "batch client wait new connection establish",
		})

	TiKVRangeTaskStats = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "range_task_stats",
			Help:      "stat of range tasks",
		}, []string{metrics.LblType, metrics.LblResult})

	TiKVRangeTaskPushDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "range_task_push_duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
			Help:      "duration to push sub tasks to range task workers",
		}, []string{metrics.LblType})
	TiKVTokenWaitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "batch_executor_token_wait_duration",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 34), // 1ns ~ 8s
			Help:      "tidb txn token wait duration to process batches",
		})

	TiKVTxnHeartBeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "txn_heart_beat",
			Help:      "Bucketed histogram of the txn_heartbeat request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{metrics.LblType})
	TiKVPessimisticLockKeysDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "pessimistic_lock_keys_duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 24), // 1ms ~ 8389s
			Help:      "tidb txn pessimistic lock keys duration",
		})

	TiKVTTLLifeTimeReachCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "ttl_lifetime_reach_total",
			Help:      "Counter of ttlManager live too long.",
		})

	TiKVNoAvailableConnectionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: version.APPMetrics,
			Subsystem: "tikvclient",
			Name:      "batch_client_no_available_connection_total",
			Help:      "Counter of no available batch client.",
		})
)
