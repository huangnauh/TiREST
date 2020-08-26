package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.s.upyun.com/platform/tikv-proxy/version"
)

var MaxProcs = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Subsystem: version.APPMetrics,
		Name:      "maxprocs",
		Help:      "The value of GOMAXPROCS.",
	})

func init() {
	prometheus.MustRegister(MaxProcs)
}
