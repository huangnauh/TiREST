package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/huangnauh/tirest/version"
)

var MaxProcs = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Subsystem: version.APP,
		Name:      "maxprocs",
		Help:      "The value of GOMAXPROCS.",
	})

func init() {
	prometheus.MustRegister(MaxProcs)
}
