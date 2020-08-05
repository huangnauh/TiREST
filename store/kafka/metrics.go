package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.s.upyun.com/platform/tikv-proxy/version"
)

type Metric struct {
	Queue prometheus.Gauge
	Chan  prometheus.Gauge
}

var metric = newMetric()

func newMetric() *Metric {
	return &Metric{
		Queue: prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: version.APPMetrics,
			Name:      "connector_queue_depth",
			Help:      "Connector queue depth.",
		}),
		Chan: prometheus.NewGauge(prometheus.GaugeOpts{
			Subsystem: version.APPMetrics,
			Name:      "connector_chan_depth",
			Help:      "Connector chan depth.",
		}),
	}
}

func (m *Metric) mustRegister() {
	prometheus.MustRegister(m.Queue, m.Chan)
}

func init() {
	metric.mustRegister()
}
