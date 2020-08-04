package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/log"
	"strconv"
	"time"
)

type Metric struct {
	inFlightGauge   prometheus.Gauge
	requestTotal    *prometheus.CounterVec
	requestDuration *prometheus.SummaryVec
	requestSize     *prometheus.SummaryVec
	responseSize    *prometheus.SummaryVec
}

const HttpMessage = "msg"

var metric = newMetric()
var logger *logrus.Logger
var logWriter *log.RotatingOuter

func init() {
	metric.mustRegister()
}

func newMetric() *Metric {
	return &Metric{
		inFlightGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gin_in_flight_requests",
			Help: "A gauge of requests currently being served by the wrapped handler.",
		}),
		requestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gin_api_requests_total",
				Help: "A counter for requests to the wrapped handler.",
			},
			[]string{"code", "method"},
		),
		requestDuration: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "gin_request_duration_seconds",
				Help: "A summary of request latencies in seconds.",
			},
			[]string{"code", "method"},
		),
		requestSize: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "gin_request_size_bytes",
				Help: "A summary of request sizes for requests.",
			},
			[]string{"code", "method"},
		),
		responseSize: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: "gin_response_size_bytes",
				Help: "A summary of response sizes for responses.",
			},
			[]string{"code", "method"},
		),
	}
}

func (m *Metric) mustRegister() {
	prometheus.MustRegister(m.inFlightGauge, m.requestTotal, m.requestDuration, m.requestSize, m.responseSize)
}

func (m *Metric) handlerFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		requestSize := c.Request.ContentLength
		m.inFlightGauge.Inc()
		defer m.inFlightGauge.Dec()
		c.Next()

		status := strconv.Itoa(c.Writer.Status())
		now := time.Now()
		latency := now.Sub(start)
		responseSize := c.Writer.Size()

		m.requestSize.WithLabelValues(status, c.Request.Method).Observe(float64(requestSize))
		m.responseSize.WithLabelValues(status, c.Request.Method).Observe(float64(responseSize))
		m.requestDuration.WithLabelValues(status, c.Request.Method).Observe(float64(latency) / float64(time.Second))
		m.requestTotal.WithLabelValues(status, c.Request.Method).Inc()

		if logger == nil {
			return
		}

		msg := ""
		if val, ok := c.Get(HttpMessage); ok && val != nil {
			msg, _ = val.(string)
		}

		logger.Infof("%s %s %s %s %d %d %d %s '%s'\n", c.Request.RemoteAddr,
			now.Format("2006-01-02T15:04:05.999"), c.Request.Method, c.Request.URL,
			c.Writer.Status(), requestSize, responseSize, latency, msg)
	}
}

func InitLog(fileName string, bufferSize int, maxBytes int, backupCount int) error {
	if fileName == "" {
		logger = logrus.StandardLogger()
		return nil
	}

	logger = logrus.New()

	var err error
	logWriter, err = log.NewRotatingOuter(fileName, bufferSize, maxBytes, backupCount)
	if err != nil {
		return err
	}

	logger.Formatter = &log.Formatter{}
	logger.Out = logWriter
	return nil
}

func CloseAccessLog() {
	if logWriter != nil {
		logWriter.Close()
	}
}

func SetAccessLog() gin.HandlerFunc {
	return metric.handlerFunc()
}
