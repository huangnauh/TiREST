package middleware

import (
	"io/ioutil"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/huangnauh/tirest/log"
)

type Metric struct {
	inFlightGauge            prometheus.Gauge
	requestTotal             *prometheus.CounterVec
	requestDuration          *prometheus.SummaryVec
	requestDurationHistogram *prometheus.HistogramVec
	requestSize              *prometheus.SummaryVec
	responseSize             *prometheus.SummaryVec
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
		requestDurationHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gin_request_duration",
				Help:    "Bucketed histogram of request latencies.",
				Buckets: prometheus.ExponentialBuckets(0.0001, 2, 21), // 0.1ms ~ 104s
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
	prometheus.MustRegister(m.inFlightGauge, m.requestTotal, m.requestDuration,
		m.requestDurationHistogram, m.requestSize, m.responseSize)
}

func (m *Metric) handlerFunc(abnormal bool, slowRequest time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		requestSize := c.Request.ContentLength
		m.inFlightGauge.Inc()
		defer m.inFlightGauge.Dec()
		c.Next()

		respStatus := c.Writer.Status()
		status := strconv.Itoa(respStatus)
		now := time.Now()
		latency := now.Sub(start)
		responseSize := c.Writer.Size()

		m.requestSize.WithLabelValues(status, c.Request.Method).Observe(float64(requestSize))
		m.responseSize.WithLabelValues(status, c.Request.Method).Observe(float64(responseSize))
		m.requestDuration.WithLabelValues(status, c.Request.Method).Observe(latency.Seconds())
		m.requestDurationHistogram.WithLabelValues(status, c.Request.Method).Observe(latency.Seconds())
		m.requestTotal.WithLabelValues(status, c.Request.Method).Inc()

		if logger == nil {
			return
		}

		if abnormal && (latency < slowRequest) && (respStatus < 400 || respStatus == 404) {
			return
		}

		msg := ""
		if val, ok := c.Get(HttpMessage); ok && val != nil {
			msg, _ = val.(string)
		}

		logger.Infof("%s %s %s %s %d %d %d %s '%s'\n", c.Request.RemoteAddr,
			now.Format("2006-01-02T15:04:05.999"), c.Request.Method, c.Request.URL,
			respStatus, requestSize, responseSize, latency, msg)
	}
}

func InitLog(fileName string, bufferSize int, maxBytes int, backupCount int) error {
	if fileName == "" {
		logger = logrus.StandardLogger()
		return nil
	}

	logger = logrus.New()

	var err error
	logWriter, err = log.NewRotatingOuter(fileName, bufferSize, maxBytes,
		backupCount, log.OriginFormatter{})
	if err != nil {
		return err
	}

	logger.SetEntryBufferDisable(true)
	logger.Formatter = log.NullFormatter{}
	logger.SetOutput(ioutil.Discard)
	logger.AddHook(logWriter)
	return nil
}

func CloseAccessLog() {
	if logWriter != nil {
		logWriter.Close()
	}
}

func SetAccessLog(abnormal bool, slowRequest time.Duration) gin.HandlerFunc {
	return metric.handlerFunc(abnormal, slowRequest)
}
