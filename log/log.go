package log

import (
	"github.com/nsqio/go-diskqueue"
	"github.com/sirupsen/logrus"
)

type Formatter struct {
}

func (f *Formatter) Format(entry *logrus.Entry) ([]byte, error) {
	return []byte(entry.Message), nil
}

func NewLogFunc(log *logrus.Entry) diskqueue.AppLogFunc {
	return func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		var level logrus.Level
		switch lvl {
		case diskqueue.DEBUG:
			level = logrus.DebugLevel
		case diskqueue.INFO:
			level = logrus.InfoLevel
		case diskqueue.WARN:
			level = logrus.WarnLevel
		case diskqueue.ERROR:
			level = logrus.ErrorLevel
		case diskqueue.FATAL:
			level = logrus.FatalLevel
		default:
			level = logrus.InfoLevel
		}
		log.Logf(level, f, args...)
	}
}
