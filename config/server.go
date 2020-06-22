package config

import (
	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

type Connector struct {
	Name            string        `toml:"name"`
	BrokerList      []string      `toml:"broker-list"`
	Retry           int           `toml:"entry"`
	BackOff         time.Duration `toml:"back-off"`
	MaxBackOff      time.Duration `toml:"max-back-off"`
	Topic           string        `toml:"topic"`
	QueueDataPath   string        `toml:"queue-data-path"`
	MemQueueSize    int64         `toml:"mem-queue-size"`
	MaxBytesPerFile int64         `toml:"max-bytes-per-file"`
	SyncEvery       int64         `toml:"sync-every"`
	SyncTimeout     time.Duration `toml:"sync-timeout"`
	MaxMsgSize      int32         `toml:"max-msg-size"`
	WriteTimeout    time.Duration `toml:"write-timeout"`
}

type Store struct {
	Name         string        `toml:"name"`
	Path         string        `toml:"path"`
	PdAddresses  []string      `toml:"pd-address"`
	ReadTimeout  time.Duration `toml:"read-timeout"`
	ListTimeout  time.Duration `toml:"list-timeout"`
	WriteTimeout time.Duration `toml:"write-timeout"`
}

type Server struct {
	HttpHost          string        `toml:"http-host"`
	HttpPort          int           `toml:"http-port"`
	ReadTimeout       time.Duration `toml:"read-timeout"`
	ConnTimeout       time.Duration `toml:"conn-timeout"`
	ReadHeaderTimeout time.Duration `toml:"read-header-timeout"`
	WriteTimeout      time.Duration `toml:"write-timeout"`
	IdleTimeout       time.Duration `toml:"idle-timeout"`
	MaxIdleConns      int           `toml:"max-idle-conns"`
	ReplicaRead       bool          `toml:"replica-read"`
}

type Log struct {
	Level        string `toml:"level"`
	ErrorLogDir  string `toml:"error-log-dir"`
	AccessLogDir string `toml:"access-log-dir"`
	BufferSize   int    `toml:"buffer-size"`
	MaxBytes     int    `toml:"max-bytes"`
	BackupCount  int    `toml:"backup_count"`
}

type Config struct {
	Store         Store     `toml:"store"`
	Server        Server    `toml:"server"`
	Connector     Connector `toml:"connector"`
	Log           Log       `toml:"log"`
	EnableTracing bool      `toml:"enable-tracing"`
}

func defaultConfig() *Config {
	return &Config{
		Store: Store{
			PdAddresses:  []string{"127.0.0.1:2379"},
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			ListTimeout:  60 * time.Second,
		},
		Connector: Connector{
			BrokerList:      []string{"127.0.0.1:2379"},
			Retry:           10000,
			BackOff:         250 * time.Millisecond,
			MaxBackOff:      time.Minute,
			MemQueueSize:    10000,
			MaxBytesPerFile: 100 * 1024 * 1024,
			SyncEvery:       1000,
			SyncTimeout:     2 * time.Second,
			MaxMsgSize:      1024 * 1024,
		},
	}
}

func (c *Config) ReadFromFile(configFile string) error {
	logrus.Infof("read config file %s", configFile)
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}
	if _, err = toml.Decode(string(data), c); err != nil {
		return err
	}
	return nil
}

func (c *Config) HttpServerMode() string {
	if c.Log.Level == gin.DebugMode {
		return gin.DebugMode
	}

	return gin.ReleaseMode
}

func InitConfig(configFile string) (*Config, error) {
	conf := defaultConfig()
	err := conf.ReadFromFile(configFile)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
