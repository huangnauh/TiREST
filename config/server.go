package config

import (
	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

type Connector struct {
	BrokerList      []string
	Retry           int
	BackOff         time.Duration
	MaxBackOff      time.Duration
	Topic           string        `toml:"topic"`
	QueueDataPath   string        `toml:"queue-data-path"`
	MemQueueSize    int64         `toml:"mem-queue-size"`
	MaxBytesPerFile int64         `toml:"max-bytes-per-file"`
	SyncEvery       int64         `toml:"sync-every"`
	SyncTimeout     time.Duration `toml:"sync-timeout"`
	MaxMsgSize      int32         `toml:"max-msg-size"`
}

type Store struct {
	PdAddresses  []string
	ReadTimeout  time.Duration
	ListTimeout  time.Duration
	WriteTimeout time.Duration
}

type Server struct {
	HttpHost          string        `toml:"http_host"`
	HttpPort          int           `toml:"http_port"`
	ReadTimeout       time.Duration `toml:"read_timeout"`
	ConnTimeout       time.Duration `toml:"conn_timeout"`
	ReadHeaderTimeout time.Duration `toml:"read_header_timeout"`
	WriteTimeout      time.Duration `toml:"write_timeout"`
	IdleTimeout       time.Duration `toml:"idle_timeout"`
	MaxIdleConns      int           `toml:"max_idle_conns"`
}

type Log struct {
	Level        string `toml:"level"`
	ErrorLogDir  string `toml:"error_log_dir"`
	AccessLogDir string `toml:"access_log_dir"`
	BufferSize   int    `toml:"buffer_size"`
	MaxBytes     int    `toml:"max_bytes"`
	BackupCount  int    `toml:"backup_count"`
}

type Config struct {
	Store         Store
	Server        Server
	Connector     Connector
	Log           Log
	EnableTracing bool `toml:"enable_tracing"`
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
