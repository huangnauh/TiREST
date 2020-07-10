package config

import (
	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"time"
)

type Duration struct {
	time.Duration
}

type Connector struct {
	Name            string    `toml:"name"`
	Version         string    `toml:"version"`
	EnableProducer  bool      `toml:"enable-producer"`
	DebugProducer   bool      `toml:"debug-producer"`
	BrokerList      []string  `toml:"broker-list"`
	FetchMetadata   bool      `toml:"fetch-metadata"`
	Retry           int       `toml:"entry"`
	BackOff         *Duration `toml:"back-off"`
	MaxBackOff      *Duration `toml:"max-back-off"`
	Topic           string    `toml:"topic"`
	PartitionNum    int32     `toml:"partition-num"`
	QueueDataPath   string    `toml:"queue-data-path"`
	MemQueueSize    int64     `toml:"mem-queue-size"`
	MaxBytesPerFile int64     `toml:"max-bytes-per-file"`
	SyncEvery       int64     `toml:"sync-every"`
	SyncTimeout     *Duration `toml:"sync-timeout"`
	MaxMsgSize      int32     `toml:"max-msg-size"`
	WriteTimeout    *Duration `toml:"write-timeout"`
}

type Store struct {
	Name               string    `toml:"name"`
	Path               string    `toml:"path"`
	PdAddresses        []string  `toml:"pd-address"`
	ReadTimeout        *Duration `toml:"read-timeout"`
	ListTimeout        *Duration `toml:"list-timeout"`
	WriteTimeout       *Duration `toml:"write-timeout"`
	BatchDeleteTimeout *Duration `toml:"batch-delete-timeout"`
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d *Duration) MarshalText() (text []byte, err error) {
	return []byte(d.Duration.String()), nil
}

type Server struct {
	HttpHost          string    `toml:"http-host"`
	HttpPort          int       `toml:"http-port"`
	ReadTimeout       *Duration `toml:"read-timeout"`
	ConnTimeout       *Duration `toml:"conn-timeout"`
	ReadHeaderTimeout *Duration `toml:"read-header-timeout"`
	WriteTimeout      *Duration `toml:"write-timeout"`
	IdleTimeout       *Duration `toml:"idle-timeout"`
	ReplicaRead       bool      `toml:"replica-read"`
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

func DefaultConfig() *Config {
	return &Config{
		Store: Store{
			Name:               "tikv",
			Path:               "tikv://127.0.0.1:2379",
			PdAddresses:        []string{"127.0.0.1:2379"},
			ReadTimeout:        &Duration{10 * time.Second},
			WriteTimeout:       &Duration{10 * time.Second},
			ListTimeout:        &Duration{60 * time.Second},
			BatchDeleteTimeout: &Duration{24 * time.Hour},
		},
		Server: Server{
			HttpHost:          "127.0.0.1",
			HttpPort:          6100,
			ReadTimeout:       &Duration{10 * time.Second},
			ConnTimeout:       &Duration{1 * time.Second},
			ReadHeaderTimeout: &Duration{5 * time.Second},
			WriteTimeout:      &Duration{10 * time.Second},
			IdleTimeout:       &Duration{2 * time.Minute},
			ReplicaRead:       false,
		},
		Connector: Connector{
			Name:            "kafka",
			Version:         "0.9.0.1",
			EnableProducer:  true,
			DebugProducer:   false,
			BrokerList:      []string{"127.0.0.1:2379"},
			FetchMetadata:   true,
			Retry:           10000,
			BackOff:         &Duration{250 * time.Millisecond},
			MaxBackOff:      &Duration{time.Minute},
			Topic:           "tikvmeta",
			PartitionNum:    512,
			QueueDataPath:   "./queue/",
			MemQueueSize:    10000,
			MaxBytesPerFile: 100 * 1024 * 1024,
			SyncEvery:       1000,
			SyncTimeout:     &Duration{2 * time.Second},
			MaxMsgSize:      1024 * 1024,
			WriteTimeout:    &Duration{50 * time.Millisecond},
		},
		Log: Log{
			Level:        "debug",
			ErrorLogDir:  "",
			AccessLogDir: "",
			BufferSize:   100 * 1024,
			MaxBytes:     512 * 1024 * 1024,
			BackupCount:  10,
		},
		EnableTracing: true,
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
	conf := DefaultConfig()
	err := conf.ReadFromFile(configFile)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func Save(cfg *Config, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return toml.NewEncoder(f).Encode(cfg)
}
