package config

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/huangnauh/tirest/utils/json"
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
	Level              string    `toml:"level"`
	GCEnable           bool      `toml:"gc-enable"`
	PdAddresses        []string  `toml:"pd-address"`
	ReadTimeout        *Duration `toml:"read-timeout"`
	ListTimeout        *Duration `toml:"list-timeout"`
	WriteTimeout       *Duration `toml:"write-timeout"`
	BatchPutTimeout    *Duration `toml:"batch-put-timeout"`
	BatchDeleteTimeout *Duration `toml:"batch-delete-timeout"`
	TsoSlowThreshold   *Duration `toml:"tso-slow-threshold"`
	DisableLockBackOff bool      `toml:"disable-lock-back-off"`
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d *Duration) MarshalText() (text []byte, err error) {
	return []byte(d.Duration.String()), nil
}

type CheckOption string

const (
	NopCheck       CheckOption = "no"
	ExactCheck     CheckOption = "exact"
	TimestampCheck CheckOption = "timestamp"
)

type Server struct {
	HttpHost          string      `toml:"http-host"`
	HttpPort          int         `toml:"http-port"`
	ReadTimeout       *Duration   `toml:"read-timeout"`
	ConnTimeout       *Duration   `toml:"conn-timeout"`
	ReadHeaderTimeout *Duration   `toml:"read-header-timeout"`
	WriteTimeout      *Duration   `toml:"write-timeout"`
	IdleTimeout       *Duration   `toml:"idle-timeout"`
	SleepBeforeClose  *Duration   `toml:"sleep-before-close"`
	ReplicaRead       bool        `toml:"replica-read"`
	CheckOption       CheckOption `toml:"check-option"`
}

type Log struct {
	Level             string    `toml:"level"`
	ErrorLogDir       string    `toml:"error-log-dir"`
	AccessLogDir      string    `toml:"access-log-dir"`
	AbnormalAccessLog bool      `toml:"abnormal-access-log"`
	SlowRequest       *Duration `toml:"slow-request"`
	BufferSize        int       `toml:"buffer-size"`
	MaxBytes          int       `toml:"max-bytes"`
	BackupCount       int       `toml:"backup_count"`
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
			Level:              "warn",
			PdAddresses:        []string{"127.0.0.1:2379"},
			GCEnable:           true,
			ReadTimeout:        &Duration{10 * time.Second},
			WriteTimeout:       &Duration{10 * time.Second},
			ListTimeout:        &Duration{60 * time.Second},
			BatchPutTimeout:    &Duration{60 * time.Second},
			BatchDeleteTimeout: &Duration{10 * time.Minute},
			TsoSlowThreshold:   &Duration{150 * time.Millisecond},
		},
		Server: Server{
			HttpHost:          "127.0.0.1",
			HttpPort:          6100,
			ReadTimeout:       &Duration{10 * time.Second},
			ConnTimeout:       &Duration{1 * time.Second},
			ReadHeaderTimeout: &Duration{5 * time.Second},
			WriteTimeout:      &Duration{10 * time.Second},
			IdleTimeout:       &Duration{2 * time.Minute},
			SleepBeforeClose:  &Duration{5 * time.Second},
			ReplicaRead:       false,
			CheckOption:       TimestampCheck,
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
			Level:             "info",
			ErrorLogDir:       "",
			AccessLogDir:      "",
			AbnormalAccessLog: false,
			BufferSize:        100 * 1024,
			MaxBytes:          512 * 1024 * 1024,
			BackupCount:       10,
			SlowRequest:       &Duration{50 * time.Millisecond},
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

func (c *Config) String() string {
	jsonBytes, err := json.Marshal(c)
	if err != nil {
		logrus.Errorf("json marshal config, err: %s", err)
		return ""
	}
	return string(jsonBytes)
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
