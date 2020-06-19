package store

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store/kafka"
	"gitlab.s.upyun.com/platform/tikv-proxy/store/tikv"
)

type DB interface {
	Close() error
	Get(key []byte, option Option) ([]byte, error)
	CheckAndPut(key, oldVal, newVal []byte) error
	List(start, end []byte, limit int, option Option) ([]KeyEntry, error)
}

type KeyEntry struct {
	Key   []byte
	Entry []byte
}

type Option struct {
	ReplicaRead bool
	KeyOnly     bool
}

type Connector interface {
	Close()
	Send(msg KeyEntry) error
}

type Store struct {
	db        DB
	connector Connector
}

type Log struct {
	Old []byte
	New []byte
}

func NewStore(conf *config.Config) (*Store, error) {
	connector, err := kafka.NewConnector(conf)
	if err != nil {
		return nil, err
	}
	db, err := tikv.NewDB(conf)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, connector: connector}, nil
}

func (s *Store) Close() error {
	s.connector.Close()
	return s.db.Close()
}

func (s *Store) Get(key []byte) ([]byte, error) {
	return s.db.Get(key)
}

func (s *Store) CheckAndPut(key, oldVal, newVal []byte) error {
	err := s.db.CheckAndPut(key, oldVal, newVal)
	if err != nil {
		return err
	}

	entry, err := json.Marshal(Log{Old: oldVal, New: newVal})
	if err != nil {
		logrus.Errorf("marshal failed %s", err)
	} else {
		err := s.connector.Send(KeyEntry{Key: key, Entry: entry})
	}
}
