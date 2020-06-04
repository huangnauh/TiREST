package store

import (
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/store/tikv"
)

type DB interface {
	Close() error
	Get(key []byte) ([]byte, error)
	CheckAndPut(key, oldVal, newVal []byte) error
	List(start, end []byte, limit int) ([]KeyEntry, error)
}

type KeyEntry struct {
	Key   []byte
	Entry []byte
}

type Connector interface {
	Close()
	Send(msg KeyEntry) error
}

type Store struct {
	db        DB
	connector Connector
}

func Open(conf *config.Config) (DB, error) {
	db, err := tikv.Open(conf)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Close(db DB) error {
	return db.Close()
}
