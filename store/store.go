package store

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/xerror"
)

type DB interface {
	Close() error
	Get(key []byte, option Option) ([]byte, error)
	CheckAndPut(key, oldVal, newVal []byte) error
	Put(key, val []byte) error
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

var ReplicaReadOption = Option{ReplicaRead: true}
var KeyOnlyOption = Option{KeyOnly: true}
var NoOption = Option{}

type Connector interface {
	Close()
	Send(msg KeyEntry) error
}

type Store struct {
	db        DB
	connector Connector
	conf      *config.Config
	log       *logrus.Entry
}

type Log struct {
	Old []byte
	New []byte
}

type DBDriver interface {
	Name() string
	Open(conf *config.Config) (DB, error)
}

type ConnectorDriver interface {
	Name() string
	Open(conf *config.Config) (Connector, error)
}

var dDrivers = make(map[string]DBDriver)
var cDrivers = make(map[string]ConnectorDriver)

func RegisterDB(driver DBDriver) {
	name := driver.Name()
	if _, ok := dDrivers[name]; ok {
		panic(fmt.Errorf("store %s is already registered", name))
	}

	dDrivers[name] = driver
}

func RegisterConnector(driver ConnectorDriver) {
	name := driver.Name()
	if _, ok := cDrivers[name]; ok {
		panic(fmt.Errorf("store %s is already registered", name))
	}

	cDrivers[name] = driver
}

func NewStore(conf *config.Config) (*Store, error) {
	_, ok := cDrivers[conf.Connector.Name]
	if !ok {
		return nil, xerror.ErrNotRegister
	}
	_, ok = dDrivers[conf.Store.Name]
	if !ok {
		return nil, xerror.ErrNotRegister
	}
	return &Store{
		conf: conf,
		log:  logrus.WithFields(logrus.Fields{"worker": "store"}),
	}, nil
}

func (s *Store) Open() error {
	cDriver := cDrivers[s.conf.Connector.Name]
	connector, err := cDriver.Open(s.conf)
	if err != nil {
		return err
	}
	s.connector = connector

	dDriver := dDrivers[s.conf.Store.Name]
	db, err := dDriver.Open(s.conf)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *Store) Close() error {
	if s.connector != nil {
		s.connector.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store) Health() error {
	if s.db == nil {
		return xerror.ErrDatabaseNotExists
	}
	return nil
}

func (s *Store) Get(key []byte, opt Option) ([]byte, error) {
	if s.db == nil {
		return nil, xerror.ErrNotExists
	}
	return s.db.Get(key, opt)
}

func (s *Store) CheckAndPut(key []byte, entry []byte) error {
	if s.db == nil {
		return xerror.ErrNotExists
	}

	if len(entry) == 0 {
		s.log.Errorf("key %s cas need body", key)
		return xerror.ErrNotExists
	}

	l := &Log{}
	err := json.Unmarshal(entry, &l)
	if err != nil {
		s.log.Errorf("key %s cas invalid, %s", key, err)
		return err
	}

	err = s.db.CheckAndPut(key, l.Old, l.New)
	if err != nil {
		s.log.Errorf("key %s cas failed, %s", key, err)
		return err
	}

	if entry != nil && s.connector != nil {
		s.connector.Send(KeyEntry{Key: key, Entry: entry})
	}
	return nil
}

func (s *Store) List(start, end []byte, limit int, option Option) ([]KeyEntry, error) {
	if s.db == nil {
		return nil, xerror.ErrNotExists
	}

	res, err := s.db.List(start, end, limit, option)
	if err != nil {
		s.log.Errorf("list (%s-%s) limit %d, %s", start, end, limit, err)
		return nil, err
	}
	return res, nil
}
