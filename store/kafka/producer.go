package kafka

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/nsqio/go-diskqueue"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/log"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/version"
	"os"
	"time"
)

const (
	MaxMessage = 1024
	MQ         = "kafka"
)

type Connector struct {
	producer  sarama.AsyncProducer
	log       *logrus.Entry
	queue     diskqueue.Interface
	writeBuf  bytes.Buffer
	writeChan chan store.KeyEntry
	conf      *config.Config
	cfg       *sarama.Config
	//TODO: metrics
}

type Driver struct {
}

func init() {
	store.RegisterConnector(Driver{})
}

func (d Driver) Name() string {
	return MQ
}

func (d Driver) Open(conf *config.Config) (store.Connector, error) {

	l := logrus.WithFields(logrus.Fields{
		"worker": "kafka connector",
	})

	if err := os.MkdirAll(conf.Connector.QueueDataPath, 0755); err != nil {
		l.Errorf("Failed to mkdir, %s", err)
		return nil, err
	}
	queue := diskqueue.New(version.APP, conf.Connector.QueueDataPath,
		conf.Connector.MaxBytesPerFile, 4, conf.Connector.MaxMsgSize,
		conf.Connector.SyncEvery, conf.Connector.SyncTimeout.Duration, log.NewLogFunc(l))

	conn := &Connector{
		queue:     queue,
		log:       l,
		writeChan: make(chan store.KeyEntry, MaxMessage),
		conf:      conf,
	}

	go conn.runQueue()

	if conf.Connector.EnableProducer {
		var err error
		sarama.Logger = l
		c := sarama.NewConfig()
		c.Version, err = sarama.ParseKafkaVersion(conf.Connector.Version)
		if err != nil {
			l.Errorf("Error parsing version: %v", err)
			return nil, err
		}
		backoff := func(retries, maxRetries int) time.Duration {
			b := conf.Connector.BackOff.Duration * time.Duration(retries+1)
			if b > conf.Connector.MaxBackOff.Duration {
				b = conf.Connector.MaxBackOff.Duration
			}
			return conf.Connector.MaxBackOff.Duration
		}
		c.ClientID = version.APP
		c.Metadata.Full = conf.Connector.FetchMetadata
		c.Metadata.Retry.Max = conf.Connector.Retry
		c.Metadata.Retry.BackoffFunc = backoff
		conn.cfg = c
		err = conn.CreateTopic()
		if err != nil {
			return nil, err
		}

		if conf.Connector.DebugProducer {
			c.Producer.Return.Successes = true
		}
		c.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
		c.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
		c.Producer.Retry.Max = conf.Connector.Retry
		c.Producer.Retry.BackoffFunc = backoff
		producer, err := sarama.NewAsyncProducer(conf.Connector.BrokerList, c)
		if err != nil {
			l.Errorf("Failed to start producer, %s", err)
			return nil, err
		}
		conn.producer = producer
		go conn.runProducer()
	}
	return conn, nil
}

func (c *Connector) CreateTopic() error {
	if !c.cfg.Version.IsAtLeast(sarama.V1_1_0_0) {
		return nil
	}

	admin, err := sarama.NewClusterAdmin(c.conf.Connector.BrokerList, c.cfg)
	if err != nil {
		c.log.Errorf("create cluster admin failed, %s", err)
		return err
	}
	topics, err := admin.ListTopics()
	if err != nil {
		c.log.Errorf("list topics failed, %s", err)
		return err
	}
	topicDetail, exist := topics[c.conf.Connector.Topic]
	if exist {
		c.log.Infof("get topic %s, partition_num %d, config partition_num %d",
			c.conf.Connector.Topic, topicDetail.NumPartitions, c.conf.Connector.PartitionNum)
	} else {
		c.log.Infof("create topic %s partition_num %d",
			c.conf.Connector.Topic, c.conf.Connector.PartitionNum)
		err := admin.CreateTopic(c.conf.Connector.Topic, &sarama.TopicDetail{
			NumPartitions:     c.conf.Connector.PartitionNum,
			ReplicationFactor: 3,
		}, false)
		if err != nil {
			return err
		}
	}
	err = admin.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *Connector) putQueue(msg store.KeyEntry) error {
	c.writeBuf.Reset()
	keyLen := uint32(len(msg.Key))
	err := binary.Write(&c.writeBuf, binary.BigEndian, keyLen)
	if err != nil {
		c.log.Errorf("buffer write failed, %s", err)
		return err
	}
	_, err = c.writeBuf.Write(msg.Key)
	if err != nil {
		return err
	}
	_, err = c.writeBuf.Write(msg.Entry)
	if err != nil {
		return err
	}
	return c.queue.Put(c.writeBuf.Bytes())
}

func (c *Connector) runQueue() {
	timer := time.NewTimer(c.conf.Connector.WriteTimeout.Duration)
	for {
		select {
		case msg, ok := <-c.writeChan:
			if !ok {
				return
			}
			err := c.putQueue(msg)
			if err != nil {
				c.log.Errorf("put queue failed, %s", err)
				if c.producer == nil {
					continue
				}

				input := &sarama.ProducerMessage{
					Topic: c.conf.Connector.Topic,
					Key:   sarama.ByteEncoder(msg.Key),
					Value: sarama.ByteEncoder(msg.Entry),
				}

				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(c.conf.Connector.WriteTimeout.Duration)
				select {
				case c.producer.Input() <- input:
				case <-timer.C:
					c.log.Errorf("put kafka timeout, %s", msg.Key)
				}
			}
		}
	}
}

func (c *Connector) runProducer() {
	for {
		select {
		case success, ok := <-c.producer.Successes():
			if !ok {
				return
			}
			logrus.Debugf("key %s, partition %d, offset %d",
				success.Key, success.Partition, success.Offset)
		case err, ok := <-c.producer.Errors():
			if !ok {
				return
			}
			c.log.Errorf("producer failed, %s", err)
		case body, ok := <-c.queue.ReadChan():
			if !ok {
				return
			}
			keyLen := binary.BigEndian.Uint32(body[:4])
			c.producer.Input() <- &sarama.ProducerMessage{
				Topic: c.conf.Connector.Topic,
				Key:   sarama.ByteEncoder(body[4 : keyLen+4]),
				Value: sarama.ByteEncoder(body[keyLen+4:]),
			}
		}
	}
}

func (c *Connector) Send(msg store.KeyEntry) error {
	c.writeChan <- msg
	return nil
}

func (c *Connector) Close() {
	close(c.writeChan)
	err := c.queue.Close()
	if err != nil {
		c.log.Errorf("queue close failed, %s", err)
	}
	if c.producer == nil {
		return
	}
	err = c.producer.Close()
	if err != nil {
		c.log.Errorf("producer close failed, %s", err)
	}
}
