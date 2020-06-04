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
	"time"
)

type Connector struct {
	producer  sarama.AsyncProducer
	log       *logrus.Entry
	queue     diskqueue.Interface
	writeBuf  bytes.Buffer
	writeChan chan store.KeyEntry
	conf      *config.Config
}

func Open(conf *config.Config) (store.Connector, error) {
	c := sarama.NewConfig()

	backoff := func(retries, maxRetries int) time.Duration {
		b := conf.Connector.BackOff * time.Duration(retries+1)
		if b > conf.Connector.MaxBackOff {
			b = conf.Connector.MaxBackOff
		}
		return conf.Connector.MaxBackOff
	}
	c.Metadata.Retry.Max = conf.Connector.Retry
	c.Metadata.Retry.BackoffFunc = backoff

	c.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	c.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	c.Producer.Retry.Max = conf.Connector.Retry
	c.Producer.Retry.BackoffFunc = backoff

	producer, err := sarama.NewAsyncProducer(conf.Connector.BrokerList, c)
	if err != nil {
		logrus.Errorf("Failed to start producer, %s", err)
		return nil, err
	}

	l := logrus.WithFields(logrus.Fields{
		"worker": "kafka connector",
	})
	queue := diskqueue.New(version.APP, conf.Connector.QueueDataPath,
		conf.Connector.MaxBytesPerFile, 4, conf.Connector.MaxMsgSize,
		conf.Connector.SyncEvery, conf.Connector.SyncTimeout, log.NewLogFunc(l))

	conn := &Connector{
		producer:  producer,
		queue:     queue,
		log:       l,
		writeChan: make(chan store.KeyEntry),
		conf:      conf,
	}
	go conn.runProducer()
	go conn.runQueue()
	return conn, nil
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
	for {
		select {
		case msg, ok := <-c.writeChan:
			if !ok {
				return
			}
			err := c.putQueue(msg)
			if err != nil {
				c.log.Errorf("put queue failed, %s", err)
				c.producer.Input() <- &sarama.ProducerMessage{
					Topic: c.conf.Connector.Topic,
					Key:   sarama.ByteEncoder(msg.Key),
					Value: sarama.ByteEncoder(msg.Entry),
				}
			}
		}
	}
}

func (c *Connector) runProducer() {
	for {
		select {
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
	err := c.producer.Close()
	if err != nil {
		c.log.Errorf("producer close failed, %s", err)
	}
	err = c.queue.Close()
	if err != nil {
		c.log.Errorf("queue close failed, %s", err)
	}
}
