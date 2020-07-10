package commands

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	registerCommand(&cli.Command{
		Name:  "consume",
		Usage: "handle meta log",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "proxy config",
				Value:   "./server.toml",
			},
			&cli.BoolFlag{
				Name:    "oldest",
				Aliases: []string{"o"},
				Usage:   "consumer consume initial offset from oldest",
			},
			&cli.StringFlag{
				Name:    "group",
				Aliases: []string{"g"},
				Usage:   "consumer group definition",
				Value:   "test",
			},
		},
		Action: runConsumer,
	})
}

func runConsumer(c *cli.Context) error {
	oldest := c.Bool("oldest")
	group := c.String("group")
	configFile := c.String("config")
	conf, err := config.InitConfig(configFile)
	if err != nil {
		logrus.Errorf("init config failed, err: %s", err)
		return err
	}
	sarama.Logger = logrus.StandardLogger()
	cf := sarama.NewConfig()
	cf.ClientID = "tikv-consumer"

	cf.Version, err = sarama.ParseKafkaVersion(conf.Connector.Version)
	if err != nil {
		log.Panicf("Error parsing version: %v", err)
	}
	logrus.Infof("version %s", cf.Version)
	if oldest {
		cf.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if cf.Version.IsAtLeast(sarama.V0_10_2_0) {
		return runNewComsumerGroup(group, cf, conf)
	} else {
		return runOldComsumerGroup(group, cf, conf)
	}
}

func runOldComsumerGroup(group string, cf *sarama.Config, conf *config.Config) error {
	cc := cluster.NewConfig()
	cc.Config = *cf
	cc.Config.Consumer.Offsets.CommitInterval = time.Second
	consumer, err := cluster.NewConsumer(conf.Connector.BrokerList, group, []string{conf.Connector.Topic}, cc)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case message, ok := <-consumer.Messages():
			if ok {
				logrus.Infof("Message claimed: value = %s, partition = %v, offset = %v, topic = %s",
					string(message.Value), message.Partition, message.Offset, message.Topic)
				consumer.MarkOffset(message, "") // mark message as processed
			}
		case err := <-consumer.Errors():
			logrus.Errorf("Error from consumer: %v", err)
			return err
		case <-sigterm:
			return nil
		}
	}
	return nil
}

func runNewComsumerGroup(group string, cf *sarama.Config, conf *config.Config) error {
	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(conf.Connector.BrokerList, group, cf)
	if err != nil {
		logrus.Errorf("init consumer failed, err: %s", err)
		return err
	}
	go func() {
		for {
			if err := client.Consume(ctx, []string{conf.Connector.Topic}, &consumer); err != nil {
				logrus.Errorf("Error from consumer: %v", err)
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	<-consumer.ready
	logrus.Infof("consumer up and running!...")
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
	return nil
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		logrus.Infof("Message claimed: value = %s, partition = %v, offset = %v, topic = %s, timestamp %s",
			string(message.Value), message.Partition, message.Offset, message.Topic, message.Timestamp)
		session.MarkMessage(message, "")
	}

	return nil
}
