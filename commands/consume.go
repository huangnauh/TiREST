package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/huangnauh/tirest/config"
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
			&cli.IntFlag{
				Name:    "limit",
				Aliases: []string{"l"},
				Usage:   "limit consumer",
				Value:   10,
			},
		},
		Action: runConsumer,
	})
}

func runConsumer(c *cli.Context) error {
	oldest := c.Bool("oldest")
	group := c.String("group")
	limit := c.Int("limit")
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
		logrus.Errorf("Error parsing version: %v", err)
		return err
	}
	logrus.Infof("version %s", cf.Version)
	if oldest {
		cf.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	consumer := Consumer{
		done:  make(chan struct{}),
		limit: limit,
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
	logrus.Infof("consumer up and running!...")
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		logrus.Info("terminating: context cancelled")
	case <-sigterm:
		logrus.Info("terminating: via signal")
	}
	cancel()
	if err = client.Close(); err != nil {
		logrus.Errorf("Error closing client: %v", err)
		return err
	}
	return nil
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	done    chan struct{}
	limit   int
	consume int
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
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
		if consumer.consume >= consumer.limit {
			break
		}
		consumer.consume++
		logrus.Infof("Message claimed: key %s, partition = %d, offset = %d, value = %s",
			message.Key, message.Partition, message.Offset, message.Value)
		session.MarkMessage(message, "")
	}

	return nil
}
