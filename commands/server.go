package commands

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/log"
	"gitlab.s.upyun.com/platform/tikv-proxy/middleware"
	"gitlab.s.upyun.com/platform/tikv-proxy/server"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	registerCommand(cli.Command{
		Name:  "server",
		Usage: "Run as server",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "config, conf",
				Usage: "proxy config",
			},
		},
		Action: func(c *cli.Context) error {
			return runServer(c)
		},
	})
}

func runServer(c *cli.Context) error {
	configFile := c.String("config")
	conf, err := config.InitConfig(configFile)
	if err != nil {
		logrus.Errorf("init config failed, err: %s", err)
		return err
	}

	level, err := logrus.ParseLevel(conf.Log.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})
	if conf.Log.ErrorLogDir != "" {
		output, err := log.NewRotatingOuter(
			conf.Log.ErrorLogDir, conf.Log.BufferSize, conf.Log.MaxBytes, conf.Log.BackupCount)
		if err != nil {
			logrus.Errorf("init error log failed, err: %s", err)
			return err
		} else {
			logrus.SetOutput(output)
		}
	} else {
		logrus.SetOutput(os.Stderr)
	}

	if conf.Log.AccessLogDir != "" {
		err = middleware.InitLog(conf.Log.AccessLogDir, conf.Log.BufferSize, conf.Log.MaxBytes, conf.Log.BackupCount)
		if err != nil {
			logrus.Errorf("init access log failed, err: %s", err)
			return err
		}
	}

	s, err := server.NewServer(conf)
	if err != nil {
		logrus.Errorf("new server, err: %s", err)
		return err
	}

	go s.Start()
	signalCh := make(chan os.Signal, 10)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-signalCh
	fmt.Printf("Received signal %s, clean up and exit...\n", sig)
	fmt.Printf("stop api...\n")
	s.Close()
	return nil
}