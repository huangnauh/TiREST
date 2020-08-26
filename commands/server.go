package commands

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/log"
	"gitlab.s.upyun.com/platform/tikv-proxy/middleware"
	"gitlab.s.upyun.com/platform/tikv-proxy/server"
)

func init() {
	registerCommand(&cli.Command{
		Name:  "server",
		Usage: "Run as server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"conf"},
				Usage:   "proxy config",
			},
		},
		Action: runServer,
	})
}

func runServer(c *cli.Context) error {
	configFile := c.String("config")
	conf, err := config.InitConfig(configFile)
	if err != nil {
		logrus.Errorf("init config failed, err: %s", err)
		return err
	}
	logrus.Infof("%s", conf)
	maxProcs := runtime.GOMAXPROCS(0)
	server.MaxProcs.Set(float64(maxProcs))
	logrus.Infof("GOMAXPROCS: %d", maxProcs)

	level, err := logrus.ParseLevel(conf.Log.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	//if level == logrus.DebugLevel {
	//	logrus.SetReportCaller(true)
	//}
	logrus.SetLevel(level)
	if conf.Log.ErrorLogDir != "" {
		hook, err := log.NewRotatingOuter(
			conf.Log.ErrorLogDir, conf.Log.BufferSize, conf.Log.MaxBytes, conf.Log.BackupCount,
			&logrus.TextFormatter{
				TimestampFormat: "2006-01-02 15:04:05",
				FullTimestamp:   true,
			})
		if err != nil {
			logrus.Errorf("init error log failed, err: %s", err)
			return err
		}
		defer hook.Close()

		logrus.SetEntryBufferDisable(true)
		logrus.SetFormatter(log.NullFormatter{})
		logrus.SetOutput(ioutil.Discard)
		logrus.AddHook(hook)
	} else {
		logrus.SetOutput(os.Stderr)
	}

	if conf.Log.AccessLogDir == "std" {
		err = middleware.InitLog("", conf.Log.BufferSize, conf.Log.MaxBytes, conf.Log.BackupCount)
	} else if conf.Log.AccessLogDir != "" {
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
