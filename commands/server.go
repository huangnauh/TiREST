package commands

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
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
}
