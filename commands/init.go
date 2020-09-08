package commands

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/huangnauh/tirest/config"
	"github.com/huangnauh/tirest/xerror"
)

func init() {
	registerCommand(&cli.Command{
		Name:  "init",
		Usage: "init config",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"conf"},
				Usage:   "init config path",
			},
		},
		Action: runInit,
	})
}

func runInit(c *cli.Context) error {
	path := c.String("config")
	_, err := os.Stat(path)
	if err == nil {
		logrus.Errorf("file exist, %s", path)
		return xerror.ErrExists
	}
	if !os.IsNotExist(err) {
		logrus.Errorf("path %s err, %s", path, err)
		return err
	}
	return config.Save(config.DefaultConfig(), path)
}
