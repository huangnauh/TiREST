package commands

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/server"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/newtikv"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
)

func init() {
	registerCommand(&cli.Command{
		Name:  "store",
		Usage: "ctl tikv store",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "proxy config",
				Value:   "./server.toml",
			},
			//&cli.StringFlag{
			//	Name:    "store-type",
			//	Aliases: []string{"t"},
			//	Usage:   "store type",
			//	Value:   "meta",
			//},
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "raw key",
			},
		},
		Subcommands: []*cli.Command{
			{
				Name:      "put",
				Usage:     "put the data to the given in the key tikv store",
				ArgsUsage: "KEY VALUE",
				Action: func(c *cli.Context) error {
					if c.NArg() != 2 {
						return errors.New("invalid KEY VALUE")
					}
					if err := runKVPut(c); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:      "get",
				Usage:     "get the value for the key in the tikv store",
				ArgsUsage: "KEY VALUE",
				Action: func(c *cli.Context) error {
					if c.NArg() != 2 {
						return errors.New("invalid KEY VALUE")
					}
					if err := runKVGet(c); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:      "delete",
				Usage:     "Delete the value for the key in the tikv store",
				ArgsUsage: "KEY",
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("invalid KEY")
					}
					if err := runKVDelete(c); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:  "list",
				Usage: "list the keys in the key-value store",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "start",
						Aliases: []string{"s"},
						Usage:   "start",
					},
					&cli.StringFlag{
						Name:    "end",
						Aliases: []string{"e"},
						Usage:   "end",
					},
					&cli.IntFlag{
						Name:    "limit",
						Aliases: []string{"l"},
						Usage:   "limit",
					},
					&cli.BoolFlag{
						Name:    "reverse",
						Aliases: []string{"r"},
						Usage:   "reverse",
					},
				},
				Action: func(c *cli.Context) error {
					if err := runKVList(c); err != nil {
						return err
					}
					return nil
				},
			},
		},
	})
}

func getStore(c *cli.Context) (*store.Store, error) {
	configFile := c.String("config")
	conf, err := config.InitConfig(configFile)
	if err != nil {
		logrus.Errorf("init config failed, err: %s", err)
		return nil, err
	}
	conf.Log.ErrorLogDir = ""
	conf.Log.AccessLogDir = ""
	s, err := store.NewStore(conf)
	if err != nil {
		return nil, err
	}
	err = s.OpenDatabase()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func runKVPut(c *cli.Context) error {
	key := c.Args().Get(0)
	value := c.Args().Get(1)
	raw := c.IsSet("raw")
	meta, err := server.EncodeMetaKey(key, raw)
	if err != nil {
		return err
	}
	s, err := getStore(c)
	if err != nil {
		return err
	}
	return s.UnsafePut(meta, utils.S2B(value))
}

func runKVDelete(c *cli.Context) error {
	key := c.Args().Get(0)
	raw := c.IsSet("raw")
	meta, err := server.EncodeMetaKey(key, raw)
	if err != nil {
		return err
	}
	s, err := getStore(c)
	if err != nil {
		return err
	}
	return s.UnsafePut(meta, nil)
}

func runKVGet(c *cli.Context) error {
	key := c.Args().Get(0)
	raw := c.IsSet("raw")
	meta, err := server.EncodeMetaKey(key, raw)
	if err != nil {
		return err
	}
	s, err := getStore(c)
	if err != nil {
		return err
	}
	v, err := s.Get(meta, server.DefaultGetOption())
	if err != nil {
		return err
	}
	fmt.Printf("Value: %s\n", v.Value)
	return nil
}

func runKVList(c *cli.Context) error {
	raw := c.IsSet("raw")
	start := c.String("start")
	end := c.String("end")
	limit := c.Int("limit")
	reverse := c.Bool("reverse")

	st, err := server.EncodeMetaKey(start, raw)
	if err != nil {
		return err
	}
	en, err := server.EncodeMetaKey(end, raw)
	if err != nil {
		return err
	}
	s, err := getStore(c)
	if err != nil {
		return err
	}
	opt := server.DefaultListOption()
	opt.Reverse = reverse
	items, err := s.List(st, en, limit, opt)
	if err != nil {
		return err
	}
	for _, item := range items {
		fmt.Printf("Key: %s, Value: %s\n", item.Key, item.Value)
	}
	return nil
}
