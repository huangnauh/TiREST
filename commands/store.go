package commands

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"gitlab.s.upyun.com/platform/tikv-proxy/server"
	"gitlab.s.upyun.com/platform/tikv-proxy/store"
	"gitlab.s.upyun.com/platform/tikv-proxy/utils"
	"strconv"
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
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "raw key",
				Value: true,
			},
			&cli.UintFlag{
				Name:    "verbose",
				Aliases: []string{"vb"},
				Usage:   "verbose info(2 error, 3 warn, 4 info, 5 debug)",
				Value:   2,
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
					return runKVPut(c)
				},
			},
			{
				Name:      "get",
				Usage:     "get the value for the key in the tikv store",
				ArgsUsage: "KEY VALUE",
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("invalid KEY VALUE")
					}
					return runKVGet(c)
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
					return runKVDelete(c)
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
						Value:   10,
					},
					&cli.BoolFlag{
						Name:    "reverse",
						Aliases: []string{"r"},
						Usage:   "reverse",
					},
				},
				Action: runKVList,
			},
		},
	})
}

func getStore(c *cli.Context) (*store.Store, error) {
	verbose := c.Uint("verbose")
	if verbose > 5 {
		return nil, errors.New("invalid verbose")
	}
	level := logrus.Level(verbose)
	logrus.SetLevel(level)
	configFile := c.String("config")
	conf, err := config.InitConfig(configFile)
	if err != nil {
		fmt.Printf("init config failed, err: %s\n", err)
		return nil, err
	}
	conf.Log.Level = level.String()
	return store.OnlyOpenDatabase(conf)
}

func unquote(s string) (string, error) {
	s, err := strconv.Unquote(`"` + s + `"`)
	return s, err
}

func runKVPut(c *cli.Context) error {
	key := c.Args().Get(0)
	value := c.Args().Get(1)
	raw := c.IsSet("raw")
	key, err := unquote(key)
	if err != nil {
		fmt.Printf("unquote key, err: %s\n", err)
		return err
	}
	meta, err := server.EncodeMetaKey(key, raw)
	if err != nil {
		fmt.Printf("encode key, err: %s\n", err)
		return err
	}
	fmt.Printf("encode key, %v\n", meta)
	s, err := getStore(c)
	if err != nil {
		return err
	}
	err = s.UnsafePut(meta, utils.S2B(value))
	if err != nil {
		fmt.Printf("put %s err: %s\n", meta, err)
		return err
	}
	return nil
}

func runKVDelete(c *cli.Context) error {
	key := c.Args().Get(0)
	raw := c.IsSet("raw")
	key, err := unquote(key)
	if err != nil {
		fmt.Printf("unquote key, err: %s\n", err)
		return err
	}
	meta, err := server.EncodeMetaKey(key, raw)
	if err != nil {
		fmt.Printf("encode key, err: %s\n", err)
		return err
	}
	fmt.Printf("encode key, %v\n", meta)
	s, err := getStore(c)
	if err != nil {
		return err
	}
	err = s.UnsafePut(meta, nil)
	if err != nil {
		fmt.Printf("del %s err: %s\n", meta, err)
		return err
	}
	return nil
}

func runKVGet(c *cli.Context) error {
	key := c.Args().Get(0)
	raw := c.IsSet("raw")
	key, err := unquote(key)
	if err != nil {
		fmt.Printf("unquote key, err: %s\n", err)
		return err
	}
	meta, err := server.EncodeMetaKey(key, raw)
	if err != nil {
		fmt.Printf("encode key, err: %s\n", err)
		return err
	}
	fmt.Printf("encode key, %v\n", meta)
	s, err := getStore(c)
	if err != nil {
		return err
	}
	v, err := s.Get(meta, server.DefaultGetOption())
	if err != nil {
		fmt.Printf("get %s err: %s\n", meta, err)
		return err
	}
	fmt.Printf("Value: %s\n", v.Value)
	return nil
}

func runKVList(c *cli.Context) error {
	raw := c.IsSet("raw")
	start := c.String("start")
	start, err := unquote(start)
	if err != nil {
		fmt.Printf("unquote start, err: %s\n", err)
		return err
	}

	end := c.String("end")
	end, err = unquote(end)
	if err != nil {
		fmt.Printf("unquote end, err: %s\n", err)
		return err
	}

	limit := c.Int("limit")
	reverse := c.Bool("reverse")

	st, err := server.EncodeMetaKey(start, raw)
	if err != nil {
		fmt.Printf("encode start, err: %s\n", err)
		return err
	}
	fmt.Printf("encode start, %v\n", st)
	en, err := server.EncodeMetaKey(end, raw)
	if err != nil {
		fmt.Printf("encode key, err: %s\n", err)
		return err
	}
	fmt.Printf("encode end, %v\n", en)
	s, err := getStore(c)
	if err != nil {
		return err
	}
	opt := server.DefaultListOption()
	opt.Reverse = reverse
	items, err := s.List(st, en, limit, opt)
	if err != nil {
		fmt.Printf("list %s-%s err: %s\n", st, en, err)
		return err
	}
	for _, item := range items {
		fmt.Printf("Key: %s\n", item.Key)
		fmt.Printf("Value: %s\n", item.Value)
	}
	return nil
}
