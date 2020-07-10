package commands

import "github.com/urfave/cli/v2"

func init() {
	registerCommand(&cli.Command{
		Name:  "clean",
		Usage: "clean tikv",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "start",
				Aliases: []string{"s"},
				Usage:   "start key",
			},
			&cli.StringFlag{
				Name:    "end",
				Aliases: []string{"e"},
				Usage:   "end key",
			},
		},
		Action: runInit,
	})
}
