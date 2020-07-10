package commands

import (
	"github.com/urfave/cli/v2"
)

var commands []*cli.Command

func registerCommand(cmd *cli.Command) {
	if commands == nil {
		commands = make([]*cli.Command, 0)
	}
	commands = append(commands, cmd)
}

func AllCommands() []*cli.Command {
	return commands
}

func AllFlags() []cli.Flag {
	return []cli.Flag{}
}
