package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/huangnauh/tirest/commands"
	_ "github.com/huangnauh/tirest/store/kafka"
	_ "github.com/huangnauh/tirest/store/newtikv"
	//_ "github.com/huangnauh/tirest/store/tikv"
	"github.com/huangnauh/tirest/version"
	"os"
	"runtime"
	"sort"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	app := cli.NewApp()
	app.Version = fmt.Sprintf("%s (%s), api:%s, runtime:%s/%s %s", version.GitDescribe,
		version.GitCommit, version.API,
		runtime.GOOS, runtime.GOARCH, runtime.Version())
	app.Usage = "tikv proxy"
	app.Commands = commands.AllCommands()
	app.EnableBashCompletion = true
	app.Flags = commands.AllFlags()
	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err := app.Run(os.Args)
	if err != nil {
		return 1
	}
	return 0
}
