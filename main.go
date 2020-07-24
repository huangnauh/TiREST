package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"gitlab.s.upyun.com/platform/tikv-proxy/commands"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/kafka"
	_ "gitlab.s.upyun.com/platform/tikv-proxy/store/newtikv"
	//_ "gitlab.s.upyun.com/platform/tikv-proxy/store/tikv"
	"gitlab.s.upyun.com/platform/tikv-proxy/version"
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
