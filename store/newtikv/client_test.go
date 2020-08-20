package newtikv

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gitlab.s.upyun.com/platform/tikv-proxy/config"
	"testing"
	"time"
)

var (
	pathArg = flag.String("conf", "", "test conf path")
)

func newClientTest(disableLockBackOff bool) (client *TiKV) {
	flag.Parse()
	if *pathArg == "" {
		panic("need config file")
		return
	}
	logrus.SetLevel(logrus.DebugLevel)
	conf, err := config.InitConfig(*pathArg)
	if err != nil {
		panic(err)
	}
	conf.Store.GCEnable = false
	conf.Store.DisableLockBackOff = disableLockBackOff
	driver := Driver{}
	db, err := driver.Open(conf)
	if err != nil {
		panic(err)
	}
	client = db.(*TiKV)
	time.Sleep(time.Second)
	return
}

func TestClient(t *testing.T) {
	t.Parallel()
	client := newClientTest(true)
	b := "abcdefg"
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("cas_%d", i), func(t *testing.T) {
			t.Parallel()
			tx, err := client.client.Begin()

			var ignoreKill uint32
			disableLockVars := kv.NewVariables(&ignoreKill)
			disableLockVars.DisableLockBackOff = true
			tx.SetVars(disableLockVars)

			assert.Nil(t, err)
			err = tx.Set([]byte(b), []byte(fmt.Sprintf("{\"new\": \"%d\"}", i)))
			assert.Nil(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			var commitDetail *execdetails.CommitDetails
			ctx = context.WithValue(ctx, execdetails.CommitDetailCtxKey, &commitDetail)
			err = tx.Commit(ctx)
			cancel()
			assert.Equal(t, commitDetail.CommitBackoffTime, int64(0))
			assert.Equal(t, commitDetail.WriteKeys, 1)
		})
	}
}
