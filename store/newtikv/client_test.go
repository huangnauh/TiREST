package newtikv

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/huangnauh/tirest/config"
)

var (
	pathArg = flag.String("conf", "", "test conf path")
)

func newClientTest(disableLockBackOff bool) (client *TiKV) {
	flag.Parse()
	if *pathArg == "" {
		panic("need config file")
	}
	logrus.SetLevel(logrus.InfoLevel)
	conf, err := config.InitConfig(*pathArg)
	if err != nil {
		panic(err)
	}
	conf.Store.GCEnable = false
	conf.Store.Level = "info"
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
	d := "gfedcba"
	for i := 0; i < 10; i++ {
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
			execDetail := &execdetails.StmtExecDetails{}
			ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, execDetail)
			err = tx.Commit(ctx)
			t.Logf("%s %s", execDetailsString(execDetail), commitDetailsString(commitDetail))
			assert.Equal(t, commitDetail.CommitBackoffTime, int64(0))
			assert.Equal(t, commitDetail.WriteKeys, 1)

			snapshot := tx.GetSnapshot()
			snapshotStats := &tikv.SnapshotRuntimeStats{}
			snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
			_, _ = tx.Get(ctx, []byte(d))
			cancel()
			t.Logf("snapshot %s", snapshotStats)
		})
	}
}
