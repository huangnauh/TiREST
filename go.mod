module gitlab.s.upyun.com/platform/tikv-proxy

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DeanThompson/ginpprof v0.0.0-20190408063150-3be636683586
	github.com/Shopify/sarama v1.26.4
	github.com/gin-gonic/gin v1.6.3
	github.com/golang/protobuf v1.3.4
	github.com/google/gopacket v1.1.17
	github.com/json-iterator/go v1.1.9
	github.com/mozillazg/go-httpheader v0.2.1
	github.com/nsqio/go-diskqueue v1.0.0
	github.com/pingcap/kvproto v0.0.0-20200518112156-d4aeb467de29
	github.com/pingcap/pd/v4 v4.0.0-rc.2.0.20200520083007-2c251bd8f181
	github.com/pingcap/tidb v1.1.0-beta.0.20200604055950-efc1c154d098
	github.com/prometheus/client_golang v1.5.1
	github.com/sirupsen/logrus v1.6.1-0.20200528085638-6699a89a232f
	github.com/stretchr/testify v1.5.1
	github.com/tikv/client-go v0.0.0-20200513031230-7253be23eb15
	github.com/urfave/cli/v2 v2.1.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	golang.org/x/sys v0.0.0-20200808120158-1030fc2bf1d9 // indirect
)

replace github.com/sirupsen/logrus v1.6.1-0.20200528085638-6699a89a232f => github.com/huangnauh/logrus v1.6.2
