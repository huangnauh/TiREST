PROG=tikv-proxy
REPO_PATH=gitlab.s.upyun.com/platform/$(PROG)
GIT_COMMIT=$(shell git rev-parse --short HEAD)
GIT_DESCRIBE=$(shell git describe --tags --always)
IMPORT=$(REPO_PATH)/version
GOLDFLAGS=-X $(IMPORT).GitCommit=$(GIT_COMMIT) -X $(IMPORT).GitDescribe=$(GIT_DESCRIBE)

WORK_DIR=$(shell pwd)

ifeq ($(shell uname -s), Darwin)
PLAT=osx
SED_EXTENSION=""
else
PLAT=linux
endif

#tikv:
#	git checkout main.go
#	sed -i $(SED_EXTENSION) '/\/tikv"/s/\/\///' main.go
#	go build -tags=jsoniter -ldflags '$(GOLDFLAGS)' -o tikv-proxy main.go


app:
#	git checkout main.go
#	sed -i $(SED_EXTENSION) '/\/newtikv"/s/\/\///' main.go
	go build -tags=jsoniter -ldflags '$(GOLDFLAGS)' -o bin/tikv-proxy$(GOOS) main.go

lint:
	revive -config ./revive.toml -formatter friendly ./...

tool:
	go build tools/tikv-assembly.go

test: lint
	go test -tags=jsoniter -v $(REPO_PATH)/... --conf=$(WORK_DIR)/example/server.toml

.PHONY: tikv test lint
