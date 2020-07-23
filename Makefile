APP=tikv-proxy
REPO_PATH=gitlab.s.upyun.com/platform/$(APP)
GIT_COMMIT=$(shell git rev-parse --short HEAD)
GIT_DESCRIBE=$(shell git describe --tags --always)
IMPORT=$(REPO_PATH)/version
GOLDFLAGS=-X $(IMPORT).GitCommit=$(GIT_COMMIT) -X $(IMPORT).GitDescribe=$(GIT_DESCRIBE)

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


tikv:
#	git checkout main.go
#	sed -i $(SED_EXTENSION) '/\/newtikv"/s/\/\///' main.go
	go build -tags=jsoniter -ldflags '$(GOLDFLAGS)' -o tikv-proxy main.go


.PHONY: tikv
