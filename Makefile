APP= tikv-proxy
GIT_COMMIT=$(shell git rev-parse --short HEAD)
GIT_DESCRIBE=$(shell git describe --tags --always)
IMPORT=$(APP)/pkg/version
GOLDFLAGS=-X $(IMPORT).GitCommit=$(GIT_COMMIT) -X $(IMPORT).GitDescribe=$(GIT_DESCRIBE)

ifeq ($(shell uname -s), Darwin)
PLAT=osx
EXTENSION=""
else
PLAT=linux
endif

tikv:
	sed -i $(EXTENSION) '/\/tikv"/s/\/\///' main.go
	go build -o tikv-proxy main.go


newtikv:
	sed -i $(EXTENSION) '/\/newtikv"/s/\/\///' main.go
	go build -o tikv-proxy main.go


.PHONY: tikv newtikv
