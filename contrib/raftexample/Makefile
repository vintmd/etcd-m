default: build
all: build
.PHONY: docker

# 构建镜像
build:
	env GOOS=linux GOARCH=amd64 CGO_ENABLED=0  GO111MODULE=off go build -o raftexample
