#!/bin/bash

set -e
set -x

go get ./...
mkdir -p build
go build -o build/koinos-p2p cmd/koinos-p2p/main.go
