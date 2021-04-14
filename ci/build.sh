#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go get ./...
   mkdir -p build
   go build -o build/koinos_p2p cmd/koinos-p2p/main.go
else
   docker build . -t koinos-p2p
fi
