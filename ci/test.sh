#!/bin/bash

set -e
set -x

go test -v github.com/koinos/koinos-p2p/internal -coverprofile=./build/internal.out -coverpkg=./internal/...
go test -v github.com/koinos/koinos-p2p/internal/node -coverprofile=./build/node.out -coverpkg=./internal/...

gcov2lcov -infile=./build/internal.out -outfile=./build/internal.info
gcov2lcov -infile=./build/node.out -outfile=./build/node.info

lcov -a ./build/internal.info -a ./build/node.info -o ./build/merged.info

golint -set_exit_status ./...
