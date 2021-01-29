#!/bin/bash

set -e
set -x

go test -v github.com/koinos/koinos-p2p/internal -coverprofile=./build/internal.out -coverpkg=./internal
gcov2lcov -infile=./build/internal.out -outfile=./build/internal.info

golint -set_exit_status ./...
