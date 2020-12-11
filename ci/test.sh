#!/bin/bash

set -e
set -x

go test -v github.com/koinos/koinos-p2p/internal/p2p -coverprofile=./build/p2p.out -coverpkg=./internal/p2p
gcov2lcov -infile=./build/p2p.out -outfile=./build/p2p.info

golint -set_exit_status ./...
