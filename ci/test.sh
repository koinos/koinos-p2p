#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go test -v github.com/koinos/koinos-p2p/internal -coverprofile=./build/internal.out -coverpkg=./internal/...
   go test -v github.com/koinos/koinos-p2p/internal/node -coverprofile=./build/node.out -coverpkg=./internal/...
   go test -v github.com/koinos/koinos-p2p/internal/p2p -coverprofile=./build/p2p.out -coverpkg=./internal/...

   gcov2lcov -infile=./build/internal.out -outfile=./build/internal.info
   gcov2lcov -infile=./build/node.out -outfile=./build/node.info
   gcov2lcov -infile=./build/p2p.out -outfile=./build/p2p.info

   lcov -a ./build/internal.info -a ./build/node.info -a ./build/p2p.info -o ./build/merged.info

   golint -set_exit_status ./...
else
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   export P2P_TAG=$TAG

   git clone https://github.com/koinos/koinos-integration-tests.git

   cd koinos-integration-tests
   go get ./...
   ./run.sh
fi
