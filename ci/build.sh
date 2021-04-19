#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go get ./...
   mkdir -p build
   go build -o build/koinos_p2p cmd/koinos-p2p/main.go
else
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   docker build . -t koinos/koinos-p2p:$TAG
fi
