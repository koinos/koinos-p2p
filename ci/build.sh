#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go get ./...
   mkdir -p build
   go build -ldflags="-X main.Commit=$(git rev-parse HEAD)" -o build/koinos_p2p cmd/koinos-p2p/main.go
else
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USERNAME --password-stdin
   docker build . -t koinos/koinos-p2p:$TAG
fi
