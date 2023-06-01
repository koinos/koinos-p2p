#!/bin/bash

if [[ -z $BUILD_DOCKER ]]; then
   coveralls-lcov --repo-token "$COVERALLS_REPO_TOKEN" --service-name travis-pro ./build/merged.info
else
   if [ "$TRAVIS_PULL_REQUEST_BRANCH" != "" ]; then
      exit 0
   fi

   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USERNAME --password-stdin
   docker push koinos/koinos-p2p:$TAG
fi
