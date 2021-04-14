#!/bin/bash

if [[ -z $BUILD_DOCKER ]]; then
   coveralls-lcov --repo-token "$COVERALLS_REPO_TOKEN" --service-name travis-pro ./build/merged.info
else
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USERNAME --password-stdin
   docker tag koinos-p2p koinos/koinos-p2p:$TAG
   docker push koinos/koinos-p2p:$TAG
fi
