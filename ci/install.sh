#!/bin/bash

if [[ -z $BUILD_DOCKER ]]; then
   sudo apt-get install -y lcov ruby
   sudo gem install coveralls-lcov
   go get -u github.com/jandelgado/gcov2lcov
   go get -u golang.org/x/lint/golint
else
   sudo curl -L "https://github.com/docker/compose/releases/download/1.29.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
   sudo chmod +x /usr/local/bin/docker-compose
   docker-compose --version
fi
