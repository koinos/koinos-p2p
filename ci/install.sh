#!/bin/bash

sudo apt-get install -y lcov ruby
sudo gem install coveralls-lcov
go get -u github.com/jandelgado/gcov2lcov
go get -u golang.org/x/lint/golint
