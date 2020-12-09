#!/bin/bash

# These commands can be removed once koinos-types-golang is public
echo -e "[url \"ssh://git@github.com/\"]\n   insteadOf = https://github.com/\n" >> ~/.gitconfig
export GOPRIVATE="`go env GOPRIVATE`,github.com/koinos/koinos-types-golang"

sudo gem install coveralls-lcov
go get -u github.com/jandelgado/gcov2lcov
go get -u golang.org/x/lint/golint
