#!/bin/bash

coveralls-lcov --repo-token "$COVERALLS_REPO_TOKEN" --service-name travis-pro ./build/merged.info
