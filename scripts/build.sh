#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

. scripts/env.sh

if [[ "$1" == "uhasql-server" ]]; then
    cd cmd/uhasql-server
    CGO_ENABLED=1 go build -ldflags "\
        -X main.buildVersion=$GITVERS \
        -X main.buildGitSHA=$GITSHA \
    " -o ../../uhasql-server main.go
elif [[ "$1" == "uhasql-cli" ]]; then
    cd cmd/uhasql-cli
    go build -o ../../uhasql-cli main.go
fi
