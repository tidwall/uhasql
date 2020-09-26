#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

. scripts/env.sh

export DOCKER_APP=tidwall/uhasql

docker build \
    --build-arg version=$GITVERS \
    --build-arg gitsha=$GITSHA \
    -t $DOCKER_APP:$GITVERS \
    -f scripts/Dockerfile .

