#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

. scripts/env.sh

docker build \
    --build-arg GITVERS=$GITVERS \
    --build-arg GITSHA=$GITSHA \
    -t tidwall/uhasql:${GITSHA} .
