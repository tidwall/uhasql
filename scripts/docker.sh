#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

. scripts/env.sh

docker build \
    --build-arg GITVERS=$GITVERS \
    --build-arg GITSHA=$GITSHA \
    -t tidwall/uhasql:${GITSHA} .

if [[ "$1" == "--edge" ]]; then
    docker tag tidwall/uhasql:$GITSHA tidwall/uhasql:edge
    echo Successfully tagged tidwall/uhasql:edge
    docker push tidwall/uhasql:edge
    echo Successfully pushed tidwall/uhasql:edge
elif [[ "$1" == "--release" ]]; then
    docker tag tidwall/uhasql:$GITSHA tidwall/uhasql:edge
    echo Successfully tagged tidwall/uhasql:edge
    docker tag tidwall/uhasql:$GITSHA tidwall/uhasql:$GITVERS
    echo Successfully tagged tidwall/uhasql:$GITVERS
    docker tag tidwall/uhasql:$GITSHA tidwall/uhasql:latest
    echo Successfully tagged tidwall/uhasql:latest
    docker push tidwall/uhasql:edge
    echo Successfully pushed tidwall/uhasql:edge
    docker push tidwall/uhasql:$GITVERS
    echo Successfully pushed tidwall/uhasql:$GITVERS
    docker push tidwall/uhasql:latest
    echo Successfully pushed tidwall/uhasql:latest
fi