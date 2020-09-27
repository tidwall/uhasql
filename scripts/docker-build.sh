#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

. scripts/env.sh

cat scripts/Dockerfile.tmpl \
    | sed "s/{{GITVERS}}/${GITVERS}/" \
    | sed "s/{{GITSHA}}/${GITSHA}/" \
    > scripts/Dockerfile.out

docker build -t tidwall/uhasql:${GITSHA} -f scripts/Dockerfile.out .
