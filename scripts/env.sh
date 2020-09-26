#!/bin/bash

# vers.sh -- export UhaSQL Version and GitSHA environment variables

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

GITVERS=${GITVERS-$(git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)}
GITSHA=${GITSHA-$(git rev-parse --short HEAD 2>/dev/null || echo 0000000)}

export GITVERS
export GITSHA
