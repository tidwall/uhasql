#!/bin/bash

set -e
cd $(dirname "${BASH_SOURCE[0]}")/..

. scripts/env.sh

if [[ "$DOCKER_LOGIN" == "" ]]; then 
	echo DOCKER_LOGIN is not set
	exit 1
fi

# export DOCKER_REPO=$DOCKER_USER/tile38

# # GIT_VERSION - always the last verison number, like 1.12.1.
# export GIT_VERSION=$(git describe --tags --abbrev=0)  
# # GIT_COMMIT_SHORT - the short git commit number, like a718ef0.
# export GIT_COMMIT_SHORT=$(git rev-parse --short HEAD)
# # DOCKER_REPO - the base repository name to push the docker build to.
# export DOCKER_REPO=$DOCKER_USER/tile38

# if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then 
# 	# never push from a pull request
# 	echo "Not pushing, on a PR or not running in Travis CI"
# elif [ "$TRAVIS_BRANCH" != "master" ]; then
# 	# only the master branch will work
# 	echo "Not pushing, not on master"
# else
# 	push(){
# 		docker tag $DOCKER_REPO:$GIT_COMMIT_SHORT $DOCKER_REPO:$1
# 		docker push $DOCKER_REPO:$1
# 		echo "Pushed $DOCKER_REPO:$1"
# 	}
# 	# docker login
# 	echo $DOCKER_PASSWORD | docker login -u $DOCKER_LOGIN --password-stdin
# 	# build the docker image
# 	docker build -f Dockerfile -t $DOCKER_REPO:$GIT_COMMIT_SHORT .
# 	if [ "$(curl -s https://hub.docker.com/v2/repositories/$DOCKER_REPO/tags/$GIT_VERSION/ | grep "digest")" == "" ]; then
# 		# push the newest tag
# 		push "$GIT_VERSION"
# 		push "latest"
# 	fi
# 	push "edge"
# fi
