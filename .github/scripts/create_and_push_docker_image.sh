#!/bin/bash -e
#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

INPUT_DOCKER_FULL_IMAGE_NAME=$1
INPUT_PUSH=$2

# ghcr.io/projectnessie/nessie-binder-demos:e12478cfb3447af4bc26e535a9daffe374233aa1
DOCKER_IMAGE_NAME=$(echo "${INPUT_DOCKER_FULL_IMAGE_NAME}" | cut -f1 -d":")
DOCKER_IMAGE_TAG=$(echo "${INPUT_DOCKER_FULL_IMAGE_NAME}" | cut -f2 -d":")

if [ "$INPUT_PUSH" = 'push' ]; then
  PUSH_ARGS='--push'
else
  PUSH_ARGS=''
fi

# The default username being used by mybinder.org is 'jovyan'
# It is used by mybinder.org to build and run docker files
# therefore it expects /home/jovyan to exist in order to run the docker image

# Build the docker base image
jupyter-repo2docker --user-id 1000 --user-name jovyan --image-name "${INPUT_DOCKER_FULL_IMAGE_NAME}" --no-run ${PUSH_ARGS} docker

if [ "$INPUT_PUSH" = 'push' ]; then
  # Tag our image with "latest" in case we need to use it in other cases like running unit tests
  docker tag "${INPUT_DOCKER_FULL_IMAGE_NAME}" "${DOCKER_IMAGE_NAME}:latest"
  docker push "${DOCKER_IMAGE_NAME}:latest"
fi
