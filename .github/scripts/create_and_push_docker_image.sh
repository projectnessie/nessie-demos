#!/bin/bash
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

INPUT_IMAGE_NAME=$1
INPUT_IMAGE_TAG=$2
INPUT_REPO_DIR=$3
INPUT_NOTEBOOK_USER=$4

# Check for docker image name
if [ -z "$INPUT_IMAGE_NAME" ]; then
  echo "Input the image name for the docker image as the first parameter"
  exit 1
fi

# Check for docker tag
if [ -z "$INPUT_IMAGE_TAG" ]; then
  echo "Input the image tag for the docker image as the second parameter"
  exit 1
fi

# Check for repo dir
if [ -z "$INPUT_REPO_DIR" ]; then
  echo "Input the repo dir to be used to build docker image as the third parameter"
  exit 1
fi

# Set jupyter username
if [ -z "$INPUT_NOTEBOOK_USER" ];
    then
        # The default username being used by mybinder.org is 'jovyan'
        # It is used by mybinder.org to build and run docker files
        # therefore it expects the /home/jovyan to be existed in order to run the docker image
        NB_USER="jovyan"

    else
        NB_USER="${INPUT_NOTEBOOK_USER}"
fi

# Set Docker image full name
DOCKER_FULL_IMAGE_NAME="${INPUT_IMAGE_NAME}:${INPUT_IMAGE_TAG}"

# Build and push docker image
jupyter-repo2docker --image-name "${DOCKER_FULL_IMAGE_NAME}" --no-run --push --user-id 1000 --user-name "${NB_USER}" ${INPUT_REPO_DIR}

# Tag our image with "latest" in case we need to use it in other cases like running unit tests
docker tag "${DOCKER_FULL_IMAGE_NAME}" "${INPUT_IMAGE_NAME}:latest"
docker push "${INPUT_IMAGE_NAME}:latest"

# Set the output docker tag we have from here
echo "::set-output name=image_tag::${INPUT_IMAGE_TAG}"
