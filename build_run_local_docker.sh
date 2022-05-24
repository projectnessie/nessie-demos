#!/bin/bash -e
#
# Copyright (C) 2022 Dremio
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

if ! command -v jupyter-repo2docker >/dev/null; then
  echo "Please prepare a python environment with jupyter-repo2docker!"
  exit 1
fi

set -x

# calculate latest base image tag
bash -ex .github/scripts/modify_dockerfile.sh

# avoid remote repository for the rest of the script
sed -i.bak 's#FROM ghcr.io/projectnessie#FROM projectnessie#' binder/Dockerfile
trap "sed -i.bak 's#FROM projectnessie#FROM ghcr.io/projectnessie#' binder/Dockerfile && git add binder/Dockerfile" EXIT

DOCKER_FULL_IMAGE_NAME=$(grep -i 'FROM' binder/Dockerfile | cut -f2 -d" ")
if docker image inspect "${DOCKER_FULL_IMAGE_NAME}" >/dev/null 2>&1; then
  echo "base image tag already exists - nothing to build"
else
  # build base image with the new tag locally
  echo "base image tag has changed! - starting build..."
  bash -ex .github/scripts/create_base_docker_image.sh "${DOCKER_FULL_IMAGE_NAME}"
fi

# build and run repo
jupyter-repo2docker --user-id 1000 --user-name jovyan .
