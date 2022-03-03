#!/bin/bash
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

if ! python -m pip list | grep jupyter-repo2docker; then
  echo "Please prepare a python environment with jupyter-repo2docker!"
  exit 1
fi

get_current_base_image_name () {
  grep -i 'FROM' binder/Dockerfile | cut -f 2 -d ' '
}

set -x

# calculate latest base image tag
OLD_BASE_IMAGE_NAME=$(get_current_base_image_name)
bash -ex .github/scripts/modify_dockerfile.sh
NEW_BASE_IMAGE_NAME=$(get_current_base_image_name)

# avoid remote repository for the rest of the script
sed -i 's#FROM ghcr.io/projectnessie#FROM projectnessie#' binder/Dockerfile
trap "sed -i 's#FROM projectnessie#FROM ghcr.io/projectnessie#' binder/Dockerfile" EXIT

if [ "${OLD_BASE_IMAGE_NAME}" != "${NEW_BASE_IMAGE_NAME}" ]; then
  # build base image with the new tag locally
  echo "base image tag has changed! - starting build..."
  REPO_FREE_BASE_IMAGE_NAME=$(get_current_base_image_name)
  jupyter-repo2docker --user-id 1000 --user-name nessiedemo --image-name "${REPO_FREE_BASE_IMAGE_NAME}" --no-run docker
else
  echo "base image tag did not change - nothing to build"
fi

# build and run repo
jupyter-repo2docker --user-id 1000 --user-name nessiedemo .
