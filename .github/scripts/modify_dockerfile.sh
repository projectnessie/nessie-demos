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

# Install checksumdir to generate a reliable sha1 for Docker tags that
# can detect the changes in content in a folder
python -m pip install checksumdir

CONTENT_FOLDER="docker/"
BINDER_DOCKERFILE="binder/Dockerfile"

# Generate the sha1 for the content of the folder
DOCKER_TAG="$(checksumdir -i -e '__pycache__' -a sha1 $CONTENT_FOLDER)"

# Change the tag in Dockerfile
sed -i -E "s/(FROM.*:).*/\1$DOCKER_TAG/" $BINDER_DOCKERFILE

git add $BINDER_DOCKERFILE
