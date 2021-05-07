#!/usr/bin/env bash
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

# Script that generates the URLs for a notebook in this repo.
# Run this script and pass the path to the Jupyter notebook as the argument. Example:
#    ./generate-notebook-urls.sh colab/try-it.ipynb

set -e

FILE=$1
GIT_REF="$(git rev-parse --abbrev-ref HEAD)"

if [[ ! -f ${FILE} || ! ${FILE} =~ .*[.]ipynb ]]; then
  echo "File argument ${FILE} is not a valid file"
  exit 1
fi

REL_FILEPATH="$(realpath --relative-to $(dirname $0) "${FILE}")"
FILEPATH_PARAM="$(echo "${REL_FILEPATH}" | sed 's/[/]/%2F/g')"

echo "Google Colaboratory URL:"
echo "        https://colab.research.google.com/github/snazy/nessie-demos/blob/${GIT_REF}/${REL_FILEPATH}"
echo ""
echo "Binder URL (mybinder.org):"
echo "        https://mybinder.org/v2/gh/snazy/nessie-demos/${GIT_REF}?filepath=${FILEPATH_PARAM}"
