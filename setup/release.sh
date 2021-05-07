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

# Prepare:
#   for testpypi:
#     keyring set https://test.pypi.org/legacy/ <YOUR_TEST_PYPI_USERNAME>
#   for pypi:
#     keyring set https://upload.pypi.org/legacy/ <YOUR_PYPI_USERNAME>

export TWINE_USERNAME=$(id -un)

set -e

cd $(dirname $0)

rm -rf dist build nessiedemo.egg-info .eggs

pip install twine build

bump2version --no-commit --no-tag patch

python3 -m build

twine check dist/*

git commit -am "nessiedemo testpypi release"

python3 -m twine upload --repository testpypi dist/*
