#!/usr/bin/env bash

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
