#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa
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
"""The setup script."""
from setuptools import find_packages
from setuptools import setup
import os

with open("README.rst") as readme_file:
    readme = readme_file.read()

if os.path.exists("requirements.txt"):
    with open("HISTORY.rst") as history_file:
        history = history_file.read()
else:
    history = ""

if os.path.exists("requirements.txt"):
    with open("requirements.txt") as fp:
        requirements = fp.read()
else:
    requirements = ""

setup_requirements = ["pytest-runner", "pip"]

setup(
    author="Robert Stupp",
    author_email="nessie-release-builder@dremio.com",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="Project Nessie Demos Helper",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + "\n" + history,
    include_package_data=True,
    keywords="nessiedemo",
    name="nessiedemo",
    packages=find_packages(include=["nessiedemo", "nessiedemo.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=[],
    url="https://github.com/projectnessie/nessie",
    version="0.0.19",
    zip_safe=False,
)
