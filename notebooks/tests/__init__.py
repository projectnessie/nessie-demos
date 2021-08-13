#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
"""Unit tests for demo notebooks."""
import os
import subprocess
import tarfile
import shutil
import site
from typing import Optional

import requests


_SPARK_VERSION = "3.0.3"
_SPARK_FILENAME = f"spark-{_SPARK_VERSION}-bin-hadoop3.2"
_SPARK_URL = f"https://archive.apache.org/dist/spark/spark-{_SPARK_VERSION}/{_SPARK_FILENAME}.tgz"

_HADOOP_VERSION = "2.10.1"
_HADOOP_FILENAME = f"hadoop-{_HADOOP_VERSION}"
_HADOOP_URL = f"https://archive.apache.org/dist/hadoop/common/hadoop-{_HADOOP_VERSION}/{_HADOOP_FILENAME}.tar.gz"

_ICEBERG_VERSION = "0.12.0"
_ICEBERG_FLINK_FILENAME = f"iceberg-flink-runtime-{_ICEBERG_VERSION}.jar"
_ICEBERG_FLINK_URL = f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime/{_ICEBERG_VERSION}/{_ICEBERG_FLINK_FILENAME}"


def _find_notebook(notebook_file: str) -> str:
    path_to_notebook = os.path.join("notebooks", notebook_file)
    if not os.path.exists(path_to_notebook):
        path_to_notebook = os.path.join("..", path_to_notebook)
    if not os.path.exists(path_to_notebook):
        path_to_notebook = os.path.join("..", path_to_notebook)
    if not os.path.exists(path_to_notebook):
        raise Exception(
            f"Could not find {notebook_file} in {os.path.abspath('.')} and {os.path.abspath('..')}"
        )
    return os.path.abspath(path_to_notebook)


def _get_unzip(filename: str, url: str) -> None:
    if not os.path.exists(filename):
        response = requests.get(url, stream=True)
        file = tarfile.open(fileobj=response.raw, mode="r|gz")
        file.extractall(path=".")


def _get_spark() -> None:
    filename = _SPARK_FILENAME
    url = _SPARK_URL
    _get_unzip(filename, url)
    os.environ['SPARK_HOME'] = os.path.join(os.getcwd(), filename)


def _get_hadoop() -> None:
    filename = _HADOOP_FILENAME
    url = _HADOOP_URL
    _get_unzip(filename, url)
    os.environ['HADOOP_HOME'] = os.path.join(os.getcwd(), filename)


def _copy_all_hadoop_jars_to_pyflink() -> None:
    _get_hadoop()
    if not os.getenv("HADOOP_HOME"):
        raise Exception("The HADOOP_HOME env var must be set and point to a valid Hadoop installation")

    pyflink_lib_dir = _find_pyflink_lib_dir()
    for i, jar in enumerate(_jar_files()):
        shutil.copy(jar, pyflink_lib_dir)
    print(f"Copyied {i} HADOOP jar files into the pyflink lib dir at location {pyflink_lib_dir}")


def _find_pyflink_lib_dir() -> Optional[str]:
    for dir in site.getsitepackages():
        package_dir = os.path.join(dir, "pyflink", "lib")
        if os.path.exists(package_dir):
            return package_dir
    return None


def _jar_files() -> str:
    for root, _, files in os.walk(os.getenv("HADOOP_HOME")):
        for file in files:
            if file.endswith(".jar"):
                yield os.path.join(root, file)


def _get(filename, url) -> None:
    if os.path.exists(filename):
        return filename
    r = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(r.content)


def fetch_nessie() -> str:
    runner = "nessie-quarkus-runner"
    import pynessie
    version = pynessie.__version__
    url = "https://github.com/projectnessie/nessie/releases/download/nessie-{}/nessie-quarkus-{}-runner".format(version, version)
    _get(runner, url)
    os.chmod(runner, 0o777)
    return runner


def fetch_iceberg_flink() -> str:
    filename = _ICEBERG_FLINK_FILENAME
    url = _ICEBERG_FLINK_URL
    _get(filename, url)
    return filename


def start_nessie() -> None:
    runner = fetch_nessie()

    class NessieRunner:
        def __enter__(self):
            self._p = subprocess.Popen(['./'+runner], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            return self._p

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._p.kill()

    return NessieRunner()
