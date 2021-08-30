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
import shutil
import site
import stat
import subprocess  # noqa: S404
import sysconfig
import tarfile
from typing import Any
from typing import Optional

import requests

try:
    import pyspark

    _SPARK_VERSION = pyspark.__version__
except ImportError:
    _SPARK_VERSION = "3.1.2"

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
        with tarfile.open(fileobj=response.raw, mode="r|gz") as file:
            file.extractall(path=".")


def fetch_spark() -> None:
    """Download and unzip Spark."""
    _get_unzip(_SPARK_FILENAME, _SPARK_URL)
    os.environ["SPARK_HOME"] = os.path.join(os.getcwd(), _SPARK_FILENAME)


def _get_hadoop() -> None:
    _get_unzip(_HADOOP_FILENAME, _HADOOP_URL)
    os.environ["HADOOP_HOME"] = os.path.join(os.getcwd(), _HADOOP_FILENAME)


def _copy_all_hadoop_jars_to_pyflink() -> None:
    _get_hadoop()
    if not os.getenv("HADOOP_HOME"):
        raise Exception(
            "The HADOOP_HOME env var must be set and point to a valid Hadoop installation"
        )

    pyflink_lib_dir = _find_pyflink_lib_dir()
    duplicates = 0
    for _jar_count, jar in enumerate(_jar_files()):
        try:
            shutil.copy(jar, pyflink_lib_dir)
        except FileExistsError:
            duplicates += 1
            print(f"Duplicate jar {jar}")
    print(
        f"Copied {_jar_count} HADOOP jar files into the pyflink lib dir at location {pyflink_lib_dir} with {duplicates} duplicates"
    )


def _find_pyflink_lib_dir() -> Optional[str]:
    for dir in site.getsitepackages() + [sysconfig.get_paths()["purelib"]]:
        package_dir = os.path.join(dir, "pyflink", "lib")
        if os.path.exists(package_dir):
            return package_dir
    return None


def _jar_files() -> str:
    for root, _, files in os.walk(os.getenv("HADOOP_HOME")):
        for file in files:
            if file.endswith(".jar"):
                yield os.path.join(root, file)


def _get(filename: str, url: str) -> None:
    if os.path.exists(filename):
        return filename
    r = requests.get(url)
    with open(filename, "wb") as f:
        f.write(r.content)


def fetch_nessie() -> str:
    """Download nessie executable."""
    runner = "nessie-quarkus-runner"
    import pynessie

    version = pynessie.__version__
    url = "https://github.com/projectnessie/nessie/releases/download/nessie-{}/nessie-quarkus-{}-runner".format(
        version, version
    )
    _get(runner, url)
    os.chmod(runner, os.stat(runner).st_mode | stat.S_IXUSR)
    return runner


def fetch_iceberg_flink() -> str:
    """Download flink jar for iceberg."""
    filename = _ICEBERG_FLINK_FILENAME
    url = _ICEBERG_FLINK_URL
    _get(filename, url)
    return filename


def start_nessie() -> None:
    """Context for starting and stopping a nessie binary."""
    runner = fetch_nessie()

    class NessieRunner:
        def __enter__(self: "NessieRunner") -> subprocess.Popen:
            self._p = subprocess.Popen(  # noqa: S603
                ["./" + runner], stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            return self._p

        def __exit__(
            self: "NessieRunner", exc_type: Any, exc_val: Any, exc_tb: Any
        ) -> None:
            self._p.kill()

    return NessieRunner()
