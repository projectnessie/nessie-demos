#!/usr/bin/env python
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
"""Utility functions to be used to download all the needed dependencies."""
import os
import shutil
import site
import stat
import sysconfig
import tarfile
from typing import Optional

import requests

try:
    import pyspark

    _SPARK_VERSION = pyspark.__version__
    _SPARK_FILENAME = f"spark-{_SPARK_VERSION}-bin-hadoop3.2"
    _SPARK_URL = f"https://archive.apache.org/dist/spark/spark-{_SPARK_VERSION}/{_SPARK_FILENAME}.tgz"
except ImportError:
    _SPARK_VERSION = None
    _SPARK_FILENAME = None
    _SPARK_URL = None

_HADOOP_VERSION = "2.10.1"
_HADOOP_FILENAME = f"hadoop-{_HADOOP_VERSION}"
_HADOOP_URL = f"https://archive.apache.org/dist/hadoop/common/hadoop-{_HADOOP_VERSION}/{_HADOOP_FILENAME}.tar.gz"

_FLINK_MAJOR_VERSION = "1.13"

_ICEBERG_VERSION = "0.13.1"
_ICEBERG_FLINK_FILENAME = f"iceberg-flink-runtime-{_FLINK_MAJOR_VERSION}-{_ICEBERG_VERSION}.jar"
_ICEBERG_FLINK_URL = f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-{_FLINK_MAJOR_VERSION}/{_ICEBERG_VERSION}/{_ICEBERG_FLINK_FILENAME}"
_ICEBERG_HIVE_FILENAME = f"iceberg-hive-runtime-{_ICEBERG_VERSION}.jar"
_ICEBERG_HIVE_URL = f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/{_ICEBERG_VERSION}/{_ICEBERG_HIVE_FILENAME}"

_HIVE_VERSION = "2.3.9"
_HIVE_FILENAME = f"apache-hive-{_HIVE_VERSION}-bin"
_HIVE_URL = (
    f"https://archive.apache.org/dist/hive/hive-{_HIVE_VERSION}/{_HIVE_FILENAME}.tar.gz"
)


def _link_file_into_dir(source_file: str, target_dir: str, replace_if_exists=True) -> None:
    assert os.path.isfile(source_file)
    assert os.path.isdir(target_dir)

    source_filename = os.path.basename(source_file)
    target_file = os.path.join(target_dir, source_filename)

    replaced = False
    target_exists = os.path.exists(target_file)
    if target_exists:
        if not replace_if_exists:
            print(f"Link target already exists: {target_file}")
            return
        if os.path.getsize(target_file) == os.path.getsize(source_file):
            os.remove(target_file)
            replaced = True

    os.link(source_file, target_file)
    assert os.path.isfile(target_file), (source_file, target_file)

    action = 'replaced' if replaced else 'created'
    print(f"Link target was {action}: {target_file} (source: {source_file})")


def _get_unzip(filename: str, url: str) -> None:
    if not os.path.exists(filename):
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with tarfile.open(fileobj=response.raw, mode="r|gz") as file:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(file, path=".")


def fetch_spark() -> None:
    """Download and unzip Spark."""
    if not _SPARK_VERSION:
        raise NotImplementedError(
            "Can't download spark as pyspark hasn't been installed."
        )
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
    for _jar_count, jar in enumerate(_jar_files()):
        _link_file_into_dir(jar, pyflink_lib_dir)
    print(f"Linked {_jar_count} HADOOP jar files into the pyflink lib dir at location {pyflink_lib_dir}")


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


def _download_file(filename: str, url: str) -> None:
    if os.path.exists(filename):
        return
    r = requests.get(url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)


def fetch_nessie() -> str:
    """Download nessie executable."""
    runner = "nessie-quarkus-runner"

    url = _get_base_nessie_url()
    _download_file(runner, url)
    os.chmod(runner, os.stat(runner).st_mode | stat.S_IXUSR)
    return runner


def fetch_nessie_jar() -> str:
    """Download nessie Jar in order to run the tests in Mac"""
    runner = "nessie-quarkus-runner.jar"

    url = _get_base_nessie_url() + ".jar"
    _download_file(runner, url)
    return runner


def _get_base_nessie_url() -> str:
    import pynessie

    version = pynessie.__version__

    return "https://github.com/projectnessie/nessie/releases/download/nessie-{}/nessie-quarkus-{}-runner".format(
        version, version
    )


def fetch_iceberg_flink() -> str:
    """Download flink jar for iceberg."""
    filename = _ICEBERG_FLINK_FILENAME
    url = _ICEBERG_FLINK_URL
    _download_file(filename, url)
    return filename


def fetch_hive() -> None:
    """Download and unzip Hive."""
    _get_unzip(_HIVE_FILENAME, _HIVE_URL)
    os.environ["HIVE_HOME"] = os.path.join(os.getcwd(), _HIVE_FILENAME)


def fetch_iceberg_hive() -> str:
    """Download Hive jar for iceberg."""
    filename = _ICEBERG_HIVE_FILENAME
    url = _ICEBERG_HIVE_URL
    _download_file(filename, url)
    return filename


def fetch_hive_with_iceberg_jars() -> None:
    """Download both Hive and Iceberg Hive jars."""
    fetch_hive()
    if not os.getenv("HIVE_HOME"):
        raise Exception(
            "The HIVE_HOME env var must be set and point to a valid Hive installation"
        )

    hive_auxlib_dir = os.path.join(os.getenv("HIVE_HOME"), "auxlib")

    if not os.path.exists(hive_auxlib_dir):
        os.mkdir(hive_auxlib_dir)

    iceberg_hive_jar = fetch_iceberg_hive()

    try:
        shutil.copy(iceberg_hive_jar, hive_auxlib_dir)
    except FileExistsError:
        print(f"Jar {iceberg_hive_jar} exists already.")


def fetch_hadoop() -> None:
    """Download Hadoop jars."""
    _get_hadoop()
