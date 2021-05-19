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
"""NessieDemoFlink handles setting up Flink and Iceberg related objects."""

import os
import re
import shutil
import site
from types import TracebackType
from typing import Any, Tuple, TypeVar

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


from .demo import _Util, NessieDemo

T = TypeVar("T", bound="NessieDemoFlink")


class NessieDemoFlink:
    """`NessieDemoFlink` is a helper class for Flink in Nessie-Demos."""

    __demo: NessieDemo

    __env: StreamExecutionEnvironment = None
    __jvm: Any = None

    def __init__(self: T, demo: NessieDemo) -> None:
        """Creates a `NessieDemoFlink` instance for respectively using the given `NessieDemo` instance."""
        self.__demo = demo
        self.__retrieve_and_extract_hadoop()
        self.__copy_all_hadoop_jars_to_pyflink()

    def __retrieve_and_extract_hadoop(self: T) -> T:
        if "hadoop" in self.__demo._get_versions_dict() and "tarball" in self.__demo._get_versions_dict()["hadoop"]:
            hadoop_url = self.__demo._get_versions_dict()["hadoop"]["tarball"]
            # derive directory name inside the tarball from the URL
            m = re.match(".*[/]([a-zA-Z0-9-.]+).tar.gz", hadoop_url)
            if not m:
                raise Exception("Invalid Hadoop download URL {}".format(hadoop_url))
            dir_name = m.group(1)
            hadoop_dir = self.__demo._asset_dir(dir_name)
            if not os.path.exists(hadoop_dir):
                tgz = self.__demo._asset_dir("{}tar.gz".format(dir_name))
                if not os.path.exists(tgz):
                    _Util.wget(hadoop_url, tgz)
                _Util.exec_fail(["tar", "-x", "-z", "-C", os.path.abspath(os.path.join(hadoop_dir, "..")), "-f", tgz])
        else:
            raise Exception("configuration does not define hadoop.tarball. Unable to find Hadoop.")

        print("Using Hadoop in {}".format(hadoop_dir))
        os.environ["HADOOP_HOME"] = hadoop_dir
        return self

    def __copy_all_hadoop_jars_to_pyflink(self: T) -> T:
        jar_files = []

        for root, _, files in os.walk(os.getenv("HADOOP_HOME")):
            for file in files:
                if file.endswith(".jar"):
                    jar_files.append(os.path.join(root, file))

        pyflink_lib_dir = os.path.join(site.getsitepackages()[0], "pyflink", "lib")
        if not os.path.exists(pyflink_lib_dir):
            raise Exception("could not find pyflink lib directory at location " + pyflink_lib_dir)

        print(f"Copying all HADOOP jar files into the pyflink lib dir at location {pyflink_lib_dir}")
        for jar in jar_files:
            shutil.copy(jar, pyflink_lib_dir)

        return self

    def __enter__(self: T) -> T:
        """Noop."""
        return self

    def __exit__(self: T, exc_type: type, exc_val: BaseException, exc_tb: TracebackType) -> None:
        """Calls `stop()` on the `NessieDemo` instance."""
        self.dispose()

    def get_flink_warehouse(self: T) -> str:
        """Get the path to for the 'Flink Warehouse'."""
        return "file://{}".format(self.__demo._asset_dir("flink_warehouse"))

    def __execution_env(self: T) -> StreamExecutionEnvironment:
        if self.__env is not None:
            return self.__env
        env = StreamExecutionEnvironment.get_execution_environment()

        iceberg_flink_runtime_jar = "iceberg-flink-runtime-{}.jar".format(self.__demo.get_iceberg_version())
        asset_dir = self.__demo._asset_dir(iceberg_flink_runtime_jar)
        _Util.wget(self.__demo.get_iceberg_download_url_for_jar("iceberg-flink-runtime"), asset_dir)
        env.add_jars("file://{}".format(asset_dir))

        self.__env = env
        return env

    def table_env_for_ref(self: T, nessie_ref: str, catalog_name: str = "nessie") -> StreamTableEnvironment:
        """Retrieve a new StreamTableEnvironment ready to use against the given Nessie reference.

        :param nessie_ref: the Nessie reference to configure in the StreamTableEnvironment.
        Can be a branch name, tag name or commit hash.
        :param catalog_name: The catalog name to use. Defaults to `nessie`
        :return: new StreamTableEnvironment
        """
        table_env = StreamTableEnvironment.create(self.__execution_env())
        uri = self.__demo.get_nessie_api_uri()
        warehouse = self.get_flink_warehouse()
        table_env.execute_sql(
            f"CREATE CATALOG {catalog_name} WITH ('type'='iceberg', "
            f"'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog', "
            f"'uri'='{uri}', 'ref'='{nessie_ref}', "
            f"'auth_type'='NONE', 'cache-enabled' = 'false', "
            f"'warehouse' = '{warehouse}')"
        )
        return table_env

    def dispose(self: T) -> None:
        """Calls `stop()` on the `NessieDemo` instance."""
        print("Stopping TableEnv ...")
        self.__env = None
        self.__jvm = None

        try:
            self.__demo.stop()
            delattr(self, "__demo")
        except AttributeError:
            pass


__NESSIE_FLINK_DEMO__ = None


def flink_for_demo(demo: NessieDemo, nessie_ref: str = "main", catalog_name: str = "nessie") -> Tuple:
    """Sets up the `StreamTableEnvironment` ready to use for the provided/default `nessie_ref`.

    :param demo: `NessieDemo` instance to use.
    :param nessie_ref: the Nessie reference as a `str` to configure in the
    `StreamTableEnvironment`.
    Can be a branch name, tag name or commit hash.
    :param catalog_name: Name of the catalog, defaults to `nessie`.
    :return: A 2-tuple of `StreamTableEnvironment` and `NessieDemoFlink`
    """
    global __NESSIE_FLINK_DEMO__
    flink_dispose()

    demo_flink = NessieDemoFlink(demo)
    __NESSIE_FLINK_DEMO__ = demo_flink
    return demo_flink.table_env_for_ref(nessie_ref=nessie_ref, catalog_name=catalog_name), demo_flink


def flink_dispose() -> None:
    """Stops whatever necessary, if setup via `flink_for_demo`."""
    global __NESSIE_FLINK_DEMO__
    if __NESSIE_FLINK_DEMO__:
        __NESSIE_FLINK_DEMO__.dispose()
        __NESSIE_FLINK_DEMO__ = None
