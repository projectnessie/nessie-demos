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
"""NessieDemoSpark handles setting up Spark and Iceberg related objects.

This code shall work for all Spark and Iceberg versions used in all demos.
Due to that, we cannot add `pyspark` as a dependency into `requirements.txt`, but `NessieDemo` takes care of
installing the correct `pyspark` version. Since packages like `pyspark` are only available after the dependencies
have been installed, all Spark related code must be in a separate Python module that is loaded after
`nessiedemo.demo.setup_demo()` (i.e. `NessieDemo.start()`) has been executed.
"""

import os
import re
import site
import sysconfig
from types import TracebackType
from typing import Any, Tuple, TypeVar

import findspark  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from py4j.java_gateway import java_import  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from pyspark import SparkConf, SparkContext  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from pyspark.sql import SparkSession  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt

from .demo import _Util, NessieDemo

T = TypeVar("T", bound="NessieDemoSpark")


class NessieDemoSpark:
    """`NessieDemoSpark` is a helper class for Spark in Nessie-Demos.

    It contains code that uses pyspark and py4j, which is only available after `NessieDemo` has been prepared (started).
    """

    __demo: NessieDemo

    __spark: SparkSession
    __spark_context: SparkContext
    __jvm: Any

    def __init__(self: T, demo: NessieDemo) -> None:
        """Creates a `NessieDemoSpark` instance for respectively using the given `NessieDemo` instance."""
        self.__demo = demo

        spark_dir = None

        pyspark_dir = os.path.join(sysconfig.get_paths()["purelib"], "pyspark")
        if os.path.isdir(pyspark_dir):
            spark_dir = pyspark_dir
        else:
            for dir in site.getsitepackages():
                pyspark_dir = os.path.join(dir, "pyspark")
                if os.path.isdir(pyspark_dir):
                    spark_dir = pyspark_dir
                    break

        if not spark_dir and "spark" in self.__demo._get_versions_dict() and "tarball" in self.__demo._get_versions_dict()["spark"]:
            spark_url = self.__demo._get_versions_dict()["spark"]["tarball"]
            # derive directory name inside the tarball from the URL
            m = re.match(".*[/]([a-zA-Z0-9-.]+)[.]tgz", spark_url)
            if not m:
                raise Exception("Invalid Spark download URL {}".format(spark_url))
            dir_name = m.group(1)
            spark_dir = self.__demo._asset_dir(dir_name)
            if not os.path.exists(spark_dir):
                tgz = self.__demo._asset_dir("{}.tgz".format(dir_name))
                if not os.path.exists(tgz):
                    _Util.wget(spark_url, tgz)
                _Util.exec_fail(["tar", "-x", "-C", os.path.abspath(os.path.join(spark_dir, "..")), "-f", tgz])

        if not spark_dir:
            raise Exception("configuration does not define spark.tarball and pyspark is not installed. Unable to find Spark.")

        print("Using Spark in {}".format(spark_dir))

        os.environ["SPARK_HOME"] = spark_dir

        findspark.init()

    def __enter__(self: T) -> T:
        """Noop."""
        return self

    def __exit__(self: T, exc_type: type, exc_val: BaseException, exc_tb: TracebackType) -> None:
        """Disposes the SparkContext and calls `stop()` on the `NessieDemo` instance."""
        self.dispose()

    def get_spark_warehouse(self: T) -> str:
        """Get the path to for the 'Spark Warehouse'."""
        return "file://{}".format(self.__demo._asset_dir("spark_warehouse"))

    def get_or_create_spark_context(self: T, nessie_ref: str = "main", catalog_name: str = "nessie") -> Tuple:
        """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

        :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
        Can be a branch name, tag name or commit hash. Default is `main`.
        :param catalog_name: Name of the catalog, defaults to `nessie`.
        :return: A 3-tuple of `SparkSession`, `SparkContext` and the JVM gateway
        """
        print("Creating SparkConf, SparkSession, SparkContext ...")
        conf = self.__spark_conf(nessie_ref, catalog_name)
        self.__spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.__spark_context = self.__spark.sparkContext
        self.__jvm = self.__jvm_for_iceberg(self.__spark_context)
        print("Created SparkConf, SparkSession, SparkContext")

        return self.__spark, self.__spark_context, self.__jvm

    def __spark_conf(self: T, nessie_ref: str = "main", catalog_name: str = "nessie") -> SparkConf:
        conf = SparkConf()

        spark_jars = "org.apache.iceberg:iceberg-spark3-runtime:{}".format(self.__demo.get_iceberg_version())
        endpoint = self.__demo.get_nessie_api_uri()

        conf.set("spark.jars.packages", spark_jars)
        conf.set("spark.sql.execution.pyarrow.enabled", "true")
        conf.set("spark.sql.catalog.{}.warehouse".format(catalog_name), self.get_spark_warehouse())
        conf.set("spark.sql.catalog.{}.url".format(catalog_name), endpoint)
        conf.set("spark.sql.catalog.{}.ref".format(catalog_name), nessie_ref)
        conf.set(
            "spark.sql.catalog.{}.catalog-impl".format(catalog_name),
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        conf.set("spark.sql.catalog.{}.auth_type".format(catalog_name), "NONE")
        conf.set("spark.sql.catalog.{}.cache-enabled".format(catalog_name), "false")
        conf.set("spark.sql.catalog.{}".format(catalog_name), "org.apache.iceberg.spark.SparkCatalog")
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        return conf

    def __jvm_for_iceberg(self: T, spark_context: SparkContext) -> Any:
        jvm = spark_context._gateway.jvm

        java_import(jvm, "org.apache.iceberg.CatalogUtil")
        java_import(jvm, "org.apache.iceberg.catalog.TableIdentifier")
        java_import(jvm, "org.apache.iceberg.Schema")
        java_import(jvm, "org.apache.iceberg.types.Types")
        java_import(jvm, "org.apache.iceberg.PartitionSpec")
        java_import(jvm, "org.apache.spark.SparkContext")
        java_import(jvm, "org.apache.spark.sql.SparkSession")

        return jvm

    def session_for_ref(self: T, nessie_ref: str, catalog_name: str = "nessie") -> SparkSession:
        """Retrieve a new `SparkSession` ready to use against the given Nessie reference.

        Note: when you use multiple `SparkSession`s in your notebook/demo, make sure you always call this method
        to "switch the branch".

        :param nessie_ref: the Nessie reference to configure in the `SparkConf`. Can be a branch name, tag name or commit hash.
        :return: new `SparkSession`
        """
        new_session = self.__spark.newSession()
        new_session.conf.set("spark.sql.catalog.{}.ref".format(catalog_name), nessie_ref)

        # Required with Spark 3.1
        self.__jvm.SparkSession.setActiveSession(new_session._jsparkSession)

        return new_session

    def dispose(self: T) -> None:
        """Disposes the SparkContext and calls `stop()` on the `NessieDemo` instance."""
        try:
            spark_sess = self.__spark
            print("Stopping SparkSession ...")
            spark_sess.stop()
            delattr(self, "__spark")
            delattr(self, "__spark_context")
            delattr(self, "__jvm")

            SparkContext._active_spark_context.stop()
            SparkContext._gateway.shutdown()
            SparkContext._gateway = None
            SparkContext._jvm = None
        except AttributeError:
            pass
        try:
            self.__demo.stop()
            delattr(self, "__demo")
        except AttributeError:
            pass


__NESSIE_SPARK_DEMO__ = None


def spark_for_demo(demo: NessieDemo, nessie_ref: str = "main", catalog_name: str = "nessie") -> Tuple:
    """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

    :param demo: `NessieDemo` instance to use.
    :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
    Can be a branch name, tag name or commit hash.
    :param catalog_name: Name of the catalog, defaults to `nessie`.
    :return: A 4-tuple of `SparkSession`, `SparkContext`, the JVM gateway and `NessieDemoSpark`
    """
    global __NESSIE_SPARK_DEMO__
    spark_dispose()

    demo_spark = NessieDemoSpark(demo)
    __NESSIE_SPARK_DEMO__ = demo_spark
    spark, sc, jvm = demo_spark.get_or_create_spark_context(nessie_ref=nessie_ref, catalog_name=catalog_name)
    # TODO need a way to properly shutdown the spark-context (the pyspark-shell process)
    return spark, sc, jvm, demo_spark


def spark_dispose() -> None:
    """Stops the SparkContext, if setup via `spark_for_demo`."""
    global __NESSIE_SPARK_DEMO__
    if __NESSIE_SPARK_DEMO__:
        __NESSIE_SPARK_DEMO__.dispose()
        __NESSIE_SPARK_DEMO__ = None
