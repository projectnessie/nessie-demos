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
"""Common code for Iceberg/Spark + Delta/Iceberg/Spark demo-support-code."""

import os
from abc import ABC
from types import TracebackType
from typing import Any, Tuple

import findspark  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from py4j.java_gateway import java_import  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from pyspark import SparkConf, SparkContext  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from pyspark.sql import SparkSession  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt

from .demo import _Util, NessieDemo


class NessieDemoSparkSupport(ABC):
    """`NessieDemoSparkSupport` is a helper class for Spark based Nessie-Demos.

    It contains code that uses pyspark and py4j, which is only available after `NessieDemo` has been prepared (started).
    """

    __demo: NessieDemo

    __spark: SparkSession
    __spark_context: SparkContext
    __jvm: Any

    def __init__(self: "NessieDemoSparkSupport", demo: NessieDemo) -> None:
        """Base class for Spark based demo instances for respectively using the given `NessieDemo` instance."""
        self.__demo = demo

        spark_dir = _Util.get_python_package_directory("pyspark")

        if not spark_dir:
            spark_dir = demo._pull_product_distribution("spark", "Spark")

        if not spark_dir:
            raise Exception("configuration does not define spark.tarball and pyspark is not installed. Unable to find Spark.")

        print("Using Spark in {}".format(spark_dir))

        os.environ["SPARK_HOME"] = spark_dir

        findspark.init()

    def __enter__(self: "NessieDemoSparkSupport") -> "NessieDemoSparkSupport":
        """Noop."""
        return self

    def __exit__(self: "NessieDemoSparkSupport", exc_type: type, exc_val: BaseException, exc_tb: TracebackType) -> None:
        """Disposes the SparkContext and calls `stop()` on the `NessieDemo` instance."""
        self.dispose()

    def _get_demo(self: "NessieDemoSparkSupport") -> NessieDemo:
        return self.__demo

    def _get_spark(self: "NessieDemoSparkSupport") -> SparkSession:
        return self.__spark

    def _get_spark_context(self: "NessieDemoSparkSupport") -> SparkContext:
        return self.__spark_context

    def _get_jvm(self: "NessieDemoSparkSupport") -> Any:
        return self.__jvm

    def get_spark_warehouse(self: "NessieDemoSparkSupport") -> str:
        """Get the path to for the 'Spark Warehouse'."""
        return "file://{}".format(self.__demo._asset_dir("spark_warehouse"))

    def _get_or_create_spark_context(self: "NessieDemoSparkSupport", conf: SparkConf) -> Tuple:
        """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

        :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
        Can be a branch name, tag name or commit hash. Default is `main`.
        :param catalog_name: Name of the catalog, defaults to `nessie`.
        :return: A 3-tuple of `SparkSession`, `SparkContext` and the JVM gateway
        """
        print("Creating SparkConf, SparkSession, SparkContext ...")
        self.__spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.__spark_context = self.__spark.sparkContext
        self.__jvm = self._jvm_for_spark(self.__spark_context)
        print("Created SparkConf, SparkSession, SparkContext")

        return self.__spark, self.__spark_context, self.__jvm

    def _jvm_for_spark(self: "NessieDemoSparkSupport", spark_context: SparkContext) -> Any:
        jvm = spark_context._gateway.jvm

        java_import(jvm, "org.apache.iceberg.CatalogUtil")
        java_import(jvm, "org.apache.iceberg.catalog.TableIdentifier")
        java_import(jvm, "org.apache.iceberg.Schema")
        java_import(jvm, "org.apache.iceberg.types.Types")
        java_import(jvm, "org.apache.iceberg.PartitionSpec")
        java_import(jvm, "org.apache.spark.SparkContext")
        java_import(jvm, "org.apache.spark.sql.SparkSession")

        return jvm

    def dispose(self: "NessieDemoSparkSupport") -> None:
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
