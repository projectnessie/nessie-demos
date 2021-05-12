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
"""NessieDemoDelta handles setting up Spark and Deltalake related objects."""

import os
from typing import Any, Tuple

from py4j.java_gateway import java_import  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from pyspark import SparkConf, SparkContext  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt

from .demo import NessieDemo
from .spark_base import NessieDemoSparkSupport


class NessieDemoDeltaSpark(NessieDemoSparkSupport):
    """`NessieDemoDelta` is a helper class for Deltalake + Spark in Nessie-Demos.

    It contains code that uses pyspark and py4j, which is only available after `NessieDemo` has been prepared (started).
    """

    __spark_version: int

    def __init__(self: "NessieDemoDeltaSpark", demo: NessieDemo, spark_version: int = 3) -> None:
        """Creates a `NessieDemoDelta` instance for respectively using the given `NessieDemo` instance."""
        super().__init__(demo)
        self.__spark_version = spark_version

    def get_or_create_spark_context(self: "NessieDemoDeltaSpark", nessie_ref: str = "main") -> Tuple:
        """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

        :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
        Can be a branch name, tag name or commit hash. Default is `main`.
        :return: A 3-tuple of `SparkSession`, `SparkContext` and the JVM gateway
        """
        conf = self.__spark_conf(nessie_ref)
        return self._get_or_create_spark_context(conf)

    def change_ref(self: "NessieDemoDeltaSpark", nessie_ref: str) -> None:
        """Change the current SparkContext to use the given reference for future requests.

        :param nessie_ref: the Nessie reference to use in future requests.
        """
        spark_context = self._get_spark_context()
        spark_context.getConf().set("spark.hadoop.nessie.ref", nessie_ref)

        hadoop_conf = spark_context._jsc.hadoopConfiguration()
        hadoop_conf.set("nessie.ref", nessie_ref)

        self._get_jvm().DeltaLog.clearCache()

    def __spark_conf(self: "NessieDemoDeltaSpark", nessie_ref: str = "main") -> SparkConf:
        conf = SparkConf()

        spark_jars = "org.projectnessie:nessie-deltalake-spark{}:{}".format(self.__spark_version, self._get_demo().get_nessie_version())
        endpoint = self._get_demo().get_nessie_api_uri()

        conf.set("spark.jars.packages", spark_jars)
        conf.set(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        conf.set("spark.sql.execution.pyarrow.enabled", "true")
        conf.set("spark.hadoop.hive.metastore.warehouse.dir", "{}/hive-metastore".format(self.get_spark_warehouse()))
        conf.set("spark.hadoop.fs.defaultFS", "file://{}/fs".format(self.get_spark_warehouse()))
        conf.set("spark.hadoop.nessie.url", endpoint)
        conf.set("spark.hadoop.nessie.ref", nessie_ref)
        conf.set("spark.hadoop.nessie.auth_type", "NONE")
        conf.set("spark.hadoop.nessie.cache-enabled", "false")
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        conf.set("spark.delta.logFileHandler.class", "org.projectnessie.deltalake.NessieLogFileMetaParser")
        conf.set("spark.delta.logStore.class", "org.projectnessie.deltalake.NessieLogStore")
        return conf

    def table_path(self: "NessieDemoDeltaSpark", path: str) -> str:
        """Construct a path for a Delta table."""
        return os.path.join(self._get_demo()._asset_dir("spark_warehouse"), "delta", path)

    def _jvm_for_spark(self: "NessieDemoDeltaSpark", spark_context: SparkContext) -> Any:
        jvm = super()._jvm_for_spark(spark_context)

        java_import(jvm, "org.apache.spark.sql.delta.DeltaLog")
        java_import(jvm, "io.delta.tables.DeltaTable")

        return jvm


__NESSIE_DELTA_DEMO__ = None


def delta_spark_for_demo(demo: NessieDemo, spark_version: int = 3, nessie_ref: str = "main") -> Tuple:
    """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

    :param demo: `NessieDemo` instance to use.
    :param spark_version: The Spark major version to use (2 or 3), defaults to `3`.
    :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
    Can be a branch name, tag name or commit hash.
    :return: A 4-tuple of `SparkSession`, `SparkContext`, the JVM gateway and `NessieDemoDelta`
    """
    global __NESSIE_DELTA_DEMO__
    delta_spark_dispose()

    demo_delta = NessieDemoDeltaSpark(demo, spark_version=spark_version)
    __NESSIE_DELTA_DEMO__ = demo_delta
    spark, sc, jvm = demo_delta.get_or_create_spark_context(nessie_ref=nessie_ref)
    # TODO need a way to properly shutdown the spark-context (the pyspark-shell process)
    return spark, sc, jvm, demo_delta


def delta_spark_dispose() -> None:
    """Stops the SparkContext, if setup via `delta_for_demo`."""
    global __NESSIE_DELTA_DEMO__
    if __NESSIE_DELTA_DEMO__:
        __NESSIE_DELTA_DEMO__.dispose()
        __NESSIE_DELTA_DEMO__ = None
