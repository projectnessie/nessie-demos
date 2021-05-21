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
"""NessieDemoIcebergSpark handles setting up Spark and Iceberg related objects.

This code shall work for all Spark and Iceberg versions used in all demos.
Due to that, we cannot add `pyspark` as a dependency into `requirements.txt`, but `NessieDemo` takes care of
installing the correct `pyspark` version. Since packages like `pyspark` are only available after the dependencies
have been installed, all Spark related code must be in a separate Python module that is loaded after
`nessiedemo.demo.setup_demo()` (i.e. `NessieDemo.start()`) has been executed.
"""

from typing import Tuple

from pyspark import SparkConf  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt
from pyspark.sql import SparkSession  # NOTE: this module is INTENTIONALLY NOT included in requirements.txt

from .demo import NessieDemo
from .spark_base import NessieDemoSparkSupport


class NessieDemoIcebergSpark(NessieDemoSparkSupport):
    """`NessieDemoIcebergSpark` is a helper class for Spark in Nessie-Demos.

    It contains code that uses pyspark and py4j, which is only available after `NessieDemo` has been prepared (started).
    """

    def __init__(self: "NessieDemoIcebergSpark", demo: NessieDemo) -> None:
        """Creates a `NessieDemoIcebergSpark` instance for respectively using the given `NessieDemo` instance."""
        super().__init__(demo)

    def get_or_create_spark_context(self: "NessieDemoIcebergSpark", nessie_ref: str = "main", catalog_name: str = "nessie") -> Tuple:
        """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

        :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
        Can be a branch name, tag name or commit hash. Default is `main`.
        :param catalog_name: Name of the catalog, defaults to `nessie`.
        :return: A 3-tuple of `SparkSession`, `SparkContext` and the JVM gateway
        """
        conf = self.__spark_conf(nessie_ref, catalog_name)
        return self._get_or_create_spark_context(conf)

    def __spark_conf(self: "NessieDemoIcebergSpark", nessie_ref: str = "main", catalog_name: str = "nessie") -> SparkConf:
        conf = SparkConf()

        spark_jars = "org.apache.iceberg:iceberg-spark3-runtime:{}".format(self._get_demo().get_iceberg_version())
        endpoint = self._get_demo().get_nessie_api_uri()

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

    def session_for_ref(self: "NessieDemoIcebergSpark", nessie_ref: str, catalog_name: str = "nessie") -> SparkSession:
        """Retrieve a new `SparkSession` ready to use against the given Nessie reference.

        Note: when you use multiple `SparkSession`s in your notebook/demo, make sure you always call this method
        to "switch the branch".

        :param nessie_ref: the Nessie reference to configure in the `SparkConf`. Can be a branch name, tag name or commit hash.
        :return: new `SparkSession`
        """
        new_session = self._get_spark().newSession()
        new_session.conf.set("spark.sql.catalog.{}.ref".format(catalog_name), nessie_ref)

        # Required with Spark 3.1
        self._get_jvm().SparkSession.setActiveSession(new_session._jsparkSession)

        return new_session


__NESSIE_ICEBERG_SPARK_DEMO__ = None


def iceberg_spark_for_demo(demo: NessieDemo, nessie_ref: str = "main", catalog_name: str = "nessie") -> Tuple:
    """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided/default `nessie_ref`.

    :param demo: `NessieDemo` instance to use.
    :param nessie_ref: the Nessie reference as a `str` to configure in the `SparkConf`.
    Can be a branch name, tag name or commit hash.
    :param catalog_name: Name of the catalog, defaults to `nessie`.
    :return: A 4-tuple of `SparkSession`, `SparkContext`, the JVM gateway and `NessieDemoIcebergSpark`
    """
    global __NESSIE_ICEBERG_SPARK_DEMO__
    iceberg_dispose()

    demo_spark = NessieDemoIcebergSpark(demo)
    __NESSIE_ICEBERG_SPARK_DEMO__ = demo_spark
    spark, sc, jvm = demo_spark.get_or_create_spark_context(nessie_ref=nessie_ref, catalog_name=catalog_name)
    # TODO need a way to properly shutdown the spark-context (the pyspark-shell process)
    return spark, sc, jvm, demo_spark


def iceberg_dispose() -> None:
    """Stops the SparkContext, if setup via `iceberg_for_demo`."""
    global __NESSIE_ICEBERG_SPARK_DEMO__
    if __NESSIE_ICEBERG_SPARK_DEMO__:
        __NESSIE_ICEBERG_SPARK_DEMO__.dispose()
        __NESSIE_ICEBERG_SPARK_DEMO__ = None
