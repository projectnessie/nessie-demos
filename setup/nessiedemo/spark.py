# -*- coding: utf-8 -*-
"""NessieDemoSpark handles setting up Spark and Iceberg related objects.

This code shall work for all Spark and Iceberg versions used in all demos.
Due to that, we cannot add `pyspark` as a dependency into `requirements.txt`, but `NessieDemo` takes care of
installing the correct `pyspark` version. Since packages like `pyspark` are only available after the dependencies
have been installed, all Spark related code must be in a separate Python module that is loaded after
`nessiedemo.demo.setup_demo()` (i.e. `NessieDemo.start()`) has been executed.
"""

import os
import re
from typing import Any, Tuple, TypeVar

import findspark  # NOTE: this module is NOT included in requirements.txt
from py4j.java_gateway import java_import  # NOTE: this module is NOT included in requirements.txt
from pyspark import SparkConf, SparkContext  # NOTE: this module is NOT included in requirements.txt
from pyspark.sql import SparkSession  # NOTE: this module is NOT included in requirements.txt

from .demo import _Util, NessieDemo

T = TypeVar("T", bound="NessieDemoSpark")


class NessieDemoSpark:
    """`NessieDemoSpark` is a helper class for Spark in Nessie-Demos.

    It contains code that uses pyspark and py4j, which is only available after `NessieDemo` has been prepared (started).
    """

    __demo: NessieDemo
    __assets_dir: str

    def __init__(self: T, demo: NessieDemo) -> None:
        """Creates a `NessieDemoSpark` instance for respectively using the given `NessieDemo` instance."""
        self.__demo = demo
        self.__assets_dir = demo._get_assets_dir()

        spark_url = self.__demo._get_versions_dict()["spark"]["tarball"]
        # derive directory name inside the tarball from the URL
        m = re.match(".*[/]([a-zA-Z0-9-.]+)[.]tgz", spark_url)
        if not m:
            raise Exception("Invalid Spark download URL {}".format(spark_url))
        dir_name = m.group(1)
        spark_dir = os.path.join(self.__assets_dir, dir_name)
        if not os.path.exists(spark_dir):
            tgz = os.path.join(self.__assets_dir, "{}.tgz".format(dir_name))
            if not os.path.exists(tgz):
                _Util.wget(spark_url, tgz)
            _Util.exec_fail(["tar", "-x", "-C", self.__assets_dir, "-f", tgz])

        print("Using Spark in {}".format(spark_dir))

        os.environ["SPARK_HOME"] = spark_dir

        findspark.init()

    def get_or_create_spark_context(self: T, nessie_ref: str = "main") -> Tuple:  # Tuple[SparkSession, SparkContext, Any]
        """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided `nessie_ref`.

        :param nessie_ref: the Nessie reference to configure in the `SparkConf`. Can be a branch name, tag name or commit hash.
        :return: A 3-tuple of `SparkSession`, `SparkContext` and the JVM gateway
        """
        print("Creating SparkConf, SparkSession, SparkContext ...")
        conf = self.__spark_conf(nessie_ref)
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        sc = spark.sparkContext
        jvm = self.__jvm_for_iceberg(sc)
        print("Created SparkConf, SparkSession, SparkContext")

        return spark, sc, jvm

    def __spark_conf(self: T, nessie_ref: str = "main") -> SparkConf:
        conf = SparkConf()
        conf.set(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark3-runtime:{}".format(self.__demo.get_iceberg_version()),
        )
        conf.set("spark.sql.execution.pyarrow.enabled", "true")
        conf.set(
            "spark.sql.catalog.nessie.warehouse",
            "file://{}/spark_warehouse".format(os.getcwd()),
        )
        conf.set("spark.sql.catalog.nessie.url", self.__demo.get_nessie_api_uri())
        conf.set("spark.sql.catalog.nessie.ref", nessie_ref)
        conf.set(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        conf.set("spark.sql.catalog.nessie.auth_type", "NONE")
        conf.set("spark.sql.catalog.nessie.cache-enabled", "false")
        conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
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

        return jvm


def spark_for_demo(demo: NessieDemo, nessie_ref: str = "main") -> Tuple:  # Tuple[SparkSession, SparkContext, Any, NessieDemoSpark]
    """Sets up the `SparkConf`, `SparkSession` and `SparkContext` ready to use for the provided `nessie_ref`.

    :param nessie_ref: the Nessie reference to configure in the `SparkConf`. Can be a branch name, tag name or commit hash.
    :return: A 4-tuple of `SparkSession`, `SparkContext`, the JVM gateway and `NessieDemoSpark`
    """
    demo_spark = NessieDemoSpark(demo)
    spark, sc, jvm = demo_spark.get_or_create_spark_context(nessie_ref)
    return spark, sc, jvm, demo_spark
