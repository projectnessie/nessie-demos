# -*- coding: utf-8 -*-
"""NessieDemo TODO docs."""

import os
import re
from typing import Any, Callable, Tuple, TypeVar

import findspark  # NOTE: this module is NOT included in requirements.txt
from .demo import NessieDemo, _Util
from py4j.java_gateway import java_import  # NOTE: this module is NOT included in requirements.txt
from pyspark import SparkConf, SparkContext  # NOTE: this module is NOT included in requirements.txt
from pyspark.sql import SparkSession  # NOTE: this module is NOT included in requirements.txt

T = TypeVar("T", bound="NessieDemoSpark")


class NessieDemoSpark:
    _demo: NessieDemo = None
    _assets_dir: os.path = None

    def __init__(self: T, demo: NessieDemo) -> None:
        self._demo = demo
        self._assets_dir = demo._assets_dir

        spark_url = self._demo._versions_dict["spark"]["tarball"]
        # derive directory name inside the tarball from the URL
        dir_name = re.match(".*[/]([a-zA-Z0-9-.]+)[.]tgz", spark_url).group(1)
        spark_dir = os.path.join(self._assets_dir, dir_name)
        if not os.path.exists(spark_dir):
            tgz = os.path.join(self._assets_dir, "{}.tgz".format(dir_name))
            if not os.path.exists(tgz):
                _Util.wget(spark_url, tgz)
            _Util.exec_fail(["tar", "-x", "-C", self._assets_dir, "-f", tgz])

        print("Using Spark in {}".format(spark_dir))

        os.environ["SPARK_HOME"] = spark_dir

        findspark.init()

    def get_or_create_spark_context(self: T, nessie_ref: str = "main") -> Tuple[SparkSession, SparkContext, Any]:
        """Nessie demo TODO docs."""
        print("Creating SparkConf, SparkSession, SparkContext ...")
        conf = SparkConf()
        self.spark_conf(conf.set, nessie_ref)
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        sc = spark.sparkContext
        jvm = self.setup_jvm_for_iceberg(sc)
        print("Created SparkConf, SparkSession, SparkContext")

        return spark, sc, jvm

    def spark_conf(self: T, conf_setter: Callable[[str, str], Any], nessie_ref: str = "main") -> None:
        """Nessie demo TODO docs."""
        conf_setter(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark3-runtime:{}".format(self._demo.get_iceberg_version()),
        )
        conf_setter("spark.sql.execution.pyarrow.enabled", "true")
        conf_setter(
            "spark.sql.catalog.nessie.warehouse",
            "file://{}/spark_warehouse".format(os.getcwd()),
        )
        conf_setter("spark.sql.catalog.nessie.url", self._demo.nessie_api_uri)
        conf_setter("spark.sql.catalog.nessie.ref", nessie_ref)
        conf_setter(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        conf_setter("spark.sql.catalog.nessie.auth_type", "NONE")
        conf_setter("spark.sql.catalog.nessie.cache-enabled", "false")
        conf_setter("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        conf_setter(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )

    def setup_jvm_for_iceberg(self: T, spark_context: SparkContext) -> Any:
        """Nessie demo TODO docs."""
        jvm = spark_context._gateway.jvm

        java_import(jvm, "org.apache.iceberg.CatalogUtil")
        java_import(jvm, "org.apache.iceberg.catalog.TableIdentifier")
        java_import(jvm, "org.apache.iceberg.Schema")
        java_import(jvm, "org.apache.iceberg.types.Types")
        java_import(jvm, "org.apache.iceberg.PartitionSpec")

        return jvm
