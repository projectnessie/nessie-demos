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
"""Tests for a GA version config for the `nessiedemo` package."""
import os
from subprocess import run  # noqa: S404

from pytest import fixture, skip

from nessiedemo.demo import setup_demo
from .util import demo_setup_fixture_for_tests


@fixture(scope="function", autouse=True)
def before_all(tmpdir_factory, request) -> None:  # noqa: ANN001
    """Sets up env-vars to use a pytest temp-dir and use assets from the source-tree."""
    demo_setup_fixture_for_tests(tmpdir_factory, request)

    def __dispose_spark() -> None:
        from nessiedemo.spark import spark_dispose

        print("TEST-TEARDOWN: Disposing SparkContext...")
        spark_dispose()

    request.addfinalizer(__dispose_spark)


class TestWithSpark:
    """Test NessieDemo with Spark."""

    @staticmethod
    def __test_with_spark(versions_yaml: str, required_envs: list) -> None:
        """Test NessieDemo plus NessieDemoSpark."""
        if False in [e in os.environ for e in required_envs]:
            skip(
                "Missing mandatory environment variable(s) {} for in-development-yaml-test, skipping test".format(
                    ", ".join(["{}={}".format(e, os.environ[e] if e in os.environ else "<not set>") for e in required_envs])
                )
            )

        demo = setup_demo(versions_yaml)

        print("Nessie version: {}".format(demo.get_nessie_version()))
        print("Iceberg version: {}".format(demo.get_iceberg_version()))

        # Same with notebooks: must NOT import nessiedemo.spark BEFORE the demo's setup has "pip-install-ed" the spark dependencies
        from nessiedemo.spark import spark_for_demo

        spark, sc, jvm, demo_spark = spark_for_demo(demo, catalog_name="test_with_spark")
        assert spark.conf.get("spark.sql.catalog.test_with_spark.ref") == "main"
        assert spark.conf.get("spark.sql.catalog.test_with_spark.url") == demo.get_nessie_api_uri()
        assert spark.conf.get("spark.jars.packages") == "org.apache.iceberg:iceberg-spark3-runtime:" + demo.get_iceberg_version()
        assert sc is not None
        assert jvm is not None

        spark_dev = demo_spark.session_for_ref("dev", catalog_name="test_with_spark")
        assert spark_dev.conf.get("spark.sql.catalog.test_with_spark.ref") == "dev"
        assert spark_dev.conf.get("spark.sql.catalog.test_with_spark.url") == demo.get_nessie_api_uri()
        assert spark_dev.conf.get("spark.jars.packages") == "org.apache.iceberg:iceberg-spark3-runtime:" + demo.get_iceberg_version()

        run(["nessie", "branch", "dev"])  # noqa: S603 S607

        catalog = jvm.CatalogUtil.loadCatalog(
            "org.apache.iceberg.nessie.NessieCatalog",
            "test_with_spark",
            {
                "ref": "dev",
                "url": demo.get_nessie_api_uri(),
                "warehouse": demo_spark.get_spark_warehouse(),
            },
            sc._jsc.hadoopConfiguration(),
        )

        dataset = demo.fetch_dataset("region-nation")

        # Creating region table
        region_name = jvm.TableIdentifier.parse("testing.region")
        region_schema = jvm.Schema(
            [
                jvm.Types.NestedField.optional(1, "R_REGIONKEY", jvm.Types.LongType.get()),
                jvm.Types.NestedField.optional(2, "R_NAME", jvm.Types.StringType.get()),
                jvm.Types.NestedField.optional(3, "R_COMMENT", jvm.Types.StringType.get()),
            ]
        )
        region_spec = jvm.PartitionSpec.unpartitioned()

        catalog.createTable(region_name, region_schema, region_spec)
        region_df = spark_dev.read.load(dataset["region.parquet"])
        region_df.write.format("iceberg").mode("overwrite").save("test_with_spark.testing.region")

    def test_with_spark(self: object) -> None:
        """Test NessieDemo+Spark against Nessie 0.5.x + Iceberg 0.11.x."""
        TestWithSpark.__test_with_spark("nessie-0.5-iceberg-0.11.yml", [])

    def test_with_spark_iceberg_in_dev(self: object) -> None:
        """Test NessieDemo+Spark against an in-development version of Iceberg built locally from source."""
        TestWithSpark.__test_with_spark("in-development-iceberg.yml", ["NESSIE_DEMO_IN_DEV_ICEBERG_VERSION"])

    def test_with_spark_nessie_in_dev(self: object) -> None:
        """Test NessieDemo+Spark against an in-development version of Nessie built locally from source."""
        TestWithSpark.__test_with_spark(
            "in-development-nessie.yml",
            [
                "NESSIE_DEMO_IN_DEV_NESSIE_RUNNER",
                "NESSIE_DEMO_IN_DEV_NESSIE_VERSION",
                "NESSIE_DEMO_IN_DEV_PYNESSIE_PATH",
            ],
        )

    def test_with_spark_nessie_iceberg_in_dev(self: object) -> None:
        """Test NessieDemo+Spark against an in-development version of Nessie built locally from source."""
        TestWithSpark.__test_with_spark(
            "in-development-nessie-iceberg.yml",
            [
                "NESSIE_DEMO_IN_DEV_NESSIE_RUNNER",
                "NESSIE_DEMO_IN_DEV_NESSIE_VERSION",
                "NESSIE_DEMO_IN_DEV_ICEBERG_VERSION",
                "NESSIE_DEMO_IN_DEV_PYNESSIE_PATH",
            ],
        )
