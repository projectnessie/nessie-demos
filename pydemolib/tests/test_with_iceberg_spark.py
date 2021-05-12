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

import pytest
from pytest import fixture, skip

from nessiedemo.demo import setup_demo
from .util import demo_setup_fixture_for_tests, expect_error


@fixture(scope="function", autouse=True)
def before_all(tmpdir_factory, request) -> None:  # noqa: ANN001
    """Sets up env-vars to use a pytest temp-dir and use assets from the source-tree."""
    demo_setup_fixture_for_tests(tmpdir_factory, request)

    def __dispose_iceberg() -> None:
        from nessiedemo.iceberg_spark import iceberg_dispose

        print("TEST-TEARDOWN: Disposing SparkContext...")
        iceberg_dispose()

    request.addfinalizer(__dispose_iceberg)


class TestWithIcebergSpark:
    """Test NessieDemo with Iceberg + Spark."""

    @staticmethod
    def __test_with_iceberg_spark(versions_yaml: str, required_envs: list) -> None:
        """Test NessieDemo plus NessieDemoIcebergSpark."""
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
        from nessiedemo.iceberg_spark import iceberg_spark_for_demo

        spark, sc, jvm, demo_iceberg = iceberg_spark_for_demo(demo, catalog_name="test_with_iceberg")
        assert spark.conf.get("spark.sql.catalog.test_with_iceberg.ref") == "main"
        assert spark.conf.get("spark.sql.catalog.test_with_iceberg.url") == demo.get_nessie_api_uri()
        assert spark.conf.get("spark.jars.packages") == "org.apache.iceberg:iceberg-spark3-runtime:" + demo.get_iceberg_version()
        assert sc is not None
        assert jvm is not None

        spark_dev = demo_iceberg.session_for_ref("dev", catalog_name="test_with_iceberg")
        assert spark_dev.conf.get("spark.sql.catalog.test_with_iceberg.ref") == "dev"
        assert spark_dev.conf.get("spark.sql.catalog.test_with_iceberg.url") == demo.get_nessie_api_uri()
        assert spark_dev.conf.get("spark.jars.packages") == "org.apache.iceberg:iceberg-spark3-runtime:" + demo.get_iceberg_version()

        run(["nessie", "branch", "dev"])  # noqa: S603 S607

        dataset = demo.fetch_dataset("region-nation")

        # Creating region table
        spark_dev.sql("CREATE TABLE test_with_iceberg.testing.region (R_REGIONKEY LONG, R_NAME STRING, R_COMMENT STRING) USING iceberg")

        assert spark_dev.sql("SELECT COUNT(*) FROM test_with_iceberg.testing.region").collect()[0][0] == 0

        region_df = spark_dev.read.load(dataset["region.parquet"])
        region_df.write.format("iceberg").mode("overwrite").save("test_with_iceberg.testing.region")

        assert spark_dev.sql("SELECT COUNT(*) FROM test_with_iceberg.testing.region").collect()[0][0] == 5

        # Verify that the table does not exist on the main branch
        expect_error("pyspark.sql.utils.AnalysisException", lambda: spark.sql("SELECT COUNT(*) FROM test_with_iceberg.testing.region"))
        expect_error(
            "pyspark.sql.utils.AnalysisException", lambda: spark_dev.sql("SELECT COUNT(*) FROM test_with_iceberg.testing.`region@main`")
        )

        assert spark.sql("SELECT COUNT(*) FROM test_with_iceberg.testing.`region@dev`").collect()[0][0] == 5

        run(["nessie", "merge", "dev", "-b", "main", "--force"])  # noqa: S603 S607

        assert spark.sql("SELECT COUNT(*) FROM test_with_iceberg.testing.region").collect()[0][0] == 5

    @pytest.mark.forked
    def test_with_iceberg_spark(self: object) -> None:
        """Test NessieDemo+Iceberg+Spark against Nessie 0.5.x + Iceberg 0.11.x."""
        TestWithIcebergSpark.__test_with_iceberg_spark("nessie-0.5-iceberg-0.11.yml", [])

    @pytest.mark.forked
    def test_with_iceberg_spark_iceberg_in_dev(self: object) -> None:
        """Test NessieDemo+Iceberg+Spark against an in-development version of Iceberg built locally from source."""
        TestWithIcebergSpark.__test_with_iceberg_spark("in-development/iceberg.yml", ["NESSIE_DEMO_IN_DEV_ICEBERG_VERSION"])

    @pytest.mark.forked
    def test_with_iceberg_spark_nessie_in_dev(self: object) -> None:
        """Test NessieDemo+Iceberg+Spark against an in-development version of Nessie built locally from source."""
        TestWithIcebergSpark.__test_with_iceberg_spark(
            "in-development/nessie.yml",
            [
                "NESSIE_DEMO_IN_DEV_NESSIE_RUNNER",
                "NESSIE_DEMO_IN_DEV_NESSIE_VERSION",
                "NESSIE_DEMO_IN_DEV_PYNESSIE_PATH",
            ],
        )

    @pytest.mark.forked
    def test_with_iceberg_spark_nessie_iceberg_in_dev(self: object) -> None:
        """Test NessieDemo+Iceberg+Spark against an in-development version of Nessie built locally from source."""
        TestWithIcebergSpark.__test_with_iceberg_spark(
            "in-development/nessie-iceberg.yml",
            [
                "NESSIE_DEMO_IN_DEV_NESSIE_RUNNER",
                "NESSIE_DEMO_IN_DEV_NESSIE_VERSION",
                "NESSIE_DEMO_IN_DEV_ICEBERG_VERSION",
                "NESSIE_DEMO_IN_DEV_PYNESSIE_PATH",
            ],
        )
