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
import sys
from subprocess import run  # noqa: S404

import pytest
from pytest import fixture, skip

from nessiedemo.demo import setup_demo
from .util import demo_setup_fixture_for_tests, expect_error


__anything_done: bool = False


@fixture(scope="function", autouse=True)
def before_all(tmpdir_factory, request) -> None:  # noqa: ANN001
    """Sets up env-vars to use a pytest temp-dir and use assets from the source-tree."""
    global __anything_done

    demo_setup_fixture_for_tests(tmpdir_factory, request)

    def __dispose_spark() -> None:
        if __anything_done:
            # Should not do this import, which would import pyspark, which fails, if we're running the wrong Python version
            from nessiedemo.delta_spark import delta_spark_dispose

            print("TEST-TEARDOWN: Disposing SparkContext...")
            delta_spark_dispose()

    request.addfinalizer(__dispose_spark)


class TestWithDelta:
    """Test NessieDemo with Deltlake."""

    @staticmethod
    def __test_with_delta(versions_yaml: str, spark_version: int, required_envs: list) -> None:
        """Test NessieDemo plus NessieDemoDelta."""
        global __anything_done

        if False in [e in os.environ for e in required_envs]:
            skip(
                "Missing mandatory environment variable(s) {} for in-development-yaml-test, skipping test".format(
                    ", ".join(["{}={}".format(e, os.environ[e] if e in os.environ else "<not set>") for e in required_envs])
                )
            )

        __anything_done = True

        demo = setup_demo(versions_yaml)

        print("Nessie version: {}".format(demo.get_nessie_version()))

        # Same with notebooks: must NOT import nessiedemo.spark BEFORE the demo's setup has "pip-install-ed" the spark dependencies
        from nessiedemo.delta_spark import delta_spark_for_demo

        spark, sc, jvm, demo_delta = delta_spark_for_demo(demo, spark_version=spark_version)
        assert spark.conf.get("spark.hadoop.nessie.ref") == "main"
        assert spark.conf.get("spark.hadoop.nessie.url") == demo.get_nessie_api_uri()
        assert spark.conf.get("spark.jars.packages") == "org.projectnessie:nessie-deltalake-spark{}:{}".format(
            spark_version, demo.get_nessie_version()
        )
        assert sc is not None
        assert jvm is not None

        run(["nessie", "branch", "dev"])  # noqa: S603 S607
        demo_delta.change_ref("dev")

        dataset = demo.fetch_dataset("region-nation")

        region_path = demo_delta.table_path("testing/region")

        region_df = spark.read.load(dataset["region.parquet"])
        region_df.write.format("delta").mode("overwrite").option("hadoop.nessie.ref", "dev").save(region_path)

        spark.sql("CREATE TABLE region USING delta LOCATION '{}'".format(region_path))

        assert spark.sql("SELECT COUNT(*) FROM region").collect()[0][0] == 5

        # Verify that the table does not exist on the main branch
        demo_delta.change_ref("main")
        # TODO the following fails with Delta 0.6!
        expect_error("pyspark.sql.utils.AnalysisException", lambda: spark.sql("SELECT COUNT(*) FROM region"))

        run(["nessie", "merge", "dev", "-b", "main", "--force"])  # noqa: S603 S607

        demo_delta.change_ref("main")
        assert spark.sql("SELECT COUNT(*) FROM region").collect()[0][0] == 5

    @pytest.mark.skip("Skipped until necessary Nessie PR is in")
    @pytest.mark.forked
    def test_with_delta_spark3(self: object) -> None:
        """Test NessieDemo+Spark against Nessie 0.6."""
        TestWithDelta.__test_with_delta("in-development/nessie-0.6-delta-spark3.yml", 3, [])

    # TODO figure out why the 'expect_error' checking that the 'region' table does not exist on the main branch fails,
    #  the table seems to exist on the main branch although it's created on the dev branch
    @pytest.mark.skip("Delta/Spark2 behaves differently")
    @pytest.mark.forked
    def test_with_delta_spark2(self: object) -> None:
        """Test NessieDemo+Spark against Nessie 0.6."""
        if sys.version_info >= (3, 8):
            skip("The necessary configuration requires pyspark==2.4.x, which does not work with Python > 3.7")
        TestWithDelta.__test_with_delta("in-development/nessie-0.6-delta-spark2.yml", 2, [])
