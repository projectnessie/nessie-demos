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
from assertpy import assert_that
from pytest import fixture, skip

from nessiedemo.demo import setup_demo
from .util import demo_setup_fixture_for_tests, expect_error


@fixture(scope="function", autouse=True)
def before_all(tmpdir_factory, request) -> None:  # noqa: ANN001
    """Sets up env-vars to use a pytest temp-dir and use assets from the source-tree."""
    demo_setup_fixture_for_tests(tmpdir_factory, request)

    def __dispose_flink() -> None:
        from nessiedemo.flink import flink_dispose

        print("TEST-TEARDOWN: Disposing Flink Resources...")
        flink_dispose()

    request.addfinalizer(__dispose_flink)


class TestWithIcebergFlink:
    """Test NessieDemo with Iceberg + Flink."""

    @staticmethod
    def __test_with_iceberg_flink(versions_yaml: str, required_envs: list) -> None:
        """Test NessieDemo plus NessieDemoFlink."""
        if False in [e in os.environ for e in required_envs]:
            skip(
                "Missing mandatory environment variable(s) {} for in-development-yaml-test, skipping test".format(
                    ", ".join(["{}={}".format(e, os.environ[e] if e in os.environ else "<not set>") for e in required_envs])
                )
            )

        demo = setup_demo(versions_yaml)

        print("Nessie version: {}".format(demo.get_nessie_version()))
        print("Iceberg version: {}".format(demo.get_iceberg_version()))

        from nessiedemo.flink import flink_for_demo
        catalog_name = "test_with_flink"
        table_env, demo_flink = flink_for_demo(demo, catalog_name=catalog_name)
        assert_that(table_env).is_not_none()
        assert_that(demo_flink).is_not_none()

        run(["nessie", "branch", "dev"])  # noqa: S603 S607
        dev_table_env = demo_flink.table_env_for_ref("dev", catalog_name=catalog_name)
        assert_that(dev_table_env).is_not_none()

        dataset = demo.fetch_dataset("nba")


        from pyflink.table import DataTypes
        from pyflink.table.descriptors import Schema, OldCsv, FileSystem

        # Creating `salaries` table
        dev_table_env.connect(FileSystem().path(dataset['salaries.csv'])) \
            .with_format(OldCsv()
                         .field('Season', DataTypes.STRING()).field("Team", DataTypes.STRING())
                         .field("Salary", DataTypes.STRING()).field("Player", DataTypes.STRING())) \
            .with_schema(Schema()
                         .field('Season', DataTypes.STRING()).field("Team", DataTypes.STRING())
                         .field("Salary", DataTypes.STRING()).field("Player", DataTypes.STRING())) \
            .create_temporary_table(f'{catalog_name}.nba.salaries_temp')

        dev_table_env.execute_sql(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.nba.salaries (Season STRING, Team STRING, Salary STRING, Player STRING)").wait()

        tab = dev_table_env.from_path(f'{catalog_name}.nba.salaries_temp')
        tab.select(tab.Season, tab.Team, tab.Salary, tab.Player).execute_insert(f'{catalog_name}.nba.salaries').wait()

        # Creating `totals_stats` table
        dev_table_env.connect(FileSystem().path(dataset['totals_stats.csv'])) \
            .with_format(OldCsv()
                         .field('Season', DataTypes.STRING()).field("Age", DataTypes.STRING()).field("Team", DataTypes.STRING())
                         .field("ORB", DataTypes.STRING()).field("DRB", DataTypes.STRING()).field("TRB", DataTypes.STRING())
                         .field("AST", DataTypes.STRING()).field("STL", DataTypes.STRING()).field("BLK", DataTypes.STRING())
                         .field("TOV", DataTypes.STRING()).field("PTS", DataTypes.STRING()).field("Player", DataTypes.STRING())
                         .field("RSorPO", DataTypes.STRING())) \
            .with_schema(Schema()
                         .field('Season', DataTypes.STRING()).field("Age", DataTypes.STRING()).field("Team", DataTypes.STRING())
                         .field("ORB", DataTypes.STRING()).field("DRB", DataTypes.STRING()).field("TRB", DataTypes.STRING())
                         .field("AST", DataTypes.STRING()).field("STL", DataTypes.STRING()).field("BLK", DataTypes.STRING())
                         .field("TOV", DataTypes.STRING()).field("PTS", DataTypes.STRING()).field("Player", DataTypes.STRING())
                         .field("RSorPO", DataTypes.STRING())) \
            .create_temporary_table(f'{catalog_name}.nba.totals_stats_temp')

        dev_table_env.execute_sql(
            f"CREATE TABLE IF NOT EXISTS {catalog_name}.nba.totals_stats (Season STRING, Age STRING, Team STRING, \
            ORB STRING, DRB STRING, TRB STRING, AST STRING, STL STRING, BLK STRING, TOV STRING, PTS STRING, \
            Player STRING, RSorPO STRING)").wait()

        tab = dev_table_env.from_path(f'{catalog_name}.nba.totals_stats_temp')
        tab.select(tab.Season, tab.Age, tab.Team, tab.ORB, tab.DRB, tab.TRB,
                   tab.AST, tab.STL, tab.BLK, tab.TOV, tab.PTS, tab.Player, tab.RSorPO).execute_insert(f'{catalog_name}.nba.totals_stats').wait()

        from pyflink.table.expressions import lit
        num_salaries = table_env.from_path(f'{catalog_name}.nba.`salaries@dev`').select(lit(1).count).to_pandas().to_string()

        assert_that(num_salaries).contains("51")
        assert_that(
          num_salaries).is_equal_to(dev_table_env.from_path(f'{catalog_name}.nba.salaries').select(lit(1).count).to_pandas().to_string())

        # verify that the table does not exist on the main branch
        expect_error("py4j.protocol.Py4JJavaError", lambda: table_env.from_path(f'{catalog_name}.nba.salaries'))
        expect_error("py4j.protocol.Py4JJavaError", lambda: table_env.from_path(f'{catalog_name}.nba.`salaries@main`'))

        run(["nessie", "merge", "dev", "-b", "main", "--force"])  # noqa: S603 S607

        assert_that(table_env.from_path(f'{catalog_name}.nba.`salaries@main`').select(lit(1).count).to_pandas().to_string()).is_equal_to(num_salaries)

    @pytest.mark.skip("Skipped until we get a new Iceberg release")
    @pytest.mark.forked
    def test_with_iceberg_flink(self: object) -> None:
        """Test NessieDemo+Iceberg+Flink against Nessie 0.6.x + Iceberg 0.11.x."""
        TestWithIcebergFlink.__test_with_iceberg_flink("nessie-0.6-iceberg-flink-0.11.yml", [])
