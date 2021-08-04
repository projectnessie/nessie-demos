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
"""Tests the Nessie + Iceberg + Flink Jupyter Notebook with the NBA dataset."""
import os
from typing import Generator

import pytest
from _pytest.tmpdir import TempPathFactory
from assertpy import assert_that
from testbook import testbook
from testbook.client import TestbookNotebookClient
from tests import _find_notebook


@pytest.fixture(scope="module")
def notebook(tmpdir_factory: TempPathFactory) -> Generator:
    """Prepares nessiedemo env and yields `testbook`."""
    tmpdir = str(tmpdir_factory.mktemp("_assets"))
    os.environ["NESSIE_DEMO_ASSETS"] = tmpdir

    path_to_notebook = _find_notebook("nessie-iceberg-flink-demo-nba.ipynb")

    with testbook(path_to_notebook, timeout=300) as tb:
        tb.execute()
        yield tb


def test_notebook_output(notebook: TestbookNotebookClient) -> None:
    """Runs through the entire notebook and checks the output.

    :param notebook: The notebook to test
    :return:
    """
    assert_that(notebook.cell_output_text(2)).contains(
        "Dataset nba with files allstar_games_stats.csv, salaries.csv, totals_stats.csv"
    )

    assert_that(notebook.cell_output_text(2)).contains("Starting Nessie")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(5)).contains("main").contains("dev")

    # table_env.from_path('nessie.nba.`salaries@dev`').to_pandas()
    # assert_that(notebook.cell_output_text(8)).is_equal_to(salaries)
    # this is just a workaround because to_pandas() output somehow isn't
    # returned by notebook.cell_output_text(8)
    assert_that(notebook.cell_output_text(8)).contains(
        "2007-08   Los Angeles Lakers  $19490625     Kobe Bryant"
    )

    # !nessie contents --list
    assert_that(notebook.cell_output_text(10)).is_empty()

    # !nessie contents --list --ref dev
    assert_that(notebook.cell_output_text(11)).is_equal_to(
        "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.totals_stats"
    )

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(13)).contains("main").contains("dev")

    # table_env.from_path('nessie.nba.`allstar_games_stats@etl`').to_pandas()
    # assert_that(notebook.cell_output_text(22)).is_equal_to(
    #    "fixme_pandas_output_isnt_shown")
    assert_that(notebook.cell_output_text(22)).contains(
        "1997-98   19   LAL    2    6    1    2    0    1   1   18     Kobe Bryant"
    )

    # !nessie contents --list
    assert_that(notebook.cell_output_text(23)).is_equal_to(
        "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.totals_stats"
    )

    # !nessie contents --list --ref etl
    assert_that(notebook.cell_output_text(24)).is_equal_to(
        "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats\r\n\tnba.new_total_stats"
    )

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(26)).contains("main").contains(
        "dev"
    ).contains("etl")

    # !nessie contents --list --ref experiment
    assert_that(notebook.cell_output_text(31)).is_equal_to(
        "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats"
    )

    # !nessie contents --list
    assert_that(notebook.cell_output_text(32)).is_equal_to(
        "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats\r\n\tnba.new_total_stats"
    )

    # table_env.from_path('nessie.nba.`salaries@experiment`').select(lit(1).count).to_pandas()
    assert_that(notebook.cell_output_text(34)).contains("59")

    # table_env.from_path('nessie.nba.`salaries@main`').select(lit(1).count).to_pandas()
    assert_that(notebook.cell_output_text(36)).contains("55")


def test_dependency_setup(notebook: TestbookNotebookClient) -> None:
    """Verifies that dependencies were correctly set up.

    :param notebook: The notebook to test
    :return:
    """
    table_env = notebook.ref("table_env")
    demo_flink = notebook.ref("demo_flink")
    assert_that(table_env).is_not_none()
    assert_that(demo_flink).is_not_none()


def test_dataset_exists(notebook: TestbookNotebookClient) -> None:
    """Verifies that the NBA dataset exists.

    :param notebook: The notebook to test
    :return:
    """
    dataset = notebook.ref("dataset")
    assert_that(dataset.get("salaries.csv", None)).is_not_none()
    assert_that(dataset.get("allstar_games_stats.csv", None)).is_not_none()
    assert_that(dataset.get("totals_stats.csv", None)).is_not_none()
