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
"""Tests the Nessie + Iceberg + Spark Jupyter Notebook with the NBA dataset."""
import os
from typing import Generator

import pytest
from _pytest.tmpdir import TempPathFactory
from assertpy import assert_that
from testbook import testbook
from testbook.client import TestbookNotebookClient
from tests import _find_notebook


salaries = """+-------+-------------------+---------+--------------+
| Season|               Team|   Salary|        Player|
+-------+-------------------+---------+--------------+
|2003-04|Cleveland Cavaliers| $4018920|  Lebron James|
|2004-05|Cleveland Cavaliers| $4320360|  Lebron James|
|2005-06|Cleveland Cavaliers| $4621800|  Lebron James|
|2006-07|Cleveland Cavaliers| $5828090|  Lebron James|
|2007-08|Cleveland Cavaliers|$13041250|  Lebron James|
|2008-09|Cleveland Cavaliers|$14410581|  Lebron James|
|2009-10|Cleveland Cavaliers|$15779912|  Lebron James|
|2010-11|         Miami Heat|$14500000|  Lebron James|
|2011-12|         Miami Heat|$16022500|  Lebron James|
|2012-13|         Miami Heat|$17545000|  Lebron James|
|2013-14|         Miami Heat|$19067500|  Lebron James|
|2014-15|Cleveland Cavaliers|$20644400|  Lebron James|
|2015-16|Cleveland Cavaliers|$22971000|  Lebron James|
|2016-17|Cleveland Cavaliers|$30963450|  Lebron James|
|2017-18|Cleveland Cavaliers|$33285709|  Lebron James|
|1984-85|      Chicago Bulls|  $550000|Michael Jordan|
|1985-86|      Chicago Bulls|  $630000|Michael Jordan|
|1987-88|      Chicago Bulls|  $845000|Michael Jordan|
|1988-89|      Chicago Bulls| $2000000|Michael Jordan|
|1989-90|      Chicago Bulls| $2500000|Michael Jordan|
+-------+-------------------+---------+--------------+
only showing top 20 rows"""

num_salaries_on_experiment = """+--------+
|count(1)|
+--------+
|      58|
+--------+"""

num_salaries_on_main = """+--------+
|count(1)|
+--------+
|      54|
+--------+"""


@pytest.fixture(scope="module")
def notebook(tmpdir_factory: TempPathFactory) -> Generator:
    """Prepares nessiedemo env and yields `testbook`."""
    tmpdir = str(tmpdir_factory.mktemp("_assets"))
    os.environ["NESSIE_DEMO_ASSETS"] = tmpdir

    path_to_notebook = _find_notebook("nessie-delta-demo-nba.ipynb")

    with testbook(path_to_notebook, timeout=300) as tb:
        tb.execute()
        yield tb


@pytest.mark.skip("Skipped until necessary Nessie PR is in")
def test_notebook_output(notebook: TestbookNotebookClient) -> None:
    """Runs throuhg the entire notebook and checks the output.

    :param notebook: The notebook to test
    :return:
    """
    assert_that(notebook.cell_output_text(2)).contains(
        "Dataset nba with files allstar_games_stats.csv, salaries.csv, totals_stats.csv"
    )

    assert_that(notebook.cell_output_text(2)).contains("Starting Nessie")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(5)).contains("main").contains("dev")

    # spark.sql("select * from nessie.nba.`salaries@dev`").show()
    assert_that(notebook.cell_output_text(8)).is_equal_to(salaries)

    # !nessie contents --list
    assert_that(notebook.cell_output_text(10)).is_empty()

    # !nessie contents --list --ref dev
    assert_that(notebook.cell_output_text(11)).contains("DELTA_LAKE_TABLE:").contains(
        "spark_warehouse.delta.nessie_nba_salaries._delta_log"
    ).contains("spark_warehouse.delta.nessie_nba_totals_stats._delta_log")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(13)).contains("main").contains("dev")

    # !nessie contents --list
    assert_that(notebook.cell_output_text(21)).contains("DELTA_LAKE_TABLE:").contains(
        "spark_warehouse.delta.nessie_nba_salaries._delta_log"
    ).contains("spark_warehouse.delta.nessie_nba_totals_stats._delta_log")

    # !nessie contents --list --ref etl
    assert_that(notebook.cell_output_text(22)).contains("DELTA_LAKE_TABLE:").contains(
        "spark_warehouse.delta.nessie_nba_salaries._delta_log"
    ).contains(
        "spark_warehouse.delta.nessie_nba_allstar_games_stats._delta_log"
    ).contains(
        "spark_warehouse.delta.nessie_nba_totals_stats._delta_log"
    )

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(24)).contains("main").contains(
        "dev"
    ).contains("etl")

    # !nessie contents --list --ref experiment
    assert_that(notebook.cell_output_text(29)).contains("DELTA_LAKE_TABLE:").contains(
        "spark_warehouse.delta.nessie_nba_salaries._delta_log"
    ).contains("spark_warehouse.delta.nessie_nba_allstar_games_stats._delta_log")

    # !nessie contents --list
    assert_that(notebook.cell_output_text(30)).contains("DELTA_LAKE_TABLE:").contains(
        "spark_warehouse.delta.nessie_nba_salaries._delta_log"
    ).contains(
        "spark_warehouse.delta.nessie_nba_allstar_games_stats._delta_log"
    ).contains(
        "spark_warehouse.delta.nessie_nba_totals_stats._delta_log"
    )

    # spark.sql("select count(*) from nessie.nba.`salaries@experiment`").show()
    assert_that(notebook.cell_output_text(32)).is_equal_to(num_salaries_on_experiment)

    # 'spark.sql("select count(*) from nessie.nba.salaries").show()'
    assert_that(notebook.cell_output_text(34)).is_equal_to(num_salaries_on_main)


@pytest.mark.skip("Skipped until necessary Nessie PR is in")
def test_dependency_setup(notebook: TestbookNotebookClient) -> None:
    """Verifies that dependencies were correctly set up.

    :param notebook: The notebook to test
    :return:
    """
    spark = notebook.ref("spark")
    demo_delta_spark = notebook.ref("demo_delta_spark")
    assert_that(spark).is_not_none()
    assert_that(demo_delta_spark).is_not_none()


@pytest.mark.skip("Skipped until necessary Nessie PR is in")
def test_dataset_exists(notebook: TestbookNotebookClient) -> None:
    """Verifies that the NBA dataset exists.

    :param notebook: The notebook to test
    :return:
    """
    dataset = notebook.ref("dataset")
    assert_that(dataset.get("salaries.csv", None)).is_not_none()
    assert_that(dataset.get("allstar_games_stats.csv", None)).is_not_none()
    assert_that(dataset.get("totals_stats.csv", None)).is_not_none()
