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


salaries = """Season                 Team     Salary          Player
0   2003-04  Cleveland Cavaliers   $4018920    Lebron James
1   2004-05  Cleveland Cavaliers   $4320360    Lebron James
2   2005-06  Cleveland Cavaliers   $4621800    Lebron James
3   2006-07  Cleveland Cavaliers   $5828090    Lebron James
4   2007-08  Cleveland Cavaliers  $13041250    Lebron James
5   2008-09  Cleveland Cavaliers  $14410581    Lebron James
6   2009-10  Cleveland Cavaliers  $15779912    Lebron James
7   2010-11           Miami Heat  $14500000    Lebron James
8   2011-12           Miami Heat  $16022500    Lebron James
9   2012-13           Miami Heat  $17545000    Lebron James
10  2013-14           Miami Heat  $19067500    Lebron James
11  2014-15  Cleveland Cavaliers  $20644400    Lebron James
12  2015-16  Cleveland Cavaliers  $22971000    Lebron James
13  2016-17  Cleveland Cavaliers  $30963450    Lebron James
14  2017-18  Cleveland Cavaliers  $33285709    Lebron James
15  1984-85        Chicago Bulls    $550000  Michael Jordan
16  1985-86        Chicago Bulls    $630000  Michael Jordan
17  1987-88        Chicago Bulls    $845000  Michael Jordan
18  1988-89        Chicago Bulls   $2000000  Michael Jordan
19  1989-90        Chicago Bulls   $2500000  Michael Jordan
20  1990-91        Chicago Bulls   $2500000  Michael Jordan
21  1991-92        Chicago Bulls   $3250000  Michael Jordan
22  1992-93        Chicago Bulls   $4000000  Michael Jordan
23  1993-94        Chicago Bulls   $4000000  Michael Jordan
24  1994-95        Chicago Bulls   $3850000  Michael Jordan
25  1995-96        Chicago Bulls   $3850000  Michael Jordan
26  1996-97        Chicago Bulls  $30140000  Michael Jordan
27  1997-98        Chicago Bulls  $33140000  Michael Jordan
28  2001-02   Washington Wizards   $1000000  Michael Jordan
29  2002-03   Washington Wizards   $1030000  Michael Jordan
30  1996-97   Los Angeles Lakers   $1015000     Kobe Bryant
31  1997-98   Los Angeles Lakers   $1167240     Kobe Bryant
32  1998-99   Los Angeles Lakers   $1319000     Kobe Bryant
33  1999-00   Los Angeles Lakers   $9000000     Kobe Bryant
34  2000-01   Los Angeles Lakers  $10130000     Kobe Bryant
35  2001-02   Los Angeles Lakers  $11250000     Kobe Bryant
36  2002-03   Los Angeles Lakers  $12375000     Kobe Bryant
37  2003-04   Los Angeles Lakers  $13500000     Kobe Bryant
38  2004-05   Los Angeles Lakers  $14175000     Kobe Bryant
39  2005-06   Los Angeles Lakers  $15946875     Kobe Bryant
40  2006-07   Los Angeles Lakers  $17718750     Kobe Bryant
41  2007-08   Los Angeles Lakers  $19490625     Kobe Bryant
42  2008-09   Los Angeles Lakers  $21262500     Kobe Bryant
43  2009-10   Los Angeles Lakers  $23034375     Kobe Bryant
44  2010-11   Los Angeles Lakers  $24806250     Kobe Bryant
45  2011-12   Los Angeles Lakers  $25244493     Kobe Bryant
46  2012-13   Los Angeles Lakers  $27849149     Kobe Bryant
47  2013-14   Los Angeles Lakers  $30453805     Kobe Bryant
48  2014-15   Los Angeles Lakers  $23500000     Kobe Bryant
49  2015-16   Los Angeles Lakers  $25000000     Kobe Bryant"""

num_salaries_on_experiment = """count(1)
0        58"""

num_salaries_on_main = """count(1)
0        54"""


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
