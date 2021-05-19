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
from assertpy import assert_that

import pytest
import os

from testbook import testbook

""" Tests the Nessie + Iceberg + Flink Jupyter Notebook with the NBA dataset"""

salaries = """     Season                 Team     Salary          Player
0   2004-05   Los Angeles Lakers  $14175000     Kobe Bryant
1   2005-06   Los Angeles Lakers  $15946875     Kobe Bryant
2   2006-07   Los Angeles Lakers  $17718750     Kobe Bryant
3   1996-97        Chicago Bulls  $30140000  Michael Jordan
4   1997-98        Chicago Bulls  $33140000  Michael Jordan
5   2001-02   Washington Wizards   $1000000  Michael Jordan
6   2006-07  Cleveland Cavaliers   $5828090    Lebron James
7   2007-08  Cleveland Cavaliers  $13041250    Lebron James
8   2008-09  Cleveland Cavaliers  $14410581    Lebron James
9   2014-15   Los Angeles Lakers  $23500000     Kobe Bryant
10  2015-16   Los Angeles Lakers  $25000000     Kobe Bryant
11  1992-93        Chicago Bulls   $4000000  Michael Jordan
12  1993-94        Chicago Bulls   $4000000  Michael Jordan
13  1994-95        Chicago Bulls   $3850000  Michael Jordan
14  1995-96        Chicago Bulls   $3850000  Michael Jordan
15  2009-10  Cleveland Cavaliers  $15779912    Lebron James
16  2010-11           Miami Heat  $14500000    Lebron James
17  2011-12           Miami Heat  $16022500    Lebron James
18  1985-86        Chicago Bulls    $630000  Michael Jordan
19  1987-88        Chicago Bulls    $845000  Michael Jordan
20  1988-89        Chicago Bulls   $2000000  Michael Jordan
21  2012-13           Miami Heat  $17545000    Lebron James
22  2013-14           Miami Heat  $19067500    Lebron James
23  2014-15  Cleveland Cavaliers  $20644400    Lebron James
24  2015-16  Cleveland Cavaliers  $22971000    Lebron James
25  2010-11   Los Angeles Lakers  $24806250     Kobe Bryant
26  2011-12   Los Angeles Lakers  $25244493     Kobe Bryant
27  2012-13   Los Angeles Lakers  $27849149     Kobe Bryant
28  2013-14   Los Angeles Lakers  $30453805     Kobe Bryant
29  2016-17  Cleveland Cavaliers  $30963450    Lebron James
30  2017-18  Cleveland Cavaliers  $33285709    Lebron James
31  1984-85        Chicago Bulls    $550000  Michael Jordan
32  1989-90        Chicago Bulls   $2500000  Michael Jordan
33  1990-91        Chicago Bulls   $2500000  Michael Jordan
34  1991-92        Chicago Bulls   $3250000  Michael Jordan
35   Season                 Team     Salary          Player
36  2003-04  Cleveland Cavaliers   $4018920    Lebron James
37  2004-05  Cleveland Cavaliers   $4320360    Lebron James
38  2005-06  Cleveland Cavaliers   $4621800    Lebron James
39  2001-02   Los Angeles Lakers  $11250000     Kobe Bryant
40  2002-03   Los Angeles Lakers  $12375000     Kobe Bryant
41  2003-04   Los Angeles Lakers  $13500000     Kobe Bryant
42  2007-08   Los Angeles Lakers  $19490625     Kobe Bryant
43  2008-09   Los Angeles Lakers  $21262500     Kobe Bryant
44  2009-10   Los Angeles Lakers  $23034375     Kobe Bryant
45  1998-99   Los Angeles Lakers   $1319000     Kobe Bryant
46  1999-00   Los Angeles Lakers   $9000000     Kobe Bryant
47  2000-01   Los Angeles Lakers  $10130000     Kobe Bryant
48  2002-03   Washington Wizards   $1030000  Michael Jordan
49  1996-97   Los Angeles Lakers   $1015000     Kobe Bryant
50  1997-98   Los Angeles Lakers   $1167240     Kobe Bryant"""

num_salaries_on_experiment = """   EXPR$0
0     59"""

num_salaries_on_main = """   EXPR$0
0     55"""


@pytest.fixture(scope='module')
def notebook(tmpdir_factory):
    tmpdir = str(tmpdir_factory.mktemp("_assets"))
    os.environ["NESSIE_DEMO_ASSETS"] = tmpdir

    path = os.path.abspath("../..")
    if not os.path.exists(path):
        path = os.path.abspath(".")
    path_to_notebook = os.path.join(path, "colab/nessie-iceberg-flink-demo-nba.ipynb")
    with testbook(path_to_notebook, execute=True, timeout=300) as tb:
        yield tb


def test_notebook_output(notebook):
    """
    Runs through the entire notebook and checks the output

    :param notebook: The notebook to test
    :return:
    """
    assert_that(notebook.cell_output_text(2)).contains(
            "Dataset nba with files allstar_games_stats.csv, salaries.csv, totals_stats.csv")

    assert_that(notebook.cell_output_text(2)).contains("Starting Nessie")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(5)).contains("main").contains("dev")

    # table_env.from_path('nessie.nba.`salaries@dev`').to_pandas()
    assert_that(notebook.cell_output_text(8)).is_equal_to(salaries)

    # !nessie contents --list
    assert_that(notebook.cell_output_text(10)).is_empty()

    # !nessie contents --list --ref dev
    assert_that(notebook.cell_output_text(11)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.totals_stats")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(13)).contains("main").contains("dev")

    # table_env.from_path('nessie.nba.`allstar_games_stats@etl`').to_pandas()
    assert_that(notebook.cell_output_text(22)).is_equal_to(
        "allstar_games_stats")

    # !nessie contents --list
    assert_that(notebook.cell_output_text(22)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.totals_stats")

    # !nessie contents --list --ref etl
    assert_that(notebook.cell_output_text(23)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats\r\n\tnba.new_total_stats")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(25)).contains("main").contains(
            "dev").contains("etl")

    # !nessie contents --list --ref experiment
    assert_that(notebook.cell_output_text(30)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats")

    # !nessie contents --list
    assert_that(notebook.cell_output_text(31)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats\r\n\tnba.new_total_stats")

    # table_env.from_path('nessie.nba.`salaries@experiment`').select(lit(1).count).to_pandas()
    assert_that(notebook.cell_output_text(33)).is_equal_to(
            num_salaries_on_experiment)

    # table_env.from_path('nessie.nba.`salaries@main`').select(lit(1).count).to_pandas()
    assert_that(notebook.cell_output_text(35)).is_equal_to(num_salaries_on_main)


def test_dependency_setup(notebook):
    """
    Verifies that dependencies were correctly set up

    :param notebook: The notebook to test
    :return:
    """
    table_env = notebook.ref("table_env")
    demo_flink = notebook.ref("demo_flink")
    assert_that(table_env).is_not_none()
    assert_that(demo_flink).is_not_none()


def test_dataset_exists(notebook):
    """
    Verifies that the NBA dataset exists

    :param notebook: The notebook to test
    :return:
    """
    dataset = notebook.ref("dataset")
    assert_that(dataset.get("salaries.csv", None)).is_not_none()
    assert_that(dataset.get("allstar_games_stats.csv", None)).is_not_none()
    assert_that(dataset.get("totals_stats.csv", None)).is_not_none()
