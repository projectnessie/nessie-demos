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

""" Tests the Nessie + Iceberg + Spark Jupyter Notebook with the NBA dataset"""

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


@pytest.fixture(scope='module')
def notebook():
    path = os.path.abspath("../..")
    if not os.path.exists(path):
        path = os.path.abspath(".")
    path_to_notebook = os.path.join(path, "colab/nessie-iceberg-demo-nba.ipynb")
    with testbook(path_to_notebook, execute=True, timeout=300) as tb:
        yield tb


def test_notebook_output(notebook):
    """
    Runs throuhg the entire notebook and checks the output

    :param notebook: The notebook to test
    :return:
    """
    assert_that(notebook.cell_output_text(2)).contains(
            "Dataset nba with files allstar_games_stats.csv, salaries.csv, totals_stats.csv")

    assert_that(notebook.cell_output_text(2)).contains("Starting Nessie")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(5)).contains("main").contains("dev")

    # spark.sql("select * from nessie.nba.`salaries@dev`").show()
    assert_that(notebook.cell_output_text(8)).is_equal_to(salaries)

    # !nessie contents --list
    assert_that(notebook.cell_output_text(10)).is_empty()

    # !nessie contents --list --ref dev
    assert_that(notebook.cell_output_text(11)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.totals_stats")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(13)).contains("main").contains("dev")

    # !nessie contents --list
    assert_that(notebook.cell_output_text(22)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.totals_stats")

    # !nessie contents --list --ref etl
    assert_that(notebook.cell_output_text(23)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats\r\n\tnba.totals_stats")

    # !nessie --verbose branch
    assert_that(notebook.cell_output_text(25)).contains("main").contains(
            "dev").contains("etl")

    # !nessie contents --list --ref experiment
    assert_that(notebook.cell_output_text(30)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats")

    # !nessie contents --list
    assert_that(notebook.cell_output_text(31)).is_equal_to(
            "ICEBERG_TABLE:\r\n\tnba.salaries\r\n\tnba.allstar_games_stats\r\n\tnba.totals_stats")

    # spark.sql("select count(*) from nessie.nba.`salaries@experiment`").show()
    assert_that(notebook.cell_output_text(33)).is_equal_to(
            num_salaries_on_experiment)

    # 'spark.sql("select count(*) from nessie.nba.salaries").show()'
    assert_that(notebook.cell_output_text(35)).is_equal_to(num_salaries_on_main)


def test_dependency_setup(notebook):
    """
    Verifies that dependencies were correctly set up

    :param notebook: The notebook to test
    :return:
    """
    spark = notebook.ref("spark")
    demo_spark = notebook.ref("demo_spark")
    assert_that(spark).is_not_none()
    assert_that(demo_spark).is_not_none()


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


@pytest.mark.skip(reason="config seems to be pointing to main for all sessions")
def test_spark_sessions_point_to_correct_nessie_branch(notebook):
    """
    Verifies that the different spark sessions point to the correct
    nessie branches

    :param notebook: The notebook to test
    :return:
    """
    spark_dev = notebook.ref("spark_dev")
    spark_etl = notebook.ref("spark_etl")
    spark_experiment = notebook.ref("spark_experiment")

    assert_that(spark_dev.sparkContext.getConf().get(
            "spark.sql.catalog.nessie.ref")).is_equal_to("dev")
    assert_that(spark_etl.sparkContext.getConf().get(
            "spark.sql.catalog.nessie.ref")).is_equal_to("etl")
    assert_that(spark_experiment.sparkContext.getConf().get(
            "spark.sql.catalog.nessie.ref")).is_equal_to("experiment")
