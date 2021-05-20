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


@pytest.fixture(scope='module')
def notebook(tmpdir_factory, request):
    tmpdir = str(tmpdir_factory.mktemp("_assets"))
    os.environ["NESSIE_DEMO_ASSETS"] = tmpdir

    path = os.path.abspath("../..")
    if not os.path.exists(path):
        path = os.path.abspath(".")
    path_to_notebook = os.path.join(path, "colab/nessie-iceberg-spark-setup.ipynb")

    # This demo downloads a Spark distribution into the current directory, so let's `cd` there before starting the test
    cwd = os.getcwd()
    os.chdir(str(tmpdir_factory.mktemp("test-iceberg-spark-setup")))
    request.addfinalizer(lambda: os.chdir(cwd))

    with testbook(path_to_notebook, timeout=300) as tb:
        tb.execute()
        yield tb


def test_notebook_output(notebook):
    """
    Runs through the entire notebook and checks the output

    :param notebook: The notebook to test
    :return:
    """
    assert_that(notebook.cell_output_text(2)).contains(
            "Nessie running with PID")

    demo = notebook.ref("demo")

    # !nessie branch
    assert_that(notebook.cell_output_text(3)).contains("main")

    # !pip show pyspark
    assert_that(notebook.cell_output_text(6)).contains("Location: ").contains("Summary: Apache Spark Python API")

    # Find Spark dist via pyspark
    spark_dir = notebook.ref("spark_dir")
    assert_that(spark_dir).is_not_none()
    assert_that(notebook.cell_output_text(9)).contains(f"Found Spark distribution in ")

    # Spark dist download URL
    spark_download_url = notebook.ref("spark_download_url")
    assert_that(spark_download_url).is_not_none()
    assert_that(notebook.cell_output_text(11)).contains(f"Spark distribution download URL is {spark_download_url}")

    # Download Spark dist (cell 12)
    spark_file_name = notebook.ref("spark_file_name")
    assert_that(os.path.isfile(spark_file_name))
    spark_dir_name = notebook.ref("spark_dir_name")

    # !ls -al .
    assert_that(notebook.cell_output_text(13)).contains(spark_file_name)

    # tar xf (cell 14)

    # !ls -al . (cell 15)
    assert_that(notebook.cell_output_text(15)).contains(spark_dir_name)
    assert_that(os.path.isdir(spark_dir_name))

    # User info
    assert_that(notebook.cell_output_text(16)).contains(f"Extracted Spark distribution in {os.path.abspath(spark_dir_name)}")
    assert_that(spark_dir).is_equal_to(os.path.abspath(spark_dir_name))

    # Check SPARK_HOME
    assert_that(notebook.cell_output_text(18)).is_equal_to(spark_dir)

    # Iceberg version
    assert_that(notebook.cell_output_text(22)).is_equal_to(f"Using Iceberg version {demo.get_iceberg_version()}")

    # Spark packages
    assert_that(notebook.cell_output_text(24)).is_equal_to(f"org.apache.iceberg:iceberg-spark3-runtime:{demo.get_iceberg_version()}")

    # Nessie URI
    assert_that(notebook.cell_output_text(26)).is_equal_to(demo.get_nessie_api_uri())
