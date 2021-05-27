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
from _pytest.fixtures import FixtureRequest
from _pytest.tmpdir import TempPathFactory
from assertpy import assert_that
from testbook import testbook
from testbook.client import TestbookNotebookClient
from tests import _find_notebook


@pytest.fixture(scope="module")
def notebook(tmpdir_factory: TempPathFactory, request: FixtureRequest) -> Generator:
    """Prepares nessiedemo env and yields `testbook`."""
    tmpdir = str(tmpdir_factory.mktemp("_assets"))
    os.environ["NESSIE_DEMO_ASSETS"] = tmpdir

    path_to_notebook = _find_notebook("nessie-iceberg-spark-setup.ipynb")

    # This demo downloads a Spark distribution into the current directory, so let's `cd` there before starting the test
    cwd = os.getcwd()
    os.chdir(str(tmpdir_factory.mktemp("test-iceberg-spark-setup")))
    request.addfinalizer(lambda: os.chdir(cwd))

    with testbook(path_to_notebook, timeout=300) as tb:
        tb.execute()
        yield tb


def test_notebook_output(notebook: TestbookNotebookClient) -> None:
    """Runs through the entire notebook and checks the output.

    :param notebook: The notebook to test
    :return:
    """
    assert_that(notebook.cell_output_text(2)).contains("Nessie running with PID")

    demo = notebook.ref("demo")

    # !nessie branch
    assert_that(notebook.cell_output_text(3)).contains("main")

    # Iceberg version
    assert_that(notebook.cell_output_text(6)).is_equal_to(
        f"Using Iceberg version {demo.get_iceberg_version()}"
    )

    # Spark packages
    assert_that(notebook.cell_output_text(8)).is_equal_to(
        f"org.apache.iceberg:iceberg-spark3-runtime:{demo.get_iceberg_version()}"
    )

    # Nessie URI
    assert_that(notebook.cell_output_text(10)).is_equal_to(demo.get_nessie_api_uri())
