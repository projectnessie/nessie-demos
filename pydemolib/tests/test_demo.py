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
"""Tests for `nessiedemo` package.

Tests in this module are rather slow, because the nessie-quarkus-runner and Spark tarball are downloaded.
"""
import os

from pytest import fixture

from nessiedemo.demo import NessieDemo, setup_demo
from .util import demo_setup_fixture_for_tests


@fixture(scope="module", autouse=True)
def before_all(tmpdir_factory, request) -> None:  # noqa: ANN001
    """Sets up env-vars to use a pytest temp-dir and use assets from the source-tree."""
    demo_setup_fixture_for_tests(tmpdir_factory, request)


class TestNessieDemo:
    """Tests for `NessieDemo`."""

    def test_new_instance_can_kill_nessie(self: object) -> None:
        """Ensure config cli option is consistent."""
        demo = NessieDemo("nessie-0.5-iceberg-0.11.yml")

        assert demo.get_nessie_version() == "0.5.1"
        assert demo.get_iceberg_version() == "0.11.1"
        assert "pyspark==3.0.2" in demo._get_versions_dict()["python_dependencies"]

        ds_nba = demo.fetch_dataset("nba")
        assert len(ds_nba) == 3
        for k, v in ds_nba.items():
            source_file = os.path.join(os.environ["NESSIE_DEMO_ROOT"][7:], "datasets", "nba", k)
            assert source_file != v
            assert os.path.exists(v)
            assert os.path.getsize(v) == os.path.getsize(source_file)

        demo.start()
        assert demo.is_nessie_running()
        pid = demo._get_pid()
        assert pid > 0

        demo.stop()
        assert not demo.is_nessie_running()
        assert demo._get_pid() == -1

        demo.start()
        assert demo.is_nessie_running()
        pid = demo._get_pid()
        assert pid > 0

        demo2 = NessieDemo("nessie-0.5-iceberg-0.11.yml")
        pid2 = demo2._get_pid()
        assert pid == pid2

        ds_nba2 = demo2.fetch_dataset("nba")
        assert ds_nba == ds_nba2

    def test_setup_method(self: object) -> None:
        """Test the convenience nessiedemo.demo.setup_demo()."""
        demo = setup_demo("nessie-0.5-iceberg-0.11.yml")
        demo2 = setup_demo("nessie-0.5-iceberg-0.11.yml", "nba")
        demo3 = setup_demo("nessie-0.5-iceberg-0.11.yml", ["nba", "region-nation"])

        assert demo2 is demo
        assert demo3 is demo
