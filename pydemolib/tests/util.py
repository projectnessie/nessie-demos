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
"""Utilities for tests."""
import os
import sys
from signal import SIGKILL
from typing import Callable

from _pytest.fixtures import FixtureRequest
from _pytest.tmpdir import TempdirFactory
from pytest import fail


def demo_setup_fixture_for_tests(tmpdir_factory: TempdirFactory, request: FixtureRequest) -> None:
    """Sets up NESSIE_DEMO_ROOT + NESSIE_DEMO_ASSETS env vars for `NessieDemo` customization and kills a running Nessie instance."""
    d = os.path.abspath("..")
    if not os.path.exists(os.path.join(d, "configs")):
        d = os.path.abspath(os.path.join(d, ".."))
    os.environ["NESSIE_DEMO_ROOT"] = "file://{}".format(d)

    if "NESSIE_DEMO_ASSETS" not in os.environ:
        tmpdir = str(tmpdir_factory.mktemp("_assets"))
        os.environ["NESSIE_DEMO_ASSETS"] = tmpdir
    else:
        tmpdir = os.environ["NESSIE_DEMO_ASSETS"]

    print("TEST-TEARDOWN: Using {} for assets".format(tmpdir))

    def __kill_running_nessie() -> None:
        f = os.path.join(tmpdir, "nessie.pid")
        if os.path.exists(f):
            with open(f, "rb") as inp:
                pid = int(inp.read())
                print("TEST-TEARDOWN: Sending SIGKILL to PID {}".format(pid))
                try:
                    os.kill(pid, SIGKILL)
                    os.unlink(f)
                except OSError:
                    pass

    request.addfinalizer(__kill_running_nessie)


def expect_error(expected_exception_type: str, f: Callable) -> None:
    """Calls the given function `f` and asserts that the fully qualified name of the thrown exception matches `expected_exception_type`."""
    passed = False
    try:
        f()
        passed = True
    except BaseException:
        ex_type, ex_value, ex_tb = sys.exc_info()
        # We don't really have that type here (comes in transitively)
        assert "{}.{}".format(ex_type.__module__, ex_type.__name__) == expected_exception_type
    if passed:
        fail("Callable passed but should have thrown '{}'".format(expected_exception_type))
