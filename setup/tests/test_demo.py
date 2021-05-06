#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `nessiedemo` package."""
import os
import shutil
import signal

from nessiedemo.demo import NessieDemo
import pytest


@pytest.fixture(scope="session", autouse=True)
def before_all(tmpdir_factory, request):
    os.environ["NESSIE_DEMO_ROOT"] = "file://{}".format(os.path.abspath(".."))
    dir = str(tmpdir_factory.mktemp("_assets"))
    os.environ["NESSIE_DEMO_ASSETS"] = dir

    def cleanup():
        f = os.path.join(dir, "nessie.pid")
        if os.path.exists(f):
            with open(f, "rb") as inp:
                pid = int(inp.read())
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
        shutil.rmtree(dir, ignore_errors=True)

    request.addfinalizer(cleanup)


def test_config_options() -> None:
    """Ensure config cli option is consistent."""
    demo = NessieDemo("nessie-0.5-iceberg-0.11.yml")
    demo.start()
