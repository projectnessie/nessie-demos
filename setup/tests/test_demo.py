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
    if "NESSIE_DEMO_ROOT" not in os.environ:
        d = os.path.abspath("..")
        if not os.path.exists(os.path.join(d, "configs")):
            d = os.path.abspath(os.path.join(d, ".."))
        os.environ["NESSIE_DEMO_ROOT"] = "file://{}".format(d)

    if "NESSIE_DEMO_ASSETS" not in os.environ:
        tmpdir = str(tmpdir_factory.mktemp("_assets"))
        os.environ["NESSIE_DEMO_ASSETS"] = tmpdir

        def cleanup():
            f = os.path.join(tmpdir, "nessie.pid")
            if os.path.exists(f):
                with open(f, "rb") as inp:
                    pid = int(inp.read())
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
            shutil.rmtree(tmpdir, ignore_errors=True)

        request.addfinalizer(cleanup)


def test_new_instance_can_kill_nessie() -> None:
    """Ensure config cli option is consistent."""
    demo = NessieDemo("nessie-0.5-iceberg-0.11.yml")

    assert demo.get_nessie_version() == "0.5.1"
    assert demo.get_iceberg_version() == "0.11.1"
    assert demo._get_versions_dict()["python_dependencies"]["pyspark"] == "3.0.2"

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
