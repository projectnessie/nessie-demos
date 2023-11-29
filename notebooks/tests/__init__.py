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
"""Unit tests for demo notebooks."""
import os
import shutil
import subprocess  # noqa: S404
from contextlib import contextmanager
from typing import Iterator
from typing import List

from utils import fetch_nessie_jar


def _find_notebook(notebook_file: str) -> str:
    path_to_notebook = os.path.join("notebooks", notebook_file)
    if not os.path.exists(path_to_notebook):
        path_to_notebook = os.path.join("..", path_to_notebook)
    if not os.path.exists(path_to_notebook):
        path_to_notebook = os.path.join("..", path_to_notebook)
    if not os.path.exists(path_to_notebook):
        raise Exception(
            f"Could not find {notebook_file} in {os.path.abspath('.')} and {os.path.abspath('..')}"
        )
    return os.path.abspath(path_to_notebook)


def _remove_folders(input_folders: List[str]) -> None:
    for folder in input_folders:
        path_to_folder = os.path.join(os.path.abspath("."), folder)
        if os.path.exists(path_to_folder):
            shutil.rmtree(path_to_folder)
        else:
            # We ignore the error if a folder doesn't exist in order not to break any tests
            print(f"Could not find {path_to_folder} in {os.path.abspath('.')}")


@contextmanager
def start_nessie() -> Iterator[subprocess.Popen]:
    """Context for starting and stopping a nessie binary."""
    start_command = _fetch_and_get_nessie_start_command()
    p = None
    try:
        p = subprocess.Popen(  # noqa: S603
            start_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        yield p
    finally:
        if p:
            p.kill()


def _fetch_and_get_nessie_start_command() -> List[str]:
    runner = fetch_nessie_jar()
    return ["java", "-jar", runner]
