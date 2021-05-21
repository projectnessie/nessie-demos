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


def _find_notebook(notebook_file: str) -> str:
    path_to_notebook = os.path.join("colab", notebook_file)
    if not os.path.exists(path_to_notebook):
        path_to_notebook = os.path.join("..", path_to_notebook)
    if not os.path.exists(path_to_notebook):
        raise Exception(
            f"Could not find {notebook_file} in {os.path.abspath('.')} and {os.path.abspath('..')}"
        )
    return os.path.abspath(path_to_notebook)
