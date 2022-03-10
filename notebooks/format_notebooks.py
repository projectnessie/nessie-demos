#  Copyright (C) 2022 Dremio
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Script for formatting notebook json for readable git diffs."""

import json
from pathlib import Path
from typing import Any


def _pretty_print_json_to_file(data: Any, path: Path) -> None:
    print(f"Writing {path} ...")
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _format_notebooks() -> None:
    for path in Path(".").glob("*.ipynb"):
        with open(path) as f:
            notebook_dict = json.load(f)

        for cell_dict in notebook_dict["cells"]:
            # "source" used to be a list of lines but when dowloading a notebook from a running server
            # those now get joined together into a single string field, which we undo here to keep an orderly diff
            old_source = cell_dict.get("source")
            if isinstance(old_source, list):
                if len(old_source) != 1:
                    continue
                old_source_text = old_source[0]
            elif isinstance(old_source, str):
                if not old_source:
                    cell_dict["source"] = []
                    continue
                old_source_text = old_source
            else:
                raise Exception(f"Unhandled cell source type: {old_source}")

            new_source = [
                source_line.rstrip() + "\n"
                for source_line in old_source_text.splitlines()
            ]
            new_source[-1] = new_source[-1].rstrip("\n")
            cell_dict["source"] = new_source

        _pretty_print_json_to_file(notebook_dict, path)


if __name__ == "__main__":
    _format_notebooks()
