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
"""NessieDemo handles installing and, if necessary, downloading of dependencies for Nessie Demos."""
import os
import re
import shutil
import site
import stat
import sys
import sysconfig
from pathlib import Path
from select import poll, POLLIN
from signal import SIGKILL, SIGTERM
from subprocess import DEVNULL, PIPE, Popen, STDOUT, TimeoutExpired  # noqa: S404
from time import sleep, time
from types import TracebackType
from typing import Any, Union

import requests
import yaml


class NessieDemo:
    """NessieDemo is the main setup and dependency handler for Nessie demos."""

    __nessie_start_wait_seconds: float = 2.0
    __nessie_terminate_wait_seconds: float = 10.0

    __versions_yaml: str
    __demos_root: str

    __nessie_api_uri: str = "http://localhost:19120/api/v1"

    __native_runner_root: str = "https://github.com/projectnessie/nessie/releases/download"

    __versions_dict: dict

    __nessie_native_runner: list  # list[str]
    __nessie_process: Popen

    __assets_dir: str

    __datasets: dict

    __nessie_version: str
    __iceberg_version: str

    pip_install_verbose: bool = False

    def __init__(self: "NessieDemo", versions_yaml: str) -> None:
        """Takes the name of the versions-dictionary."""
        if "NESSIE_DEMO_ROOT" in os.environ and len(os.environ["NESSIE_DEMO_ROOT"]) > 0:
            self.__demos_root = os.environ["NESSIE_DEMO_ROOT"]
        else:
            self.__demos_root = "https://raw.githubusercontent.com/projectnessie/nessie-demos/main"

        self.__load_versions_yaml(versions_yaml)

        self.__assets_dir: str = (
            os.path.abspath(os.environ["NESSIE_DEMO_ASSETS"])
            if "NESSIE_DEMO_ASSETS" in os.environ and len(os.environ["NESSIE_DEMO_ASSETS"]) > 0
            else os.path.join(os.getcwd(), "_assets")
        )
        if not os.path.isdir(self.__assets_dir):
            os.makedirs(self.__assets_dir)

        self.__datasets = dict()

        self.__nessie_version = self.__versions_dict["versions"]["nessie"]
        self.__iceberg_version = self.__versions_dict["versions"]["iceberg"] if "iceberg" in self.__versions_dict["versions"] else ""

    def __enter__(self: "NessieDemo") -> "NessieDemo":
        """Starts Nessie."""
        self.start()
        return self

    def __exit__(self: "NessieDemo", exc_type: type, exc_val: BaseException, exc_tb: TracebackType) -> None:
        """Stops Nessie."""
        self.stop()

    def __load_versions_yaml(self: "NessieDemo", versions_yaml: str) -> None:
        self.__versions_yaml = versions_yaml
        if "://" in versions_yaml:
            versions_url = versions_yaml
        else:
            versions_url = "{}/configs/{}".format(self.__demos_root, versions_yaml)

        def __env_constructor(loader: yaml.Loader, node: yaml.Node) -> str:
            env_var = node.value
            if node.value not in os.environ:
                raise Exception("Unknown environment variable '{}' referenced in {}".format(env_var, versions_url))
            return os.environ[env_var]

        yaml_loader = yaml.SafeLoader(_Util.curl(versions_url))
        try:
            yaml_loader.add_constructor("!env", __env_constructor)
            self.__versions_dict = yaml_loader.get_single_data()
        finally:
            yaml_loader.dispose()

    def __str__(self: "NessieDemo") -> str:
        """String-ified representation."""
        if self.is_nessie_running():
            run_info = "RUNNING, PID {}".format(self.__pid_from_file())
        else:
            run_info = "not running"
        return "Nessie-Demo: Nessie {nessie_version} ({nessie_running}),{iceberg_version}".format(
            nessie_version=self.get_nessie_version(),
            iceberg_version=" Apache Iceberg {}".format(self.get_iceberg_version()) if len(self.get_iceberg_version()) > 0 else "",
            nessie_running=run_info,
        )

    def _asset_dir(self: "NessieDemo", name: str) -> str:
        return os.path.join(self.__assets_dir, name)

    def get_nessie_version(self: "NessieDemo") -> str:
        """Get the Nessie version defined in the versions-dictionary."""
        return self.__nessie_version

    def get_iceberg_version(self: "NessieDemo") -> str:
        """Get the Iceberg version defined in the versions-dictionary."""
        return self.__iceberg_version

    def _get_iceberg_download_url_for_jar(self: "NessieDemo", iceberg_component: str) -> str:
        """Get the Iceberg download URL for a jar file of the given iceberg component."""
        version = self.get_iceberg_version()

        home = str(Path.home())
        local_url = f"{home}/.m2/repository/org/apache/iceberg/{iceberg_component}/{version}/{iceberg_component}-{version}.jar"
        if os.path.exists(local_url):
            return "file://" + local_url
        return f"https://repo.maven.apache.org/maven2/org/apache/iceberg/{iceberg_component}/{version}/{iceberg_component}-{version}.jar"

    def __nessie_native_runner_url(self: "NessieDemo") -> str:
        nessie_native_runner_url = None
        try:
            nessie_native_runner_url = self.__versions_dict["uris"]["nessie_native_image_binary"]
        except KeyError:
            pass
        if not nessie_native_runner_url:
            nessie_native_runner_url = "{}/nessie-{}/nessie-quarkus-{}-runner".format(
                self.__native_runner_root,
                self.get_nessie_version(),
                self.get_nessie_version(),
            )
        return nessie_native_runner_url

    def __prepare_nessie_runner(self: "NessieDemo") -> None:
        if "nessie_runner" in self.__versions_dict:
            runner = self.__versions_dict["nessie_runner"]
            if isinstance(runner, str):
                runner = [runner]
            elif not isinstance(runner, list):
                raise Exception("Optional YAML property 'nessie_runner' must be either a 'str' or a 'list[str]'")
            if not os.path.exists(runner[0]) or os.stat(runner[0]).st_mode & stat.S_IXUSR != stat.S_IXUSR:
                raise Exception("Optional YAML property 'nessie_runner' '{}', which is not an executable".format(" ".join(runner)))
        else:
            # Download nessie native runner binary
            runner = [self._asset_dir("nessie-quarkus-{}-runner".format(self.get_nessie_version()))]

            if not os.path.exists(runner[0]) or not os.stat(runner[0]).st_mode & stat.S_IXUSR == stat.S_IXUSR:
                _Util.wget(self.__nessie_native_runner_url(), runner[0], executable=True)

        self.__nessie_native_runner = runner

    def __prepare(self: "NessieDemo") -> None:
        # Install Python dependencies
        if "python_dependencies" in self.__versions_dict:
            deps_list = self.__versions_dict["python_dependencies"]
            _Util.exec_fail([sys.executable, "-m", "pip", "install"] + deps_list, pipe_output=self.pip_install_verbose)
        if "python_dependencies_reinstall" in self.__versions_dict:
            deps_list = self.__versions_dict["python_dependencies_reinstall"]
            _Util.exec_fail([sys.executable, "-m", "pip", "install", "--force-reinstall"] + deps_list, pipe_output=self.pip_install_verbose)

        self.__prepare_nessie_runner()

    def _get_pid_file(self: "NessieDemo") -> str:
        return self._asset_dir("nessie.pid")

    def _get_version_file(self: "NessieDemo") -> str:
        return self._asset_dir("nessie.version")

    def __pid_from_file(self: "NessieDemo") -> int:
        pid_file = self._get_pid_file()
        if not os.path.exists(pid_file):
            return -1
        with open(pid_file, "rb") as inp:
            pid = int(inp.read())
            try:
                os.kill(pid, 0)
            except OSError:
                os.unlink(pid_file)
                return -1
            else:
                return pid

    def _get_pid(self: "NessieDemo") -> int:
        if hasattr(self, "__nessie_process") and not self.__nessie_process.poll():
            return self.__nessie_process.pid
        return self.__pid_from_file()

    def is_nessie_running(self: "NessieDemo") -> bool:
        """Check whether a Nessie process is running."""
        if hasattr(self, "__nessie_process") and not self.__nessie_process.poll():
            return True
        return self.__pid_from_file() > 0

    def start(self: "NessieDemo") -> None:
        """Starts the Nessie process.

        A running Nessie process will only be stopped, if it is running a different Nessie version.
        Necessary Python dependencies will be installed, as defined in the versions-dictionary.
        """
        if self.is_nessie_running():
            # Nessie process is still alive, leave it running.
            if os.path.exists(self._get_version_file()):
                with open(self._get_version_file(), "rb") as inp:
                    running_version = inp.read().decode("utf-8")
            else:
                running_version = "UNKNOWN_VERSION"
            if running_version != self.get_nessie_version():
                # Running Nessie version is different. Kill it.
                self.stop()
            else:
                return

        self.__prepare()

        log_file = self._asset_dir("nessie-runner-output.log")
        std_capt = open(log_file, "wb")
        try:
            print("Starting Nessie {} ...".format(" ".join(self.__nessie_native_runner)))

            self.__nessie_process = Popen(self.__nessie_native_runner, stdin=DEVNULL, stdout=std_capt, stderr=std_capt)  # noqa: S603

            with open(self._get_pid_file(), "wb") as out:
                out.write(str(self.__nessie_process.pid).encode("utf-8"))
            with open(self._get_version_file(), "wb") as out:
                out.write(self.get_nessie_version().encode("utf-8"))

            try:
                self.__nessie_process.wait(self.__nessie_start_wait_seconds)
                with open(log_file) as log:
                    log_lines = log.readlines()
                raise Exception(
                    "Nessie process disappeared. Exit-code: {}, stdout/stderr:\n  {}".format(
                        self.__nessie_process.returncode, "  ".join(log_lines)
                    )
                )
            except TimeoutExpired:
                print("Nessie running with PID {}".format(self.__nessie_process.pid))
                pass
        except BaseException as e:
            os.unlink(log_file)
            raise e
        finally:
            std_capt.close()

    def stop(self: "NessieDemo") -> None:
        """Stops a running Nessie process. This method is a no-op, if Nessie is not running."""
        pid = self._get_pid()
        if pid > 0:
            print("Stopping Nessie ...")
            timeout_at = time() + self.__nessie_terminate_wait_seconds
            while True:
                try:
                    os.kill(pid, SIGTERM)
                    if time() > timeout_at:
                        print("Running Nessie process with PID {} didn't react to SIGTERM, sending SIGKILL".format(pid))
                        os.kill(pid, SIGKILL)
                        break
                    sleep(0.1)
                except OSError:
                    print("Nessie stopped")
                    break
        for f in [self._get_pid_file(), self._get_version_file()]:
            if os.path.exists(f):
                os.unlink(f)

    def fetch_dataset(self: "NessieDemo", dataset_name: str) -> dict:  # dict[str, os.path]
        """Fetches a data set, a collection of files, for a demo.

        Data sets are identified by a name, which corresponds to a subdirectory underneath the `datasets/`
        directory in the source tree. If a data set already exists on the executing machine, the data set will
        not be downloaded again. In other words: it is safe to call this function multiple times for the same data
        set and it will only be downloaded once.
        :param dataset_name: the name of the data set to fetch.
        :return: a dictionary of file-name to path on the running machine.
        """
        if dataset_name in self.__datasets:
            return self.__datasets[dataset_name]

        dataset_root = "{}/datasets/{}".format(self.__demos_root, dataset_name)
        contents = _Util.curl("{}/ls.txt".format(dataset_root)).decode("utf-8").split("\n")
        dataset_dir = self._asset_dir("datasets/{}".format(dataset_name))
        if not os.path.isdir(dataset_dir):
            os.makedirs(dataset_dir)
        name_to_path = dict()
        for file_name in contents:
            file_name = file_name.strip()
            if len(file_name) > 0 and not file_name.startswith("#"):
                url = "{}/{}".format(dataset_root, file_name)
                f = os.path.join(dataset_dir, file_name)
                if not os.path.exists(f):
                    _Util.wget(url, f)
                name_to_path[file_name] = f

        print("Dataset {} with files {}".format(dataset_name, ", ".join(name_to_path.keys())))

        self.__datasets[dataset_name] = name_to_path

        return name_to_path

    def _get_versions_dict(self: "NessieDemo") -> dict:
        """Get the versions-dictionary retrieved from one of the config files in the `configs/` directory."""
        return self.__versions_dict

    def get_nessie_api_uri(self: "NessieDemo") -> str:
        """Get the Nessie server's API URL."""
        return self.__nessie_api_uri

    def get_versions_yaml(self: "NessieDemo") -> str:
        """Get the name of the versions-dictionary, as passed to the constructor of `NessieDemo` or to `nessiedemos.demo.setupDemo()`."""
        return self.__versions_yaml

    def _pull_product_distribution(self: "NessieDemo", product_id: str, product_name: str) -> Union[None, str]:
        if product_id not in self._get_versions_dict() or "tarball" not in self._get_versions_dict()[product_id]:
            return None

        download_url = self._get_versions_dict()[product_id]["tarball"]
        # derive directory name inside the tarball from the URL
        m = re.match(".*[/]([a-zA-Z0-9-.]+)[.](tgz|tar[.]gz)", download_url)
        if not m:
            raise Exception(f"Invalid {product_name} download URL {download_url}")
        dir_name = m.group(1)
        target_dir = self._asset_dir(dir_name)
        if not os.path.exists(target_dir):
            tgz = self._asset_dir("{}.tgz".format(dir_name))
            if not os.path.exists(tgz):
                _Util.wget(download_url, tgz)
            try:
                os.makedirs(target_dir)
                _Util.exec_fail(["tar", "-x", "-C", target_dir, "--strip-components=1", "-f", tgz])
            except BaseException as e:
                shutil.rmtree(target_dir, True)
                raise e
        return target_dir


class _Util:
    @staticmethod
    def exec_fail(args: list, cwd: str = None, pipe_output: bool = True) -> None:  # noqa: C901
        print("Executing {}{} ...".format(" ".join(args), "" if pipe_output else ", process output hidden"))
        proc = Popen(args, stdin=DEVNULL, stdout=PIPE, stderr=STDOUT, text=True, cwd=cwd)  # noqa: S603
        stdout: Any = proc.stdout
        poll_obj = poll()
        poll_obj.register(stdout, POLLIN)
        exit_code = None
        captured_output = []
        while True:
            poll_result = poll_obj.poll(0.1)
            if poll_result:
                line = stdout.readline()
                if pipe_output:
                    sys.stdout.write(line)
                else:
                    captured_output.append(line)
                if exit_code and len(line) == 0:
                    break
            if not exit_code:
                try:
                    exit_code = proc.poll()
                    if exit_code is not None:
                        if exit_code != 0:
                            if not pipe_output:
                                sys.stdout.write("".join(captured_output))
                            raise Exception("Executable failed, exit-code={}. args: {}".format(exit_code, args))
                        break
                except OSError:
                    pass

    @staticmethod
    def wget(url: str, target: str, executable: bool = False) -> None:
        try:
            print("Downloading {} ...".format(url))
            with open(target, "wb") as f:
                if url.startswith("file://"):
                    with open(url[7:], "rb") as inp:
                        while True:
                            chunk = inp.read(65536)
                            if len(chunk) == 0:
                                break
                            f.write(chunk)
                else:
                    with requests.get(url, stream=True) as resp:
                        if not resp.ok:
                            raise Exception("Could not fetch {}, HTTP/{} {}".format(url, resp.status_code, resp.reason))
                        for chunk in resp.iter_content(chunk_size=65536):
                            f.write(chunk)
            if executable:
                os.chmod(
                    target,
                    os.stat(target).st_mode | stat.S_IXUSR,
                )
            print("Completed download of {}".format(url))
        except BaseException as e:
            if os.path.exists(target):
                os.unlink(target)
            raise e

    @staticmethod
    def curl(url: str) -> bytes:
        if url.startswith("file://"):
            with open(url[7:], "rb") as inp:
                return inp.read()

        with requests.get(url) as resp:
            if resp.ok:
                return resp.content
            else:
                raise Exception("Could not fetch {}, HTTP/{} {}".format(url, resp.status_code, resp.reason))

    @staticmethod
    def get_python_package_directory(python_package: str, *sub_directories: str) -> Union[None, str]:
        for dir in [sysconfig.get_paths()["purelib"]] + site.getsitepackages():
            package_dir = os.path.join(dir, python_package, *sub_directories)
            if os.path.exists(package_dir):
                return package_dir
        return None


__NESSIE_DEMO__ = None


def setup_demo(versions_yaml: str, datasets: Any = None) -> NessieDemo:  # datasets: list[str]  or  str
    """Sets up and starts a `NessieDemo` instance.

    It uses the versions-dictionary identified by `versions_yaml`, fetches the datasets mentioned in the
    `datasets` parameter and starts a Nessie process.
    :param versions_yaml: the name of the versions-dictionary, as in the `configs/` directory in the source tree.
    :param datasets: either a single string or a list of strings with the names of the datasets to fetch.
    Dictionaries containing the filename-to-path mapping of a dataset can be retrieved by calling `NessieDemo.fetch_dataset`.
    :return: the constructed `NessieDemo` instance.
    """
    global __NESSIE_DEMO__
    if __NESSIE_DEMO__:
        if __NESSIE_DEMO__.get_versions_yaml() == versions_yaml:
            # reuse the existing NessieDemo instance, if possible
            print("Reusing existing NessieDemo instance: {}".format(__NESSIE_DEMO__))
            __NESSIE_DEMO__.start()
            return __NESSIE_DEMO__
        # stop the Nessie server (it's using a different Nessie version)
        print("Discarding existing NessieDemo instance: {}".format(__NESSIE_DEMO__))
        __NESSIE_DEMO__.stop()
    demo = NessieDemo(versions_yaml)
    if datasets:
        if isinstance(datasets, str):
            datasets = [datasets]
        for dataset_name in datasets:
            demo.fetch_dataset(dataset_name)
    demo.start()
    __NESSIE_DEMO__ = demo
    return demo
