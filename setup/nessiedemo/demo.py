# -*- coding: utf-8 -*-
"""NessieDemo handles installing and, if necessary, downloading of dependencies for Nessie Demos."""
import os
import signal
import stat
import subprocess  # noqa: S404
import sys
import time
from typing import Any, TypeVar

import requests
import yaml


T = TypeVar("T", bound="NessieDemo")


class NessieDemo:
    """NessieDemo is the main setup and dependency handler for Nessie demos."""

    __nessie_process_wait_seconds: float = 2.0

    __versions_yaml: str
    __demos_root: str = "https://raw.githubusercontent.com/snazy/nessie-demos/master"

    __nessie_api_uri: str = "http://localhost:19120/api/v1"

    __native_runner_root: str = "https://github.com/projectnessie/nessie/releases/download"

    __versions_dict: dict

    __nessie_native_runner: str
    __nessie_process: subprocess.Popen

    __assets_dir: str

    __datasets: dict = dict()

    def __init__(self: T, versions_yaml: str) -> None:
        """Takes the name of the versions-dictionary."""
        self.__versions_yaml = versions_yaml

        versions_url = "{}/configs/{}".format(self.__demos_root, self.__versions_yaml)

        self.__assets_dir = (
            os.path.abspath(os.environ["NESSIE_DEMO_ASSETS"])
            if "NESSIE_DEMO_ASSETS" in os.environ
            else os.path.join(os.getcwd(), "_assets")
        )

        self.__versions_dict = yaml.safe_load(_Util.curl(versions_url))

    def __str__(self: T) -> str:
        """String-ified representation."""
        if self.is_nessie_running():
            run_info = "RUNNING, PID {}".format(self.__pid_from_file())
        else:
            run_info = "not running"
        return "Nessie-Demo: Nessie {nessie_version} ({nessie_running}), Apache Iceberg {iceberg_version}".format(
            nessie_version=self.get_nessie_version(),
            iceberg_version=self.get_iceberg_version(),
            nessie_running=run_info,
        )

    def get_nessie_version(self: T) -> str:
        """Get the Nessie version defined in the versions-dictionary."""
        return self.__versions_dict["versions"]["nessie"]

    def get_pynessie_version(self: T) -> str:
        """Get the pynessie version defined in the versions-dictionary."""
        return self.__versions_dict["versions"]["pynessie"]

    def get_iceberg_version(self: T) -> str:
        """Get the Iceberg version defined in the versions-dictionary."""
        return self.__versions_dict["versions"]["iceberg"]

    def __prepare(self: T) -> None:
        # Install Python dependencies
        if "python_dependencies" in self.__versions_dict:
            deps = self.__versions_dict["python_dependencies"]
            _Util.exec_fail([sys.executable, "-m", "pip", "install"] + ["{}=={}".format(k, v) for k, v in deps.items()])

        # Download nessie native runner binary
        self.__nessie_native_runner = os.path.join(
            self.__assets_dir,
            "nessie-quarkus-{}-runner".format(self.get_nessie_version()),
        )

        if os.path.exists(self.__nessie_native_runner) and os.stat(self.__nessie_native_runner).st_mode & stat.S_IXUSR == stat.S_IXUSR:
            return

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

        # TODO find a way to either download binaries to the same place (don't download AND STORE the same binary again)
        # TODO find a way to remove the downloaded binaries

        _Util.wget(nessie_native_runner_url, self.__nessie_native_runner, executable=True)

    def __pid_file(self: T) -> str:
        return os.path.join(self.__assets_dir, "nessie.pid")

    def __version_file(self: T) -> str:
        return os.path.join(self.__assets_dir, "nessie.version")

    def __pid_from_file(self: T) -> int:
        pid_file = self.__pid_file()
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

    def is_nessie_running(self: T) -> bool:
        """Check whether a Nessie process is running."""
        if hasattr(self, "__nessie_process") and not self.__nessie_process.poll():
            return True
        return self.__pid_from_file() > 0

    def start(self: T) -> None:
        """Starts the Nessie process.

        A running Nessie process will only be stopped, if it is running a different Nessie version.
        Necessary Python dependencies will be installed, as defined in the versions-dictionary.
        """
        self.__prepare()

        if self.is_nessie_running():
            # Nessie process is still alive, leave it running.
            if os.path.exists(self.__version_file()):
                with open(self.__version_file(), "rb") as inp:
                    running_version = inp.read().decode("utf-8")
            else:
                running_version = "UNKNOWN_VERSION"
            if running_version != self.get_nessie_version():
                # Running Nessie version is different. Kill it.
                self.stop()
            else:
                return

        # TODO need a way to actually _prevent_ multiple Nessie server instances
        #  (in case steps are repeated, notebooks reloaded, etc)

        # TODO capture stdout+stderr using a daemon thread and pipe it to the notebook
        std_capt = open("nessie-runner-output.log", "wb")
        try:
            print("Starting Nessie...")

            self.__nessie_process = subprocess.Popen(self.__nessie_native_runner, stderr=std_capt, stdout=std_capt)  # noqa: S603

            with open(self.__pid_file(), "wb") as out:
                out.write(str(self.__nessie_process.pid).encode("utf-8"))
            with open(self.__version_file(), "wb") as out:
                out.write(self.get_nessie_version().encode("utf-8"))

            try:
                std_capt.close()
                self.__nessie_process.wait(self.__nessie_process_wait_seconds)
                with open("nessie-runner-output.log") as log:
                    log_lines = log.readlines()
                raise Exception(
                    "Nessie process disappeared. Exit-code: {}, stdout/stderr:\n  {}".format(
                        self.__nessie_process.returncode, "  ".join(log_lines)
                    )
                )
            except subprocess.TimeoutExpired:
                print("Nessie running with PID {}".format(self.__nessie_process.pid))
                pass
        except Exception:
            std_capt.close()
            os.unlink("nessie-runner-output.log")
            raise

    def stop(self: T) -> None:
        """Stops a running Nessie process. This method is a no-op, if Nessie is not running."""
        if hasattr(self, "__nessie_process"):
            print("Stopping Nessie ...")
            exit_code = self.__nessie_process.poll()
            if not exit_code:
                self.__nessie_process.terminate()
                self.__nessie_process.wait()
            print("Nessie stopped")
        pid = self.__pid_from_file()
        if pid > 0:
            timeout_at = time.time() + 10
            while True:
                try:
                    os.kill(pid, signal.SIGTERM)
                    if time.time() > timeout_at:
                        print("Running Nessie process with PID {} didn't react to SIGTERM, sending SIGKILL".format(pid))
                        os.kill(pid, signal.SIGKILL)
                    time.sleep(0.1)
                except OSError:
                    break
        os.unlink(self.__pid_file())
        os.unlink(self.__version_file())

    def fetch_dataset(self: T, dataset_name: str) -> dict:  # dict[str, os.path]
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
        dataset_dir = os.path.join(self.__assets_dir, "datasets/{}".format(dataset_name))
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

    def _get_versions_dict(self: T) -> dict:
        """Get the versions-dictionary retrieved from one of the config files in the `configs/` directory."""
        return self.__versions_dict

    def _get_assets_dir(self: T) -> str:
        """The directory used to store assets, which are the downloaded Nessie-Quarkus-Runners, Spark tarballs, datasets, etc."""
        return self.__assets_dir

    def get_nessie_api_uri(self: T) -> str:
        """Get the Nessie server's API URL."""
        return self.__nessie_api_uri

    def get_versions_yaml(self: T) -> str:
        """Get the name of the versions-dictionary, as passed to the constructor of `NessieDemo` or to `nessiedemos.demo.setupDemo()`."""
        return self.__versions_yaml


class _Util:
    @staticmethod
    def exec_fail(args: list) -> None:
        print("Executing {} ...".format(" ".join(args)))
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
        if result.returncode != 0:
            raise Exception(
                "Executable failed. args: {}, stdout={}, stderr={}".format(
                    " ".join(result.args), result.stdout.decode("utf-8"), result.stderr.decode("utf-8")
                )
            )

    @staticmethod
    def wget(url: str, target: str, executable: bool = False) -> None:
        try:
            print("Downloading {} ...".format(url))
            with requests.get(url, stream=True) as resp:
                if not resp.ok:
                    raise Exception("Could not fetch {}, HTTP/{} {}".format(url, resp.status_code, resp.reason))
                with open(target, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                if executable:
                    os.chmod(
                        target,
                        os.stat(target).st_mode | stat.S_IXUSR,
                    )
            print("Completed download of {}".format(url))
        except Exception:
            if os.path.exists(target):
                os.unlink(target)
            raise

    @staticmethod
    def curl(url: str) -> bytes:
        with requests.get(url) as resp:
            if resp.ok:
                return resp.content
            else:
                raise Exception("Could not fetch {}, HTTP/{} {}".format(url, resp.status_code, resp.reason))


__NESSIE_DEMO__ = None


def setup_demo(versions_yaml: str, datasets: Any) -> NessieDemo:  # datasets: list[str]  or  str
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
