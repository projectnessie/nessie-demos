# -*- coding: utf-8 -*-
"""NessieDemo TODO docs."""
import os
import signal
import stat
import subprocess  # noqa: S404
import sys
import time
from typing import TypeVar, Any

import requests
import yaml


T = TypeVar("T", bound="NessieDemo")


class NessieDemo:
    """NessieDemo TODO docs."""

    nessie_process_wait_seconds: float = 2.0

    versions_yaml: str = None
    demos_root: str = "https://raw.githubusercontent.com/snazy/nessie-demos/master"

    nessie_api_uri: str = "http://localhost:19120/api/v1"

    native_runner_root: str = "https://github.com/projectnessie/nessie/releases/download"

    _versions_dict: dict = None

    _nessie_native_runner: os.path = None
    _nessie_process: subprocess.Popen = None

    _assets_dir: os.path = None

    _datasets: dict = dict()

    def __init__(self: T, versions_yaml: str, demos_root: str = None) -> None:
        """Nessie demo TODO docs."""
        self.versions_yaml = versions_yaml
        if demos_root:
            self.demos_root = demos_root

        versions_url = "{}/configs/{}".format(self.demos_root, self.versions_yaml)

        self._assets_dir = (
            os.path.abspath(os.environ["NESSIE_DEMO_ASSETS"])
            if "NESSIE_DEMO_ASSETS" in os.environ
            else os.path.join(os.getcwd(), "_assets")
        )

        self._versions_dict = yaml.safe_load(_Util.curl(versions_url))

    def __str__(self: T) -> str:
        """Todo the docs."""
        if self.is_nessie_running():
            run_info = "RUNNING, PID {}".format(self._pid_from_file())
        else
            run_info = "not running"
        return "Nessie-Demo: Nessie {nessie_version} ({nessie_running}), Apache Iceberg {iceberg_version}".format(
            nessie_version=self.get_nessie_version(),
            iceberg_version=self.get_iceberg_version(),
            nessie_running=run_info,
        )

    def get_nessie_version(self: T) -> str:
        """Todo the docs."""
        return self._versions_dict["versions"]["nessie"]

    def get_pynessie_version(self: T) -> str:
        """Todo the docs."""
        return self._versions_dict["versions"]["pynessie"]

    def get_iceberg_version(self: T) -> str:
        """Todo the docs."""
        return self._versions_dict["versions"]["iceberg"]

    def _prepare(self: T) -> None:
        # Install Python dependencies
        if "python_dependencies" in self._versions_dict:
            deps = self._versions_dict["python_dependencies"]
            _Util.exec_fail([sys.executable, "-m", "pip", "install"] + ["{}=={}".format(k, v) for k, v in deps.items()])

        # Download nessie native runner binary
        self._nessie_native_runner = os.path.join(
            self._assets_dir,
            "nessie-quarkus-{}-runner".format(self.get_nessie_version()),
        )

        if os.path.exists(self._nessie_native_runner) and os.stat(self._nessie_native_runner).st_mode & stat.S_IXUSR == stat.S_IXUSR:
            return

        nessie_native_runner_url = None
        try:
            nessie_native_runner_url = self._versions_dict["uris"]["nessie_native_image_binary"]
        except KeyError:
            pass
        if not nessie_native_runner_url:
            nessie_native_runner_url = "{}/nessie-{}/nessie-quarkus-{}-runner".format(
                self.native_runner_root,
                self.get_nessie_version(),
                self.get_nessie_version(),
            )

        # TODO find a way to either download binaries to the same place (don't download AND STORE the same binary again)
        # TODO find a way to remove the downloaded binaries

        _Util.wget(nessie_native_runner_url, self._nessie_native_runner, executable=True)

    def _pid_file(self: T) -> os.path:
        return os.path.join(self._assets_dir, "nessie.pid")

    def _version_file(self: T) -> os.path:
        return os.path.join(self._assets_dir, "nessie.version")

    def _pid_from_file(self: T) -> int:
        pid_file = self._pid_file();
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
        """Nessie docs TODO."""
        if self._nessie_process and not self._nessie_process.poll():
            return True
        return self._pid_from_file() > 0

    def start(self: T) -> None:
        """Nessie demo TODO docs."""
        self._prepare()

        if self.is_nessie_running():
            # Nessie process is still alive, leave it running.
            if os.path.exists(self._version_file()):
                with open(self._version_file(), "rb") as inp:
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

            self._nessie_process = subprocess.Popen(self._nessie_native_runner, stderr=std_capt, stdout=std_capt)  # noqa: S603

            with open(self._pid_file(), "wb") as out:
                out.write(str(self._nessie_process.pid).encode("utf-8"))
            with open(self._version_file(), "wb") as out:
                out.write(self.get_nessie_version().encode("utf-8"))

            try:
                std_capt.close()
                self._nessie_process.wait(self.nessie_process_wait_seconds)
                with open("nessie-runner-output.log") as log:
                    log_lines = log.readlines()
                raise Exception(
                    "Nessie process disappeared. Exit-code: {}, stdout/stderr:\n  {}".format(
                        self._nessie_process.returncode, "  ".join(log_lines)
                    )
                )
            except subprocess.TimeoutExpired:
                print("Nessie running with PID {}".format(self._nessie_process.pid))
                pass
        except Exception:
            std_capt.close()
            os.unlink("nessie-runner-output.log")
            raise

    def stop(self: T) -> None:
        """Nessie demo TODO docs."""
        if self._nessie_process:
            print("Stopping Nessie ...")
            exit_code = self._nessie_process.poll()
            if not exit_code:
                self._nessie_process.terminate()
                self._nessie_process.wait()
            print("Nessie stopped")
        pid = self._pid_from_file()
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
        os.unlink(self._pid_file())
        os.unlink(self._version_file())

    def fetch_dataset(self: T, dataset_name: str) -> dict:  # dict[str, os.path]
        """Nessie demo TODO docs."""
        if dataset_name in self._datasets:
            return self._datasets[dataset_name]

        dataset_root = "{}/datasets/{}/".format(self.demos_root, dataset_name)
        contents = _Util.curl("{}/ls.txt".format(dataset_root)).decode("utf-8").split("\n")
        dataset_dir = os.path.join(self._assets_dir, "datasets/{}".format(dataset_name))
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

        self._datasets[dataset_name] = name_to_path

        return name_to_path


class _Util:
    @staticmethod
    def exec_fail(args: list) -> None:
        print("Executing {} ...".format(" ".join(args)))
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
        if result.returncode != 0:
            raise Exception("Executable failed. args: {}, stdout={}, stderr={}".format(" ".join(result.args), result.stdout, result.stderr))

    @staticmethod
    def wget(url: str, target: os.path, executable: bool = False) -> None:
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
    global __NESSIE_DEMO__
    if __NESSIE_DEMO__:
        if __NESSIE_DEMO__.versions_yaml == versions_yaml:
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
