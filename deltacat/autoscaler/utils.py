import subprocess
from typing import Any, Optional, List


def run_cmd(cmd: str,
            background_process: bool = False,
            stdout: Optional[Any] = subprocess.DEVNULL,
            stderr: Optional[Any] = subprocess.DEVNULL) -> None:
    cmd_list = cmd.split(" ")
    if background_process:
        subprocess.Popen(cmd_list, stdout=stdout, stderr=stderr)
        return
    else:
        exit_code = subprocess.call(cmd_list, stdout=stdout, stderr=stderr)
        assert exit_code == 0, f"`{cmd}` failed. Exit code: {exit_code}"


def ray_submit(cluster_cfg: str,
               path_to_script: str,
               script_arguments: Optional[List[str]] = None,
               background_process: bool = False,
               stdout: Optional[Any] = None,
               stderr: Optional[Any] = None) -> None:
    if script_arguments is None:
        script_arguments = []

    script_arguments_str = " ".join(script_arguments)
    cmd = f"ray submit --no-config-cache --start --stop {cluster_cfg} {path_to_script}"
    if script_arguments:
        cmd = f"{cmd} -- {script_arguments_str}"

    run_cmd(cmd, background_process=background_process, stdout=stdout, stderr=stderr)
    print(f"Submitted script on Ray cluster '{cluster_cfg}' with command '{cmd}'")


