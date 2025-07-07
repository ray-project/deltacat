# from deltacat.compute import index
import subprocess
import socket
import os
import time
import re

import deltacat as dc

from dataclasses import dataclass

from typing import Set, Optional, Dict, Any, Union

from ray.job_submission import JobSubmissionClient, JobStatus

from deltacat.utils.performance import timed_invocation


def _run_cmd(cmd: str) -> None:
    exit_code = int(os.system(cmd))
    assert exit_code == 0, f"`{cmd}` failed. Exit code: {exit_code}"


def _ray_up(
    cluster_cfg: str, cluster_name_override: str = None, restart_only: bool = False
) -> None:
    restart_flag = "--no-restart" if not restart_only else "--restart-only"
    cluster_name_option = (
        f"-n '{cluster_name_override}'" if cluster_name_override else ""
    )
    print(f"Starting Ray cluster from '{cluster_cfg}'")
    _run_cmd(
        f"ray up '{cluster_cfg}' -y --no-config-cache {restart_flag} {cluster_name_option} --disable-usage-stats"
    )
    print(f"Started Ray cluster from '{cluster_cfg}'")


def _is_port_in_use(port: Union[int, str]) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", int(port))) == 0


def _is_dashboard_running(port: Union[int, str]) -> bool:
    return _is_port_in_use(port)


def _ray_dashboard_up(
    cluster_cfg: str, port: Union[str, int], timeout_seconds=15
) -> None:
    print(f"Starting Ray Dashboard for Ray cluster '{cluster_cfg}'")
    _run_cmd(f"ray dashboard '{cluster_cfg}' --port {port} &")
    start = time.monotonic()
    dashboard_is_up = False
    while time.monotonic() - start <= timeout_seconds:
        if _is_dashboard_running(port):
            dashboard_is_up = True
            break
        time.sleep(0.1)
    if not dashboard_is_up:
        raise TimeoutError(
            f"Timed out after waiting {timeout_seconds} seconds for dashboard "
            f"to establish connection on port {port}."
        )
    print(f"Started Ray Dashboard for Ray cluster '{cluster_cfg}'")


def _get_head_node_ip(cluster_cfg: str) -> str:
    print(f"Getting Ray cluster head node IP for '{cluster_cfg}'")
    cmd = f"ray get-head-ip '{cluster_cfg}'"
    proc = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        check=True,
    )
    # the head node IP should be the last line printed to stdout
    # TODO(pdames): add IPv6 support
    head_node_ip = proc.stdout.splitlines()[-1]
    if not re.match(
        r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
        head_node_ip,
    ):
        print(
            f"Failed to find Ray Head Node IP Address in `{cmd}` "
            f"output: {proc.stdout}"
        )
        raise RuntimeError("No Ray Head Node IP Address Found")
    print(f"Ray cluster head node IP for '{cluster_cfg}': {head_node_ip}")
    return head_node_ip


def _ray_down_cmd(cluster_cfg: str) -> str:
    return f"ray down '{cluster_cfg}' -y"


def _ray_down(cluster_cfg: str) -> None:
    print(f"Destroying Ray cluster for '{cluster_cfg}'")
    _run_cmd(_ray_down_cmd(cluster_cfg))
    print(f"Destroyed Ray cluster for '{cluster_cfg}'")


def _ray_cluster_running(cluster_cfg: str) -> bool:
    try:
        _get_head_node_ip(cluster_cfg)
    except Exception as e:
        print(f"Get Head Node IP Failed with Exception: {e}")
        print(f"Assuming Ray Cluster is Not Running")
        return False
    return True


@dataclass(frozen=True)
class DeltaCatJobRunResult:
    job_id: str
    job_status: JobStatus
    job_logs: Any


class DeltaCatJobClient(JobSubmissionClient):
    @staticmethod
    def of(
        cluster_cfg_file_path: str = "./deltacat.yaml",
        *,
        launch_cluster: bool = True,
        start_dashboard: bool = True,
        restart_ray: bool = False,
        head_node_ip: str = None,
        dashboard_wait_time_seconds: int = 30,
        port: Union[int, str] = "8265",
        cluster_name_override: str = None,
    ):
        job_submission_client_url = None
        try:
            # launch Ray cluster if necessary
            if cluster_cfg_file_path:
                if launch_cluster:
                    if not _ray_cluster_running(cluster_cfg_file_path) or restart_ray:
                        _ray_up(cluster_cfg_file_path, cluster_name_override)
                elif restart_ray:
                    if _ray_cluster_running(cluster_cfg_file_path):
                        _ray_up(
                            cluster_cfg_file_path, restart_ray, cluster_name_override
                        )
                    else:
                        raise RuntimeError(
                            f"Cannot Restart Ray: Ray Cluster for "
                            f"`{cluster_cfg_file_path}` not found."
                        )
                dashboard_running = _is_dashboard_running(port)
                if not dashboard_running and start_dashboard:
                    _ray_dashboard_up(
                        cluster_cfg=cluster_cfg_file_path,
                        port=port,
                        timeout_seconds=dashboard_wait_time_seconds,
                    )
                    dashboard_running = True
                if not head_node_ip:
                    head_node_ip = (
                        "127.0.0.1"
                        # use dashboard port forwarding on localhost
                        if dashboard_running
                        # fetch the remote head node IP
                        else _get_head_node_ip(cluster_cfg_file_path)
                    )
            else:
                head_node_ip = "127.0.0.1"
            job_submission_client_url = f"http://{head_node_ip}:{port}"
            print(
                f"Initializing Ray Job Submission Client with URL: "
                f"{job_submission_client_url}"
            )
            client = JobSubmissionClient(f"http://{head_node_ip}:{port}")
            # the below class change is safe as long as we only add new methods
            # to the wrapped JobSubmissionClient that don't alter its internal
            # state
            client.__class__ = DeltaCatJobClient
            return client
        except Exception as e:
            print(f"Unexpected error while initializing Ray Job Client: {e}")
            if job_submission_client_url:
                print(
                    f"Please ensure that Ray was installed with a job server "
                    f'enabled via `pip install -U "ray[default]"` and '
                    f"that http://{head_node_ip}:{port} is accessible. You "
                    f"can optionally run `ray dashboard` to forward the "
                    f"remote Ray head node port to a local port (default 8265) "
                    f'then run `ray_job_client("127.0.0.1", 8265)` '
                    f"to connect via localhost."
                )
            if cluster_cfg_file_path:
                print(
                    f"If you're done submitting jobs, ensure that the remote "
                    f"Ray Cluster is shut down by running: "
                    f"{_ray_down_cmd(cluster_cfg_file_path)}"
                )
            raise e

    def run_job(
        self,
        *,
        entrypoint: str,
        runtime_env: Optional[Dict[str, Any]] = None,
        timeout_seconds: int = 600,
        **kwargs,
    ) -> DeltaCatJobRunResult:
        """
        Synchronously submit and run a Ray job. This method combines Ray job submission and monitoring by submitting
        the job to the Ray Job Server, waiting for the job to complete,
        validating the job's terminal status, retrieving and returning job run
        result information if successful.

        Args:
            entrypoint: The entry point for the job to be executed (module
                or script to run)
            runtime_env: Runtime environment configuration for the job.
                Some commonly used keys include `working_dir` (directory
                containing the job code), `pip` (list of pip packages to
                install), and `env_vars` (environment variables for the job).
            timeout_seconds: Maximum time in seconds to wait for job completion.
                Default to 600 seconds (10 minutes).
            kwargs: Additional keyword arguments to pass to the job submission.

        Returns:
            Final results from the successful job run execution.

        Raises:
            RuntimeError: If the job fails or terminates with status other
                than SUCCEEDED.
            TimeoutError: If the job doesn't complete within the specified
                timeout period

        Example:
            >>> client = job_client()
            >>> logs = client.run_job(
            ...     # Shell command to run job
            ...     entrypoint="my_script.py",
            ...     runtime_env={
            ...         # Path to the local directory containing my_script.py
            ...         "working_dir": "./",
            ...         # Pip dependencies to install
            ...         "pip": ["pandas", "numpy"],
            ...         # System environment variables to set
            ...         "env_vars": {"DATA_PATH": "/path/to/data"},
            ...     },
            ...     timeout_seconds=1200
            ... )
        """

        job_id = self.submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            **kwargs,
        )
        job_status, latency = timed_invocation(
            self.await_job,
            job_id,
            timeout_seconds=timeout_seconds,
        )
        job_logs = self.get_job_logs(job_id)
        if job_status != JobStatus.SUCCEEDED:
            print(f"Job `{job_id}` logs: ")
            print(job_logs)
            raise RuntimeError(f"Job `{job_id}` terminated with status: {job_status}")
        return DeltaCatJobRunResult(
            job_id=job_id,
            job_status=job_status,
            job_logs=job_logs,
        )

    def await_job(
        self,
        job_id: str,
        await_status: Set[JobStatus] = {
            JobStatus.SUCCEEDED,
            JobStatus.STOPPED,
            JobStatus.FAILED,
        },
        *,
        timeout_seconds: int = 600,
    ) -> JobStatus:
        """
        Polls a job's status until it matches the desired status or times out.

        This function continuously checks the status of a specified job using the
        provided client. It will keep polling until either the desired status is
        reached or the timeout period expires.

        Args:
            job_id: The unique identifier of the job to monitor.
            await_status: Set of :class:`ray.job_submission.JobStatus` to wait for.
                The function will return when the job reaches any of these states.
            timeout_seconds: Maximum time to wait in seconds.
                Defaults to 600 seconds (10 minutes).

        Returns:
            The final status of the job.

        Raises:
            TimeoutError: If the desired status is not reached within the
            specified timeout period.

        Example:
            >>>
            >>> client = job_client()
            >>> job_id = client.submit_job(
            >>>     # Shell command to run job
            >>>     entrypoint=f"python copy.py --source '{source}' --dest '{dest}'",
            >>>     # Path to the local directory containing copy.py
            >>>     runtime_env={"working_dir": "./"},
            >>> )
            >>> # wait for the job to reach a terminal state
            >>> client.await_job(job_id)
        """
        start = time.monotonic()
        terminal_status = None
        while time.monotonic() - start <= timeout_seconds:
            status = self.get_job_status(job_id)
            if status in await_status:
                terminal_status = status
                break
            time.sleep(0.1)
        if not terminal_status:
            self.stop_job(job_id)
            raise TimeoutError(
                f"Timed out after waiting {timeout_seconds} seconds for job "
                f"`{job_id}` status: {status}"
            )
        return terminal_status


def local_job_client(*args, **kwargs) -> DeltaCatJobClient:
    """
    Create a DeltaCAT Job Client that can be used to submit jobs to a local Ray
    cluster. Initializes Ray if it's not already running.

    Args:
        *args: Positional arguments to pass to `deltacat.init()`.
        **kwargs: Keyword arguments to pass to `deltacat.init()`.
    Returns:
        DeltaCatJobClient: A client instance that can be used to submit and
            manage local Ray jobs.

    Raises:
        RuntimeError: If a local Ray Job Server cannot be found.
    """
    # force reinitialization to ensure that we can get the Ray context
    kwargs["force"] = True
    context = dc.init(*args, **kwargs)
    if context is None:
        raise RuntimeError("Failed to retrieve Ray context.")
    if context.dashboard_url:
        head_node_ip, port = context.dashboard_url.split(":")
    else:
        # the Ray Dashboard URL is also the Ray Job Server URL
        raise RuntimeError(
            "Ray Job Server not found! Please reinstall Ray using "
            "`pip install -U `ray[default]`"
        )
    return DeltaCatJobClient.of(
        None,
        launch_cluster=False,
        start_dashboard=False,
        head_node_ip=head_node_ip,
        port=port,
    )


def job_client(
    cluster_cfg_file_path: str = "./deltacat.yaml",
    *,
    launch_cluster: bool = True,
    start_dashboard: bool = True,
    restart_ray: bool = False,
    head_node_ip: str = None,
    dashboard_wait_time_seconds: int = 15,
    port: Union[str, int] = "8265",
    cluster_name_override: str = None,
) -> DeltaCatJobClient:
    """
    Create a DeltaCAT Job Client that can be used to submit jobs to a remote
    Ray cluster.

    Args:
        cluster_cfg_file_path: Path to the Ray Cluster Launcher
            Config file. Defaults to "./deltacat.yaml".
        launch_cluster : Whether to launch a new Ray cluster.
            Defaults to True.
        start_dashboard: Whether to start the Ray dashboard.
            Defaults to True.
        restart_ray: Whether to restart Ray if it's already
            running. Defaults to False.
        head_node_ip: IP address of the Ray cluster head node.
            If None, will use the configuration from the cluster config file.
            Defaults to None.
        dashboard_wait_time_seconds: Time in seconds to wait for the Ray
            dashboard to start if `start_dashboard` is True.
        port: Port number for the Ray
            dashboard/job server. Defaults to "8265".

    Returns:
        DeltaCatJobClient: A client instance that can be used to submit and
            manage jobs on the Ray cluster.

    Raises:
        RuntimeError: If the Ray Job Server is not found.
    """
    return DeltaCatJobClient.of(
        cluster_cfg_file_path,
        launch_cluster=launch_cluster,
        start_dashboard=start_dashboard,
        restart_ray=restart_ray,
        head_node_ip=head_node_ip,
        dashboard_wait_time_seconds=dashboard_wait_time_seconds,
        port=port,
        cluster_name_override=cluster_name_override,
    )
