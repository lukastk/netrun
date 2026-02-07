"""Deploy a netrun pool server to a Hetzner Cloud VM — composable API.

Each step can be run independently, making the workflow idempotent and
easy to iterate on.

Requirements:
    - ``hcloud`` CLI installed and authenticated (``hcloud context create``)
    - An SSH key registered in Hetzner Cloud (``hcloud ssh-key list``)
    - ``pyinfra`` installed (listed in this project's dependencies)

Usage::

    from deploy_to_hetzner import (
        create_hetzner_server, deploy_to_server, check_deployed,
        start_pool_server, stop_pool_server, delete_server,
    )

    # 1. Create (idempotent — returns existing IP if server exists)
    ip = create_hetzner_server(server_name="netrun-demo", ssh_key_name="my-key")

    # 2. Deploy code + deps
    deploy_to_server(host=ip, ssh_private_key_path="~/.ssh/id_ed25519",
                     local_folder="./app", net_source="net_config.toml")

    # 3. Start pool server + SSH tunnel
    handle = start_pool_server(host=ip, ssh_private_key_path="~/.ssh/id_ed25519")
    print(handle.pool_server_url)  # ws://localhost:8765

    # 4. When done
    handle.close_tunnel()
    stop_pool_server(host=ip, ssh_private_key_path="~/.ssh/id_ed25519")
    delete_server("netrun-demo")
"""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ._hetzner import create_server, delete_server, get_server_ip, wait_for_ssh
from ._infra import (
    build_serve_script,
    build_start_script,
    build_watchdog_script,
    build_watchdog_service,
    build_watchdog_timer,
    run_deployment,
)
from ._tunnel import _ssh_base, start_ssh_tunnel, wait_for_remote_port

__all__ = [
    "PoolServerHandle",
    "create_hetzner_server",
    "deploy_to_server",
    "check_deployed",
    "check_pool_server_running",
    "start_pool_server",
    "stop_pool_server",
    "delete_server",
]


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class PoolServerHandle:
    """Returned by :func:`start_pool_server` with connection and tunnel info."""

    server_ip: str
    pool_server_url: str
    already_running: bool = False
    tunnel_pid: int | None = field(default=None, repr=False)

    def close_tunnel(self) -> None:
        """Terminate the SSH tunnel process if one is active."""
        if self.tunnel_pid is not None:
            try:
                os.kill(self.tunnel_pid, signal.SIGTERM)
                print("SSH tunnel closed.")
            except ProcessLookupError:
                pass
            self.tunnel_pid = None


# ---------------------------------------------------------------------------
# 1. create_hetzner_server — idempotent VM creation
# ---------------------------------------------------------------------------

def create_hetzner_server(
    *,
    server_name: str,
    ssh_key_name: str,
    server_type: str = "cpx11",
    server_image: str = "ubuntu-24.04",
    server_location: str = "fsn1",
) -> str:
    """Create a Hetzner Cloud server and return its IPv4 address.

    Idempotent — if the server already exists, returns its IP immediately.
    """
    existing_ip = get_server_ip(server_name)
    if existing_ip:
        print(f"Server '{server_name}' already exists at {existing_ip}")
        return existing_ip

    ip = create_server(server_name, ssh_key_name, server_type, server_image, server_location)

    # Hetzner reuses IPs — remove stale known_hosts entries
    subprocess.run(["ssh-keygen", "-R", ip], capture_output=True)

    wait_for_ssh(ip)
    return ip


# ---------------------------------------------------------------------------
# 2. deploy_to_server — upload code + install deps (no start)
# ---------------------------------------------------------------------------

def deploy_to_server(
    *,
    host: str,
    ssh_private_key_path: str,
    net_source: str,
    local_folder: str | None = None,
    git_url: str | None = None,
    git_branch: str | None = None,
    user: str = "root",
    remote_dir: str = "/opt/netrun-app",
    pool_server_port: int = 8765,
    worker_name: str = "execution_manager",
    log_file: str | None = "pool_server.log",
    python_version: str | None = None,
    uv_extra_args: str = "",
    pre_commands: list[str] | None = None,
    setup_firewall: bool = False,
    ssh_tunnel: bool = True,
    netrun_install_spec: str | None = None,
    exclude: list[str] | None = None,
    exclude_dir: list[str] | None = None,
    enable_watchdog: bool = False,
    hcloud_api_token: str | None = None,
    auto_delete_idle_minutes: int = 10,
    auto_delete_start_delay_minutes: int = 15,
) -> None:
    """Deploy code and dependencies to a remote server (does not start the pool server).

    Uploads the application, installs uv + dependencies, and writes the
    start/serve scripts. Call :func:`start_pool_server` afterwards to launch
    the pool server.

    If *enable_watchdog* is ``True``, installs a systemd watchdog that
    auto-deletes the server via the Hetzner API after it has been idle
    (pool server not running) for *auto_delete_idle_minutes*.  The watchdog
    waits *auto_delete_start_delay_minutes* after deployment before it
    starts checking, to allow time for setup and first use.

    Requires *hcloud_api_token* to be set — raises ``ValueError`` if not.
    """
    if enable_watchdog and not hcloud_api_token:
        raise ValueError(
            "enable_watchdog=True requires hcloud_api_token to be set"
        )
    if (local_folder is None) == (git_url is None):
        raise ValueError("Specify exactly one of 'local_folder' or 'git_url'")

    key = str(Path(ssh_private_key_path).expanduser())
    if local_folder:
        local_folder = str(Path(local_folder).resolve())

    bind_host = "127.0.0.1" if ssh_tunnel else "0.0.0.0"
    serve_script = build_serve_script(
        net_source, bind_host, pool_server_port, worker_name, log_file,
    )
    start_script = build_start_script(remote_dir)

    # Watchdog (opt-in via enable_watchdog)
    wd_script = wd_service = wd_timer = None
    if enable_watchdog:
        wd_script = build_watchdog_script(
            remote_dir, auto_delete_idle_minutes, auto_delete_start_delay_minutes,
        )
        wd_service = build_watchdog_service(remote_dir)
        wd_timer = build_watchdog_timer()

    print("Deploying with pyinfra...")
    run_deployment(
        host=host, user=user, ssh_key=key,
        local_folder=local_folder, exclude=exclude, exclude_dir=exclude_dir,
        git_url=git_url, git_branch=git_branch,
        remote_dir=remote_dir,
        serve_script=serve_script, start_script=start_script,
        python_version=python_version, uv_extra_args=uv_extra_args,
        pre_commands=pre_commands, setup_firewall=setup_firewall,
        netrun_install_spec=netrun_install_spec,
        watchdog_script=wd_script, watchdog_service=wd_service,
        watchdog_timer=wd_timer, hcloud_api_token=hcloud_api_token,
    )
    print("Deployment complete.")


# ---------------------------------------------------------------------------
# 3. check_deployed — check if deployment exists on remote
# ---------------------------------------------------------------------------

def check_deployed(
    *,
    host: str,
    ssh_private_key_path: str,
    user: str = "root",
    remote_dir: str = "/opt/netrun-app",
) -> bool:
    """Check whether a deployment exists on the remote server.

    Returns ``True`` if both the serve script and the virtualenv exist.
    """
    key = str(Path(ssh_private_key_path).expanduser())
    _qdir = shlex.quote(remote_dir)
    cmd = _ssh_base(host, user, key) + [
        f"test -f {_qdir}/.netrun_serve_pool.py && test -d {_qdir}/.venv"
    ]
    result = subprocess.run(cmd, capture_output=True)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# 3b. check_pool_server_running — check if pool server process is alive
# ---------------------------------------------------------------------------

def check_pool_server_running(
    *,
    host: str,
    ssh_private_key_path: str,
    user: str = "root",
    remote_dir: str = "/opt/netrun-app",
) -> bool:
    """Check whether the pool server process is running on the remote.

    Returns ``True`` if the PID file exists and the process is alive.
    """
    key = str(Path(ssh_private_key_path).expanduser())
    _qdir = shlex.quote(remote_dir)
    pid_file = f"{_qdir}/.netrun_serve_pool.pid"
    cmd = _ssh_base(host, user, key) + [
        f'test -f {pid_file} && kill -0 "$(cat {pid_file})" 2>/dev/null'
    ]
    result = subprocess.run(cmd, capture_output=True)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# 4. start_pool_server — start pool server + open SSH tunnel
# ---------------------------------------------------------------------------

def start_pool_server(
    *,
    host: str,
    ssh_private_key_path: str,
    user: str = "root",
    remote_dir: str = "/opt/netrun-app",
    pool_server_port: int = 8765,
    ssh_tunnel: bool = True,
    tunnel_local_port: int | None = None,
    forward_ssh_agent: bool = False,
) -> PoolServerHandle:
    """Start the pool server on the remote and optionally open an SSH tunnel.

    The pool server is launched via the ``.netrun_start.sh`` script that
    was uploaded during :func:`deploy_to_server`.  If the server is already
    running, it is not restarted.

    If *forward_ssh_agent* is ``True`` (requires *ssh_tunnel*), the SSH
    tunnel connection carries agent forwarding so that code running on the
    remote can use the local machine's SSH keys.

    Returns a :class:`PoolServerHandle` with the connection URL, tunnel info,
    and ``already_running`` indicating whether the server was already up.
    """
    if forward_ssh_agent and not ssh_tunnel:
        raise ValueError("forward_ssh_agent=True requires ssh_tunnel=True")

    key = str(Path(ssh_private_key_path).expanduser())

    was_running = check_pool_server_running(
        host=host, ssh_private_key_path=ssh_private_key_path,
        user=user, remote_dir=remote_dir,
    )

    # Start the tunnel first so the forwarded SSH agent socket is available
    # before the pool server launches.
    _qdir = shlex.quote(remote_dir)
    tunnel_pid: int | None = None
    if ssh_tunnel:
        agent_sock = f"{remote_dir}/.ssh_agent.sock" if forward_ssh_agent else None
        local_port, tunnel_pid = start_ssh_tunnel(
            host, user, key, pool_server_port, tunnel_local_port,
            forward_agent=forward_ssh_agent, agent_sock_path=agent_sock,
        )
        url = f"ws://localhost:{local_port}"
    else:
        url = f"ws://{host}:{pool_server_port}"

    # Execute the start script on the remote (no-op if already running)
    cmd = _ssh_base(host, user, key) + [f"{_qdir}/.netrun_start.sh"]
    subprocess.run(cmd, check=True, capture_output=True, text=True)

    # Wait for the server process to start listening
    wait_for_remote_port(host, user, key, pool_server_port)

    if was_running:
        print(f"Pool server was already running: {url}")
    else:
        print(f"Pool server started: {url}")
    return PoolServerHandle(
        server_ip=host, pool_server_url=url,
        already_running=was_running, tunnel_pid=tunnel_pid,
    )


# ---------------------------------------------------------------------------
# 5. stop_pool_server — kill remote pool server process
# ---------------------------------------------------------------------------

def stop_pool_server(
    *,
    host: str,
    ssh_private_key_path: str,
    user: str = "root",
    remote_dir: str = "/opt/netrun-app",
) -> None:
    """Stop the pool server on the remote.

    Reads the PID from ``.netrun_serve_pool.pid`` and kills the process.
    No-op if no PID file exists.
    """
    key = str(Path(ssh_private_key_path).expanduser())
    _qdir = shlex.quote(remote_dir)
    pid_file = f"{_qdir}/.netrun_serve_pool.pid"
    cmd = _ssh_base(host, user, key) + [
        f"test -f {pid_file} && kill $(cat {pid_file}) && rm -f {pid_file} "
        f"&& echo 'Pool server stopped.' || echo 'No pool server running.'"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout.strip())
