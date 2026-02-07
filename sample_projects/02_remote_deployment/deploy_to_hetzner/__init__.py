"""Deploy a netrun pool server to a Hetzner Cloud VM.

Handles server provisioning, file upload, environment setup, and pool server
startup.  The pool server binds to ``127.0.0.1`` on the remote and is
accessed through an SSH tunnel so the port is never exposed publicly.
A UFW firewall is configured to allow only SSH traffic.

Requirements:
    - ``hcloud`` CLI installed and authenticated (``hcloud context create``)
    - An SSH key registered in Hetzner Cloud (``hcloud ssh-key list``)
    - ``pyinfra`` installed (listed in this project's dependencies)

Usage::

    from deploy_to_hetzner import deploy_to_hetzner, delete_server

    result = deploy_to_hetzner(
        server_name="netrun-demo",
        ssh_key_name="my-key",
        ssh_private_key_path="~/.ssh/id_ed25519",
        local_folder="./app",
        remote_dir="/opt/netrun-app",
        net_source="net_config.toml",
        pool_server_port=8765,
        python_version="3.11",
    )
    print(result.pool_server_url)  # ws://localhost:8765

    # When done:
    result.close_tunnel()
    delete_server("netrun-demo")
"""

from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ._hetzner import create_server, delete_server, wait_for_ssh
from ._infra import build_serve_script, build_start_script, run_deployment
from ._tunnel import start_ssh_tunnel, wait_for_remote_port

__all__ = [
    "DeployResult",
    "deploy_to_hetzner",
    "deploy_to_server",
    "create_server",
    "delete_server",
    "wait_for_ssh",
    "start_ssh_tunnel",
]


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class DeployResult:
    """Returned by :func:`deploy_to_hetzner` with server and tunnel info."""

    server_name: str
    server_ip: str
    pool_server_url: str
    remote_dir: str
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
# Entry points
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
    tunnel_local_port: int | None = None,
) -> tuple[str, int | None]:
    """Deploy a netrun pool server to an existing SSH-accessible server.

    When *ssh_tunnel* is ``True`` (the default), the pool server binds to
    ``127.0.0.1`` and an SSH tunnel is opened so the client can reach it
    via ``ws://localhost:<port>``.

    Returns ``(pool_server_url, tunnel_pid)``.
    """
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

    print("Deploying with pyinfra...")
    run_deployment(
        host=host, user=user, ssh_key=key,
        local_folder=local_folder, git_url=git_url, git_branch=git_branch,
        remote_dir=remote_dir,
        serve_script=serve_script, start_script=start_script,
        python_version=python_version, uv_extra_args=uv_extra_args,
        pre_commands=pre_commands, setup_firewall=setup_firewall,
    )

    # Wait for the server process to start listening (via SSH since the
    # port is bound to localhost on the remote).
    wait_for_remote_port(host, user, key, pool_server_port)

    tunnel_pid: int | None = None
    if ssh_tunnel:
        local_port, tunnel_pid = start_ssh_tunnel(
            host, user, key, pool_server_port, tunnel_local_port,
        )
        url = f"ws://localhost:{local_port}"
    else:
        url = f"ws://{host}:{pool_server_port}"

    print(f"Pool server ready: {url}")
    return url, tunnel_pid


def deploy_to_hetzner(
    *,
    server_name: str,
    ssh_key_name: str,
    ssh_private_key_path: str,
    net_source: str,
    local_folder: str | None = None,
    git_url: str | None = None,
    git_branch: str | None = None,
    remote_dir: str = "/opt/netrun-app",
    pool_server_port: int = 8765,
    worker_name: str = "execution_manager",
    log_file: str | None = "pool_server.log",
    server_type: str = "cpx11",
    server_image: str = "ubuntu-24.04",
    server_location: str = "fsn1",
    python_version: str | None = None,
    uv_extra_args: str = "",
    pre_commands: list[str] | None = None,
    ssh_user: str = "root",
    ssh_tunnel: bool = True,
    tunnel_local_port: int | None = None,
    setup_firewall: bool = True,
) -> DeployResult:
    """Create a Hetzner Cloud server and deploy a netrun pool server to it.

    By default:

    - A **UFW firewall** is configured to allow only SSH (port 22).
    - An **SSH tunnel** is opened so the pool server port is never exposed.

    Call ``result.close_tunnel()`` when done, then
    ``delete_server(server_name)`` to tear down the VM.
    """
    ip = create_server(
        server_name, ssh_key_name, server_type, server_image, server_location,
    )

    # Hetzner reuses IPs — remove stale known_hosts entries
    subprocess.run(["ssh-keygen", "-R", ip], capture_output=True)

    wait_for_ssh(ip)

    url, tunnel_pid = deploy_to_server(
        host=ip, user=ssh_user, ssh_private_key_path=ssh_private_key_path,
        local_folder=local_folder, git_url=git_url, git_branch=git_branch,
        remote_dir=remote_dir, net_source=net_source,
        pool_server_port=pool_server_port,
        worker_name=worker_name, log_file=log_file,
        python_version=python_version, uv_extra_args=uv_extra_args,
        pre_commands=pre_commands, setup_firewall=setup_firewall,
        ssh_tunnel=ssh_tunnel, tunnel_local_port=tunnel_local_port,
    )

    return DeployResult(
        server_name=server_name,
        server_ip=ip,
        pool_server_url=url,
        remote_dir=remote_dir,
        tunnel_pid=tunnel_pid,
    )
