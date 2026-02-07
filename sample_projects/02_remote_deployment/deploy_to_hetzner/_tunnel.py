"""SSH tunnel management and remote port checking."""

from __future__ import annotations

import shlex
import subprocess
import time

_SSH_OPTS = ["-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10"]


def _ssh_base(host: str, user: str, key_path: str) -> list[str]:
    return ["ssh", "-i", key_path, *_SSH_OPTS, f"{user}@{host}"]


def wait_for_remote_port(
    host: str, user: str, key_path: str,
    port: int, timeout: int = 120,
) -> None:
    """Block until *port* is listening on the remote server (checked via SSH).

    Used when the port is bound to 127.0.0.1 and not reachable externally.
    """
    print(f"Waiting for remote port {port}", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            _ssh_base(host, user, key_path) + [
                f"bash -c 'echo > /dev/tcp/localhost/{int(port)}' 2>/dev/null"
            ],
            capture_output=True, text=True,
        )
        if r.returncode == 0:
            print(" ready!")
            return
        print(".", end="", flush=True)
        time.sleep(2)
    raise TimeoutError(f"Remote port {port} not ready after {timeout}s")


def start_ssh_tunnel(
    host: str, user: str, key_path: str,
    remote_port: int, local_port: int | None = None,
    forward_agent: bool = False,
    agent_sock_path: str | None = None,
) -> tuple[int, int]:
    """Start an SSH port-forward in the background.

    If *forward_agent* is ``True``, enables SSH agent forwarding (``-A``)
    and symlinks the forwarded agent socket to *agent_sock_path* on the
    remote so that other processes can use it.

    Returns ``(local_port, pid)`` of the tunnel process.
    """
    if local_port is None:
        local_port = remote_port
    print(f"Opening SSH tunnel (localhost:{local_port} -> {host}:{remote_port})...")

    extra_flags: list[str] = []
    if forward_agent:
        extra_flags.append("-A")

    if forward_agent and agent_sock_path:
        # Symlink the forwarded agent socket to a fixed path, then sleep
        # to keep the connection (and the agent socket) alive.
        remote_cmd = (
            f'ln -sf "$SSH_AUTH_SOCK" {shlex.quote(agent_sock_path)} && '
            f"sleep infinity"
        )
        cmd = [
            "ssh", "-i", key_path, *_SSH_OPTS, *extra_flags,
            "-L", f"{local_port}:localhost:{remote_port}",
            f"{user}@{host}",
            remote_cmd,
        ]
    else:
        cmd = [
            "ssh", "-i", key_path, *_SSH_OPTS, *extra_flags,
            "-N",  # no remote command
            "-L", f"{local_port}:localhost:{remote_port}",
            f"{user}@{host}",
        ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Give it a moment to establish
    time.sleep(2)
    if proc.poll() is not None:
        raise RuntimeError("SSH tunnel process exited immediately — check your SSH key and host")
    print(f"  Tunnel active (pid {proc.pid})")
    if forward_agent:
        print(f"  SSH agent forwarding enabled")
    return local_port, proc.pid
