"""Hetzner Cloud server management via the ``hcloud`` CLI."""

from __future__ import annotations

import socket
import subprocess
import time


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{r.stderr}")
    return r


def get_server_ip(server_name: str) -> str | None:
    """Return the IPv4 address of an existing server, or None."""
    result = subprocess.run(
        ["hcloud", "server", "ip", server_name],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return None


def create_server(
    name: str,
    ssh_key_name: str,
    server_type: str = "cpx11",
    image: str = "ubuntu-24.04",
    location: str = "fsn1",
) -> str:
    """Create a Hetzner Cloud server and return its IPv4 address."""
    print(f"Creating server '{name}' ({server_type}, {image}, {location})...")
    _run([
        "hcloud", "server", "create",
        "--name", name,
        "--type", server_type,
        "--image", image,
        "--location", location,
        "--ssh-key", ssh_key_name,
    ])
    ip = _run(["hcloud", "server", "ip", name]).stdout.strip()
    print(f"  Server IP: {ip}")
    return ip


def delete_server(name: str) -> None:
    """Delete a Hetzner Cloud server."""
    print(f"Deleting server '{name}'...")
    _run(["hcloud", "server", "delete", name])
    print("  Done.")


def wait_for_ssh(host: str, timeout: int = 60) -> None:
    """Block until the SSH port (22) on *host* is reachable.

    *timeout* of -1 means wait indefinitely.
    """
    print("Waiting for SSH", end="", flush=True)
    deadline = None if timeout < 0 else time.time() + timeout
    while deadline is None or time.time() < deadline:
        try:
            with socket.create_connection((host, 22), timeout=2):
                pass
            print(" ready!")
            time.sleep(3)  # give sshd a moment to fully start
            return
        except (ConnectionRefusedError, OSError, socket.timeout):
            print(".", end="", flush=True)
            time.sleep(2)
    raise TimeoutError(f"SSH not ready after {timeout}s")
