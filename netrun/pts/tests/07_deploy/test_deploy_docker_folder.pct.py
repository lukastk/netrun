# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Integration Test: Deploy Local Folder to Docker Container via SSH
#
# Same as test_deploy_docker but uses `local_folder_path` (folder upload)
# instead of git clone.

# %%
#|default_exp deploy.test_deploy_docker_folder
#|export_as_func true

# %%
#|top_export
import subprocess
import tempfile
import shutil
import time
import os
import sys
import asyncio
import socket
from pathlib import Path

import pytest

# %%
#|top_export
DOCKERFILE = """\
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server rsync curl && \
    mkdir -p /var/run/sshd && \
    ssh-keygen -A && \
    mkdir -p /root/.ssh && \
    chmod 700 /root/.ssh && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
"""

IMAGE_NAME = "netrun-test-sshd-folder"
CONTAINER_NAME = "netrun-test-deploy-folder"

# %%
#|top_export
# Path to the netrun project root (contains pyproject.toml, src/, etc.)
NETRUN_PROJECT_DIR = str(Path(__file__).resolve().parent.parent.parent.parent)

# %%
#|top_export
def _docker_available() -> bool:
    """Check if Docker CLI exists and daemon is reachable."""
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=10)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False

def _pyinfra_available() -> bool:
    """Check if pyinfra is importable."""
    try:
        import pyinfra  # noqa: F401
        return True
    except ImportError:
        return False

def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a subprocess command, raising on failure with stderr in the message."""
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd,
            output=result.stdout, stderr=result.stderr,
        )
    return result

def _ssh_command(port: int, key_path: Path, cmd: str) -> subprocess.CompletedProcess:
    """Run a command on the Docker container via SSH."""
    return _run([
        "ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR", "-o", "IdentitiesOnly=yes",
        "-i", str(key_path), "-p", str(port), "root@localhost", cmd,
    ])

def _get_free_port() -> int:
    """Get a free TCP port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

# %%
#|set_func_signature
def test_deploy_docker_folder_mode():
    """Integration test: deploy a local folder (no git) to a Docker container."""
    ...

# %%
#|export
if not _docker_available():
    pytest.skip("Docker not available", allow_module_level=True)
if not _pyinfra_available():
    pytest.skip("pyinfra not installed", allow_module_level=True)

# %%
#|export
from netrun.deploy._config import (
    DeployConfig,
    SSHConfig,
    RepoConfig,
    InlineScriptEnvSetup,
    ConfigFileNetSource,
    PoolServerConfig,
)
from netrun.deploy._deploy import deploy
from netrun.net import Net
from netrun.net.config._net_config import NetConfig, PoolConfig, RemotePoolConfig
from netrun.net.config._graph import GraphConfig
from netrun.net.config._nodes import NodeConfig, NodeExecutionConfig

# %%
#|export
# Clean up any stale container from a previous interrupted run
subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)

tmpdir = tempfile.mkdtemp(prefix="netrun_docker_folder_test_")
tmpdir_path = Path(tmpdir)
original_sys_path = sys.path.copy()

try:
    # --- Generate SSH key pair ---
    key_path = tmpdir_path / "test_key"
    _run(["ssh-keygen", "-t", "ed25519", "-f", str(key_path), "-N", "", "-q"])

    pub_key = (key_path.with_suffix(".pub")).read_text().strip()

    # --- Allocate ports ---
    ssh_port = _get_free_port()
    pool_port = _get_free_port()

    # --- Create a local folder (NOT a git repo) with a factory node ---
    repo_dir = tmpdir_path / "repo"
    repo_dir.mkdir()

    (repo_dir / "pyproject.toml").write_text(
        '[project]\nname = "test-app"\nversion = "0.1.0"\n'
        'requires-python = ">=3.11"\n'
    )

    # nodes.py — a simple doubler function used by the factory
    (repo_dir / "nodes.py").write_text(
        "def doubler(x: int) -> int:\n"
        "    return x * 2\n"
    )

    # net_config.toml — server-side config with factory node
    (repo_dir / "net_config.toml").write_text(
        "[pools.main]\n"
        "\n"
        "[pools.main.spec]\n"
        'type = "main"\n'
        "\n"
        "[[graph.nodes]]\n"
        'factory = "netrun.node_factories.from_function"\n'
        "\n"
        "[graph.nodes.factory_args]\n"
        'func = "nodes.doubler"\n'
        "\n"
        "[graph.nodes.execution_config]\n"
        'pools = ["main"]\n'
    )

    # NOTE: No git init — this is a plain folder, not a git repo

    # --- Build Docker image (with rsync for files.sync) ---
    dockerfile_path = tmpdir_path / "Dockerfile"
    dockerfile_path.write_text(DOCKERFILE)
    _run(["docker", "build", "-t", IMAGE_NAME, str(tmpdir_path)])

    # --- Start container ---
    _run([
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", f"{ssh_port}:22",
        "-p", f"{pool_port}:8080",
        "-v", f"{NETRUN_PROJECT_DIR}:/mnt/netrun:ro",
        IMAGE_NAME,
    ])

    # Inject authorized_keys into the running container
    _run(["docker", "exec", CONTAINER_NAME, "bash", "-c",
          f"echo '{pub_key}' > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"])

    # --- Wait for SSH readiness ---
    for i in range(30):
        try:
            _ssh_command(ssh_port, key_path, "echo ready")
            break
        except subprocess.CalledProcessError:
            time.sleep(1)
    else:
        raise TimeoutError("SSH not ready after 30 seconds")

    # Add container host key to known_hosts so pyinfra can connect without prompting
    known_hosts = Path.home() / ".ssh" / "known_hosts"
    keyscan_result = _run(["ssh-keyscan", "-p", str(ssh_port), "localhost"])
    with open(known_hosts, "a") as f:
        f.write(keyscan_result.stdout)

    # --- Call deploy() with local_folder_path ---
    config = DeployConfig(
        ssh=SSHConfig(host="localhost", port=ssh_port, user="root", ssh_key=str(key_path)),
        repo=RepoConfig(local_folder_path=str(repo_dir), remote_dir="/opt/test-app"),
        env_setup=InlineScriptEnvSetup(script="pip install /mnt/netrun"),
        net_source=ConfigFileNetSource(path="net_config.toml"),
        pool_server=PoolServerConfig(port=8080, log_file=None),
    )

    result = deploy(config)

    # --- Verify deploy results ---
    assert result.success, f"Deploy failed with errors: {result.errors}"
    assert result.host == "localhost"
    assert result.port == 8080

    # Verify folder was uploaded (no .git directory should exist)
    _ssh_command(ssh_port, key_path, "test -d /opt/test-app")
    _ssh_command(ssh_port, key_path, "test -f /opt/test-app/nodes.py")
    _ssh_command(ssh_port, key_path, "test -f /opt/test-app/net_config.toml")

    # Verify NO .git directory (folder mode, not git clone)
    check_no_git = subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
         "-o", "LogLevel=ERROR", "-o", "IdentitiesOnly=yes",
         "-i", str(key_path), "-p", str(ssh_port), "root@localhost",
         "test -d /opt/test-app/.git"],
        capture_output=True, text=True,
    )
    assert check_no_git.returncode != 0, "Expected no .git directory in folder mode"

    # Verify serve script was uploaded
    _ssh_command(ssh_port, key_path, "test -f /opt/test-app/.netrun_serve_pool.py")

    # Verify the nohup command ran (stdout log and PID file created)
    _ssh_command(ssh_port, key_path, "test -f /opt/test-app/pool_server_stdout.log")
    _ssh_command(ssh_port, key_path, "test -f /opt/test-app/.netrun_serve_pool.pid")

    # --- Wait for pool server readiness ---
    for i in range(120):
        try:
            s = socket.create_connection(("localhost", pool_port), timeout=1)
            s.close()
            break
        except (ConnectionRefusedError, OSError):
            time.sleep(1)
    else:
        # Dump server log for debugging
        try:
            log = _ssh_command(ssh_port, key_path, "cat /opt/test-app/pool_server_stdout.log")
            print(f"Pool server log:\n{log.stdout}")
        except Exception:
            pass
        raise TimeoutError("Pool server not ready after 120 seconds")

    # --- Verify end-to-end execution ---
    # Add repo_dir to sys.path so the factory can resolve nodes.doubler on the client
    sys.path.insert(0, str(repo_dir))

    client_config = NetConfig(
        pools={
            "remote": PoolConfig(
                spec=RemotePoolConfig(
                    url=f"ws://localhost:{pool_port}",
                    worker_name="execution_manager",
                    num_processes=1,
                    threads_per_process=1,
                ),
            ),
        },
        graph=GraphConfig(
            nodes=[
                NodeConfig(
                    factory="netrun.node_factories.from_function",
                    factory_args={"func": "nodes.doubler"},
                    execution_config=NodeExecutionConfig(pools=["remote"]),
                ),
            ],
            edges=[],
        ),
    )

    async def _run_e2e():
        async with Net(client_config) as net:
            net.inject_data("doubler", "x", [5])
            await net.run_until_blocked()

            outputs = net.flush_all_output_queues()
            values = [v for vs in outputs.values() for v in vs]
            assert 10 in values, f"Expected 10 in output values, got {values}"

            await net.request_pool_shutdown("remote")

    asyncio.run(_run_e2e())

finally:
    # --- Cleanup ---
    sys.path[:] = original_sys_path
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
    shutil.rmtree(tmpdir, ignore_errors=True)
