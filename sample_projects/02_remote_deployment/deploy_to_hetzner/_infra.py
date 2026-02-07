"""Deployment operations using pyinfra.

All remote work (file transfer, package installation, server start) is
expressed as pyinfra operations and executed in a single ``run_ops`` call.
"""

from __future__ import annotations

import io
from pathlib import Path

_ASSETS = Path(__file__).parent / "assets"


def _load_asset(name: str) -> str:
    return (_ASSETS / name).read_text()


# ---------------------------------------------------------------------------
# Serve / start script generation
# ---------------------------------------------------------------------------

def build_serve_script(
    net_source: str,
    host: str = "127.0.0.1",
    port: int = 8765,
    worker_name: str = "execution_manager",
    log_file: str | None = "pool_server.log",
) -> str:
    """Generate the Python script that starts a pool server on the remote.

    *net_source* determines how the ``NetConfig`` is loaded:

    - Ends with ``.toml`` or ``.json`` — config file (relative to script dir).
    - Contains ``::`` (e.g. ``config.py::my_var``) — Python file + variable.
    - Otherwise — dotted import path (e.g. ``myapp.config.net``).
    """
    if "::" in net_source:
        file_path, var = net_source.split("::", 1)
        config_loader = "\n".join([
            "import importlib.util",
            f'_spec = importlib.util.spec_from_file_location("_m", "{file_path}")',
            "_mod = importlib.util.module_from_spec(_spec)",
            "sys.modules[_spec.name] = _mod",
            "_spec.loader.exec_module(_mod)",
            f'_cfg = getattr(_mod, "{var}")',
        ])
    elif net_source.endswith((".toml", ".json")):
        config_loader = "\n".join([
            "from netrun.net.config import NetConfig",
            f'_cfg = NetConfig.from_file("{net_source}")',
        ])
    else:
        mod, attr = net_source.rsplit(".", 1)
        config_loader = f"from {mod} import {attr} as _cfg"

    log_arg = f'"{log_file}"' if log_file else "None"

    return (
        _load_asset("serve_pool.py.template")
        .replace("__CONFIG_LOADER__", config_loader)
        .replace("__HOST__", host)
        .replace("__PORT__", str(port))
        .replace("__LOG_ARG__", log_arg)
        .replace("__WORKER_NAME__", worker_name)
    )


def build_start_script(remote_dir: str, use_uv: bool = True) -> str:
    """Generate the shell wrapper that launches the pool server via nohup."""
    prefix = "uv run " if use_uv else ""
    return (
        _load_asset("start.sh")
        .replace("__REMOTE_DIR__", remote_dir)
        .replace("__RUN_PREFIX__", prefix)
    )


def build_watchdog_script(
    remote_dir: str,
    idle_minutes: int = 10,
    start_delay_minutes: int = 15,
) -> str:
    """Generate a bash script that auto-deletes the server after idle timeout.

    The script checks whether the pool server process is running (via PID file).
    If not running for longer than *idle_minutes*, it deletes the server via the
    Hetzner API.  A *start_delay_minutes* grace period prevents deletion during
    initial deployment / first use.
    """
    return (
        _load_asset("watchdog.sh")
        .replace("__REMOTE_DIR__", remote_dir)
        .replace("__IDLE_TIMEOUT__", str(idle_minutes * 60))
        .replace("__START_DELAY__", str(start_delay_minutes * 60))
    )


def build_watchdog_service(remote_dir: str) -> str:
    """Generate a systemd service unit for the watchdog."""
    return _load_asset("netrun-watchdog.service").replace("__REMOTE_DIR__", remote_dir)


def build_watchdog_timer() -> str:
    """Generate a systemd timer unit that fires the watchdog every minute."""
    return _load_asset("netrun-watchdog.timer")


# ---------------------------------------------------------------------------
# pyinfra deployment
# ---------------------------------------------------------------------------

def run_deployment(
    *,
    host: str,
    user: str,
    ssh_key: str | None,
    local_folder: str | None,
    exclude: list[str] | None,
    exclude_dir: list[str] | None,
    git_url: str | None,
    git_branch: str | None,
    remote_dir: str,
    serve_script: str,
    start_script: str,
    python_version: str | None,
    uv_extra_args: str,
    pre_commands: list[str] | None,
    setup_firewall: bool,
    netrun_install_spec: str | None,
    watchdog_script: str | None = None,
    watchdog_service: str | None = None,
    watchdog_timer: str | None = None,
    hcloud_api_token: str | None = None,
) -> None:
    """Execute all deployment operations on a remote host via pyinfra."""
    from pyinfra.api import Config, Inventory, State
    from pyinfra.api.connect import connect_all
    from pyinfra.api.operations import run_ops
    from pyinfra.context import ctx_config, ctx_host, ctx_state
    from pyinfra.operations import files, git, server

    # -- pyinfra inventory ---------------------------------------------------
    ssh_kwargs: dict = {"ssh_user": user}
    if ssh_key:
        ssh_kwargs["ssh_key"] = ssh_key

    pyinfra_config = Config()
    inventory = Inventory(([(host, ssh_kwargs)], {}))
    state = State(inventory, pyinfra_config)
    connect_all(state)

    # -- register operations -------------------------------------------------
    ctx_state.set(state)
    ctx_config.set(pyinfra_config)
    try:
        for host_obj in inventory:
            ctx_host.set(host_obj)

            # 1. Firewall — deny everything except SSH
            if setup_firewall:
                server.shell(commands=[
                    "apt-get update -qq && apt-get install -y -qq ufw > /dev/null 2>&1",
                    "ufw default deny incoming",
                    "ufw default allow outgoing",
                    "ufw allow 22/tcp",
                    "ufw --force enable",
                ])

            # 2. Pre-deploy commands
            if pre_commands:
                for cmd in pre_commands:
                    server.shell(commands=[cmd])

            # 3. Transfer code
            if local_folder:
                server.shell(commands=[
                    "which rsync > /dev/null 2>&1 "
                    "|| (apt-get update -qq "
                    "&& apt-get install -y -qq rsync > /dev/null 2>&1)",
                ])
                sync_kwargs: dict = {}
                if exclude:
                    sync_kwargs["exclude"] = exclude
                if exclude_dir:
                    sync_kwargs["exclude_dir"] = exclude_dir
                files.sync(src=local_folder, dest=remote_dir, **sync_kwargs)
            elif git_url:
                if git_url.startswith("git@"):
                    git_host = git_url.split("@")[1].split(":")[0]
                    server.shell(commands=[
                        f"ssh-keyscan -H {git_host} "
                        f">> ~/.ssh/known_hosts 2>/dev/null || true",
                    ])
                branch_kwargs = {}
                if git_branch:
                    branch_kwargs["branch"] = git_branch
                git.repo(src=git_url, dest=remote_dir, **branch_kwargs)

            # 4. Install uv + set up Python environment
            server.shell(commands=[
                "curl -LsSf https://astral.sh/uv/install.sh | sh",
            ])
            uv_parts = [
                'export PATH="$HOME/.local/bin:$PATH"',
                f"cd {remote_dir}",
            ]
            if python_version:
                uv_parts.append(f"uv python install {python_version}")
            sync_cmd = "uv sync"
            if uv_extra_args:
                sync_cmd += f" {uv_extra_args}"
            uv_parts.append(sync_cmd)
            server.shell(commands=[" && ".join(uv_parts)])

            # 4b. Override netrun with git version if requested
            if netrun_install_spec:
                git_install_parts = [
                    'export PATH="$HOME/.local/bin:$PATH"',
                    f"cd {remote_dir}",
                    f"uv pip install --reinstall --python .venv/bin/python "
                    f"'{netrun_install_spec}'",
                ]
                server.shell(commands=[" && ".join(git_install_parts)])

            # 5. Upload scripts (server is started separately via start_pool_server())
            files.put(
                src=io.BytesIO(serve_script.encode()),
                dest=f"{remote_dir}/.netrun_serve_pool.py",
            )
            files.put(
                src=io.BytesIO(start_script.encode()),
                dest=f"{remote_dir}/.netrun_start.sh",
                mode="755",
            )

            # 6. Auto-delete watchdog (opt-in)
            if watchdog_script and watchdog_service and watchdog_timer and hcloud_api_token:
                files.put(
                    src=io.BytesIO(watchdog_script.encode()),
                    dest=f"{remote_dir}/.netrun_watchdog.sh",
                    mode="755",
                )
                files.put(
                    src=io.BytesIO(hcloud_api_token.encode()),
                    dest=f"{remote_dir}/.hcloud_token",
                    mode="600",
                )
                files.put(
                    src=io.BytesIO(watchdog_service.encode()),
                    dest="/etc/systemd/system/netrun-watchdog.service",
                )
                files.put(
                    src=io.BytesIO(watchdog_timer.encode()),
                    dest="/etc/systemd/system/netrun-watchdog.timer",
                )
                server.shell(commands=[
                    "systemctl daemon-reload "
                    "&& systemctl enable --now netrun-watchdog.timer",
                ])

        # -- execute ---------------------------------------------------------
        run_ops(state)
    finally:
        ctx_state.reset()
        ctx_config.reset()
        ctx_host.reset()
