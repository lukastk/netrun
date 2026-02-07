"""Deployment operations using pyinfra.

All remote work (file transfer, package installation, server start) is
expressed as pyinfra operations and executed in a single ``run_ops`` call.
"""

from __future__ import annotations

import io


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
    log_arg = f'"{log_file}"' if log_file else "None"

    lines = [
        "import asyncio, os, sys",
        "",
        "_dir = os.path.dirname(os.path.abspath(__file__))",
        "if _dir not in sys.path:",
        "    sys.path.insert(0, _dir)",
        'os.environ["PYTHONPATH"] = _dir + os.pathsep + os.environ.get("PYTHONPATH", "")',
        "",
        "from netrun.net import Net",
    ]

    if "::" in net_source:
        file_path, var = net_source.split("::", 1)
        lines += [
            "import importlib.util",
            f'_spec = importlib.util.spec_from_file_location("_m", "{file_path}")',
            "_mod = importlib.util.module_from_spec(_spec)",
            "sys.modules[_spec.name] = _mod",
            "_spec.loader.exec_module(_mod)",
            f'_cfg = getattr(_mod, "{var}")',
        ]
    elif net_source.endswith((".toml", ".json")):
        lines += [
            "from netrun.net.config import NetConfig",
            f'_cfg = NetConfig.from_file("{net_source}")',
        ]
    else:
        mod, attr = net_source.rsplit(".", 1)
        lines += [f"from {mod} import {attr} as _cfg"]

    lines += [
        "",
        "async def main():",
        f'    ctx = Net.serve_pool(_cfg, "{host}", {port}, '
        f'log_file={log_arg}, worker_name="{worker_name}")',
        "    await ctx.start()",
        "    await ctx.wait_until_stopped()",
        "    await ctx.stop()",
        "",
        'if __name__ == "__main__":',
        "    asyncio.run(main())",
    ]
    return "\n".join(lines) + "\n"


def build_start_script(remote_dir: str, use_uv: bool = True) -> str:
    """Generate the shell wrapper that launches the pool server via nohup."""
    prefix = "uv run " if use_uv else ""
    return (
        "#!/bin/sh\n"
        f'export PATH="$HOME/.local/bin:$PATH"\n'
        f"cd {remote_dir}\n"
        f"nohup {prefix}python .netrun_serve_pool.py "
        f"> pool_server_stdout.log 2>&1 &\n"
        f"echo $! > .netrun_serve_pool.pid\n"
    )


# ---------------------------------------------------------------------------
# pyinfra deployment
# ---------------------------------------------------------------------------

def run_deployment(
    *,
    host: str,
    user: str,
    ssh_key: str | None,
    local_folder: str | None,
    git_url: str | None,
    git_branch: str | None,
    remote_dir: str,
    serve_script: str,
    start_script: str,
    python_version: str | None,
    uv_extra_args: str,
    pre_commands: list[str] | None,
    setup_firewall: bool,
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
                files.sync(src=local_folder, dest=remote_dir)
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

            # 5. Upload scripts and start the pool server
            files.put(
                src=io.BytesIO(serve_script.encode()),
                dest=f"{remote_dir}/.netrun_serve_pool.py",
            )
            files.put(
                src=io.BytesIO(start_script.encode()),
                dest=f"{remote_dir}/.netrun_start.sh",
                mode="755",
            )
            server.shell(commands=[f"{remote_dir}/.netrun_start.sh"])

        # -- execute ---------------------------------------------------------
        run_ops(state)
    finally:
        ctx_state.reset()
        ctx_config.reset()
        ctx_host.reset()
