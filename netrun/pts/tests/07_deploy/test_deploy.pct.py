# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Deploy Functions

# %%
#|default_exp deploy.test_deploy

# %%
#|export
import pytest
from unittest.mock import patch, MagicMock

from netrun.deploy._config import (
    SSHConfig,
    RepoConfig,
    DeployConfig,
    DeployResult,
    ImportPathNetSource,
    ConfigFileNetSource,
    FileVarNetSource,
    UvEnvSetup,
    PixiEnvSetup,
    CondaEnvSetup,
    InlineScriptEnvSetup,
    ScriptFileEnvSetup,
    PoolServerConfig,
)
from netrun.deploy._deploy import (
    _build_serve_script,
    _get_env_prefix,
    _build_nohup_command,
    update_pool_url,
)
from netrun.deploy._env_scripts import (
    get_uv_setup_script,
    get_pixi_setup_script,
    get_conda_setup_script,
    get_env_setup_script,
)
from netrun.net.config._net_config import (
    NetConfig,
    PoolConfig,
    RemotePoolConfig,
    ThreadPoolConfig,
)
from netrun.net.config._graph import GraphConfig
from netrun.net.config._nodes import NodeConfig

# %% [markdown]
# ## RepoConfig local_folder_path Tests

# %%
#|export
def test_repo_config_local_folder_path():
    """Test RepoConfig accepts local_folder_path as sole source."""
    cfg = RepoConfig(local_folder_path="/tmp/my_project", remote_dir="/opt/app")
    assert cfg.local_folder_path == "/tmp/my_project"
    assert cfg.git_url is None
    assert cfg.local_repo_path is None

# %%
test_repo_config_local_folder_path();

# %%
#|export
def test_repo_config_folder_exclusive():
    """Test RepoConfig rejects local_folder_path combined with git_url."""
    with pytest.raises(ValueError, match="exactly one"):
        RepoConfig(local_folder_path="/tmp/proj", git_url="git@github.com:u/r.git", remote_dir="/opt/app")

# %%
test_repo_config_folder_exclusive();

# %%
#|export
def test_repo_config_resolve_git_url_folder():
    """Test resolve_git_url returns None for folder mode."""
    cfg = RepoConfig(local_folder_path="/tmp/my_project", remote_dir="/opt/app")
    assert cfg.resolve_git_url() is None

# %%
test_repo_config_resolve_git_url_folder();

# %% [markdown]
# ## _build_serve_script Tests

# %%
#|export
def _make_deploy_config(**overrides):
    """Helper to create a DeployConfig for testing."""
    defaults = dict(
        ssh=SSHConfig(host="example.com", user="deploy"),
        repo=RepoConfig(git_url="git@github.com:user/repo.git", remote_dir="/opt/app"),
        net_source=ImportPathNetSource(path="myapp.config.net_config"),
    )
    defaults.update(overrides)
    return DeployConfig(**defaults)

# %%
#|export
def test_build_serve_script_import_path():
    """Test _build_serve_script with import_path net source."""
    config = _make_deploy_config(
        net_source=ImportPathNetSource(path="myapp.config.net_config"),
    )
    script = _build_serve_script(config)
    assert "from myapp.config import net_config as _net_config" in script
    assert "Net.serve_pool" in script
    assert "asyncio.run(main())" in script
    # Verify sys.path / PYTHONPATH setup for repo dir importability
    assert "sys.path.insert(0, _script_dir)" in script
    assert 'os.environ["PYTHONPATH"]' in script

# %%
test_build_serve_script_import_path();

# %%
#|export
def test_build_serve_script_config_file():
    """Test _build_serve_script with config_file net source."""
    config = _make_deploy_config(
        net_source=ConfigFileNetSource(path="config.netrun.toml"),
    )
    script = _build_serve_script(config)
    assert 'NetConfig.from_file("config.netrun.toml")' in script
    assert "from netrun.net.config import NetConfig" in script

# %%
test_build_serve_script_config_file();

# %%
#|export
def test_build_serve_script_file_var():
    """Test _build_serve_script with file_var net source."""
    config = _make_deploy_config(
        net_source=FileVarNetSource(path="config.py::my_net_config"),
    )
    script = _build_serve_script(config)
    assert 'spec_from_file_location("_net_mod", "config.py")' in script
    assert 'getattr(_mod, "my_net_config")' in script

# %%
test_build_serve_script_file_var();

# %%
#|export
def test_build_serve_script_custom_pool_server():
    """Test _build_serve_script respects pool_server config."""
    config = _make_deploy_config(
        pool_server=PoolServerConfig(host="127.0.0.1", port=9090, worker_name="custom", log_file="server.log"),
    )
    script = _build_serve_script(config)
    assert '"127.0.0.1"' in script
    assert "9090" in script
    assert '"custom"' in script
    assert '"server.log"' in script

# %%
test_build_serve_script_custom_pool_server();

# %%
#|export
def test_build_serve_script_no_log_file():
    """Test _build_serve_script with log_file=None."""
    config = _make_deploy_config(
        pool_server=PoolServerConfig(log_file=None),
    )
    script = _build_serve_script(config)
    assert "log_file=None" in script

# %%
test_build_serve_script_no_log_file();

# %% [markdown]
# ## _get_env_prefix Tests

# %%
#|export
def test_get_env_prefix_none():
    """Test _get_env_prefix with no env_setup."""
    config = _make_deploy_config(env_setup=None)
    assert _get_env_prefix(config) == ""

# %%
test_get_env_prefix_none();

# %%
#|export
def test_get_env_prefix_uv():
    """Test _get_env_prefix with uv env setup."""
    config = _make_deploy_config(env_setup=UvEnvSetup())
    assert _get_env_prefix(config) == "uv run "

# %%
test_get_env_prefix_uv();

# %%
#|export
def test_get_env_prefix_pixi():
    """Test _get_env_prefix with pixi env setup."""
    config = _make_deploy_config(env_setup=PixiEnvSetup())
    assert _get_env_prefix(config) == "pixi run "

# %%
test_get_env_prefix_pixi();

# %%
#|export
def test_get_env_prefix_conda():
    """Test _get_env_prefix with conda env setup."""
    config = _make_deploy_config(env_setup=CondaEnvSetup(env_name="myenv"))
    assert _get_env_prefix(config) == "conda run -n myenv "

# %%
test_get_env_prefix_conda();

# %%
#|export
def test_get_env_prefix_inline_script():
    """Test _get_env_prefix with inline script returns empty string."""
    config = _make_deploy_config(env_setup=InlineScriptEnvSetup(script="echo hello"))
    assert _get_env_prefix(config) == ""

# %%
test_get_env_prefix_inline_script();

# %%
#|export
def test_get_env_prefix_script_file():
    """Test _get_env_prefix with script file returns empty string."""
    config = _make_deploy_config(env_setup=ScriptFileEnvSetup(local_path="./setup.sh"))
    assert _get_env_prefix(config) == ""

# %%
test_get_env_prefix_script_file();

# %% [markdown]
# ## _build_nohup_command Tests

# %%
#|export
def test_build_nohup_command_basic():
    """Test _build_nohup_command generates correct command shape."""
    config = _make_deploy_config()
    cmd = _build_nohup_command(config, "")
    assert cmd.startswith("cd /opt/app && ")
    assert "nohup" in cmd
    assert "python .netrun_serve_pool.py" in cmd
    assert "pool_server_stdout.log" in cmd
    assert ".netrun_serve_pool.pid" in cmd
    assert ".netrun_start.sh" in cmd

# %%
test_build_nohup_command_basic();

# %%
#|export
def test_build_nohup_command_with_prefix():
    """Test _build_nohup_command with env prefix."""
    config = _make_deploy_config()
    cmd = _build_nohup_command(config, "uv run ")
    assert "uv run python .netrun_serve_pool.py" in cmd

# %%
test_build_nohup_command_with_prefix();

# %% [markdown]
# ## update_pool_url Tests

# %%
#|export
def test_update_pool_url():
    """Test update_pool_url updates the remote pool URL."""
    net_config = NetConfig(
        graph=GraphConfig(nodes=[NodeConfig(name="A")]),
        pools={
            "main": PoolConfig(),
            "remote": PoolConfig(spec=RemotePoolConfig(url="ws://old:8080", worker_name="em")),
        },
    )
    deploy_result = DeployResult(
        host="newhost.com",
        port=9090,
        pool_server_url="ws://newhost.com:9090",
        pid_file="/opt/app/.netrun_serve_pool.pid",
        success=True,
    )

    updated = update_pool_url(net_config, "remote", deploy_result)

    # Original unchanged
    assert net_config.pools["remote"].spec.url == "ws://old:8080"
    # Updated has new URL
    assert updated.pools["remote"].spec.url == "ws://newhost.com:9090"
    # Other pools unchanged
    assert updated.pools["main"].spec.type == "main"

# %%
test_update_pool_url();

# %%
#|export
def test_update_pool_url_missing_pool():
    """Test update_pool_url raises KeyError for missing pool."""
    net_config = NetConfig(
        graph=GraphConfig(nodes=[NodeConfig(name="A")]),
        pools={"main": PoolConfig()},
    )
    deploy_result = DeployResult(
        host="h", port=8080, pool_server_url="ws://h:8080",
        pid_file="/p", success=True,
    )

    with pytest.raises(KeyError, match="nonexistent"):
        update_pool_url(net_config, "nonexistent", deploy_result)

# %%
test_update_pool_url_missing_pool();

# %%
#|export
def test_update_pool_url_non_remote_pool():
    """Test update_pool_url raises TypeError for non-remote pool."""
    net_config = NetConfig(
        graph=GraphConfig(nodes=[NodeConfig(name="A")]),
        pools={
            "threads": PoolConfig(spec=ThreadPoolConfig(num_workers=4)),
        },
    )
    deploy_result = DeployResult(
        host="h", port=8080, pool_server_url="ws://h:8080",
        pid_file="/p", success=True,
    )

    with pytest.raises(TypeError, match="not 'remote'"):
        update_pool_url(net_config, "threads", deploy_result)

# %%
test_update_pool_url_non_remote_pool();

# %% [markdown]
# ## deploy() Import Error Test

# %%
#|export
def test_deploy_raises_import_error_when_pyinfra_missing():
    """Test deploy() raises ImportError when pyinfra is not installed."""
    from netrun.deploy._deploy import deploy

    config = _make_deploy_config()
    with patch.dict("sys.modules", {"pyinfra": None, "pyinfra.api": None}):
        with patch("builtins.__import__", side_effect=_make_import_error("pyinfra")):
            with pytest.raises(ImportError, match="pyinfra is required"):
                deploy(config)

def _make_import_error(blocked_module):
    """Create a side_effect function that blocks import of a specific module."""
    _original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
    def _import(name, *args, **kwargs):
        if name.startswith(blocked_module):
            raise ImportError(f"No module named '{name}'")
        return _original_import(name, *args, **kwargs)
    return _import

# %%
test_deploy_raises_import_error_when_pyinfra_missing();

# %% [markdown]
# ## Env Script Generator Tests

# %%
#|export
def test_get_uv_setup_script_basic():
    """Test uv setup script generation."""
    config = UvEnvSetup()
    script = get_uv_setup_script(config, "/opt/app")
    assert "cd /opt/app" in script
    assert "curl -LsSf https://astral.sh/uv/install.sh" in script
    assert "uv sync" in script

# %%
test_get_uv_setup_script_basic();

# %%
#|export
def test_get_uv_setup_script_with_python_version():
    """Test uv setup script with python_version."""
    config = UvEnvSetup(python_version="3.12")
    script = get_uv_setup_script(config, "/opt/app")
    assert "uv python install 3.12" in script

# %%
test_get_uv_setup_script_with_python_version();

# %%
#|export
def test_get_uv_setup_script_with_extra_args():
    """Test uv setup script with extra_args."""
    config = UvEnvSetup(extra_args="--no-dev")
    script = get_uv_setup_script(config, "/opt/app")
    assert "uv sync --no-dev" in script

# %%
test_get_uv_setup_script_with_extra_args();

# %%
#|export
def test_get_pixi_setup_script_basic():
    """Test pixi setup script generation."""
    config = PixiEnvSetup()
    script = get_pixi_setup_script(config, "/opt/app")
    assert "cd /opt/app" in script
    assert "pixi.sh/install.sh" in script
    assert "pixi install" in script

# %%
test_get_pixi_setup_script_basic();

# %%
#|export
def test_get_pixi_setup_script_with_environment():
    """Test pixi setup script with environment."""
    config = PixiEnvSetup(environment="prod")
    script = get_pixi_setup_script(config, "/opt/app")
    assert "pixi install -e prod" in script

# %%
test_get_pixi_setup_script_with_environment();

# %%
#|export
def test_get_conda_setup_script():
    """Test conda setup script generation."""
    config = CondaEnvSetup(env_name="myenv", env_file="env.yml")
    script = get_conda_setup_script(config, "/opt/app")
    assert "cd /opt/app" in script
    assert "conda env update -n myenv -f env.yml --prune" in script

# %%
test_get_conda_setup_script();

# %%
#|export
def test_get_env_setup_script_dispatches():
    """Test get_env_setup_script dispatches to correct generator."""
    uv = UvEnvSetup()
    assert "uv sync" in get_env_setup_script(uv, "/app")

    pixi = PixiEnvSetup()
    assert "pixi install" in get_env_setup_script(pixi, "/app")

    conda = CondaEnvSetup(env_name="e")
    assert "conda env update" in get_env_setup_script(conda, "/app")

    inline = InlineScriptEnvSetup(script="custom command")
    assert get_env_setup_script(inline, "/app") == "custom command"

    script_file = ScriptFileEnvSetup(local_path="./setup.sh")
    assert "bash .netrun_env_setup.sh" in get_env_setup_script(script_file, "/app")

# %%
test_get_env_setup_script_dispatches();
