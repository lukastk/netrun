# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Deploy Config Models

# %%
#|default_exp deploy.test_config

# %%
#|export
import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from netrun.deploy._config import (
    SSHConfig,
    RepoConfig,
    InlineScriptEnvSetup,
    ScriptFileEnvSetup,
    UvEnvSetup,
    PixiEnvSetup,
    CondaEnvSetup,
    FileUpload,
    ImportPathNetSource,
    ConfigFileNetSource,
    FileVarNetSource,
    PoolServerConfig,
    DeployConfig,
    DeployResult,
)

# %% [markdown]
# ## SSHConfig Tests

# %%
#|export
def test_ssh_config_basic():
    """Test SSHConfig creation with required fields."""
    config = SSHConfig(host="example.com", user="deploy")
    assert config.host == "example.com"
    assert config.port == 22
    assert config.user == "deploy"
    assert config.ssh_key is None
    assert config.ssh_key_password is None

# %%
test_ssh_config_basic();

# %%
#|export
def test_ssh_config_full():
    """Test SSHConfig with all fields."""
    config = SSHConfig(
        host="10.0.0.1",
        port=2222,
        user="admin",
        ssh_key="/home/user/.ssh/id_rsa",
        ssh_key_password="secret",
    )
    assert config.port == 2222
    assert config.ssh_key == "/home/user/.ssh/id_rsa"

# %%
test_ssh_config_full();

# %% [markdown]
# ## RepoConfig Tests

# %%
#|export
def test_repo_config_with_git_url():
    """Test RepoConfig with git_url."""
    config = RepoConfig(git_url="git@github.com:user/repo.git", remote_dir="/opt/app")
    assert config.git_url == "git@github.com:user/repo.git"
    assert config.local_repo_path is None
    assert config.resolve_git_url() == "git@github.com:user/repo.git"

# %%
test_repo_config_with_git_url();

# %%
#|export
def test_repo_config_with_local_repo_path():
    """Test RepoConfig with local_repo_path and mocked git."""
    config = RepoConfig(local_repo_path="/home/user/project", remote_dir="/opt/app")
    assert config.local_repo_path == "/home/user/project"
    assert config.git_url is None

    mock_result = MagicMock()
    mock_result.stdout = "git@github.com:user/project.git\n"
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        url = config.resolve_git_url()
        assert url == "git@github.com:user/project.git"
        mock_run.assert_called_once()

# %%
test_repo_config_with_local_repo_path();

# %%
#|export
def test_repo_config_both_set_raises():
    """Test RepoConfig rejects both git_url and local_repo_path."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        RepoConfig(
            git_url="git@github.com:user/repo.git",
            local_repo_path="/home/user/project",
            remote_dir="/opt/app",
        )

# %%
test_repo_config_both_set_raises();

# %%
#|export
def test_repo_config_neither_set_raises():
    """Test RepoConfig rejects neither git_url nor local_repo_path."""
    with pytest.raises(ValueError, match="Must specify either"):
        RepoConfig(remote_dir="/opt/app")

# %%
test_repo_config_neither_set_raises();

# %%
#|export
def test_repo_config_with_branch():
    """Test RepoConfig with branch."""
    config = RepoConfig(git_url="git@github.com:user/repo.git", remote_dir="/opt/app", branch="develop")
    assert config.branch == "develop"

# %%
test_repo_config_with_branch();

# %% [markdown]
# ## EnvSetupConfig Discriminated Union Tests

# %%
#|export
def test_env_setup_inline_script():
    """Test InlineScriptEnvSetup deserialization."""
    data = {"type": "inline_script", "script": "pip install -r requirements.txt"}
    config = InlineScriptEnvSetup.model_validate(data)
    assert config.type == "inline_script"
    assert config.script == "pip install -r requirements.txt"

# %%
test_env_setup_inline_script();

# %%
#|export
def test_env_setup_script_file():
    """Test ScriptFileEnvSetup deserialization."""
    data = {"type": "script_file", "local_path": "./setup.sh"}
    config = ScriptFileEnvSetup.model_validate(data)
    assert config.type == "script_file"
    assert config.local_path == "./setup.sh"

# %%
test_env_setup_script_file();

# %%
#|export
def test_env_setup_uv():
    """Test UvEnvSetup deserialization."""
    data = {"type": "uv", "python_version": "3.12", "extra_args": "--no-dev"}
    config = UvEnvSetup.model_validate(data)
    assert config.type == "uv"
    assert config.python_version == "3.12"
    assert config.extra_args == "--no-dev"

# %%
test_env_setup_uv();

# %%
#|export
def test_env_setup_uv_defaults():
    """Test UvEnvSetup with defaults."""
    config = UvEnvSetup()
    assert config.python_version is None
    assert config.extra_args == ""

# %%
test_env_setup_uv_defaults();

# %%
#|export
def test_env_setup_pixi():
    """Test PixiEnvSetup deserialization."""
    data = {"type": "pixi", "environment": "prod"}
    config = PixiEnvSetup.model_validate(data)
    assert config.type == "pixi"
    assert config.environment == "prod"

# %%
test_env_setup_pixi();

# %%
#|export
def test_env_setup_conda():
    """Test CondaEnvSetup deserialization."""
    data = {"type": "conda", "env_name": "myenv", "env_file": "env.yml"}
    config = CondaEnvSetup.model_validate(data)
    assert config.type == "conda"
    assert config.env_name == "myenv"
    assert config.env_file == "env.yml"

# %%
test_env_setup_conda();

# %%
#|export
def test_env_setup_conda_defaults():
    """Test CondaEnvSetup defaults."""
    config = CondaEnvSetup(env_name="myenv")
    assert config.env_file == "environment.yml"

# %%
test_env_setup_conda_defaults();

# %% [markdown]
# ## NetSourceConfig Discriminated Union Tests

# %%
#|export
def test_net_source_import_path():
    """Test ImportPathNetSource deserialization."""
    data = {"type": "import_path", "path": "myproject.config.net_config"}
    config = ImportPathNetSource.model_validate(data)
    assert config.type == "import_path"
    assert config.path == "myproject.config.net_config"

# %%
test_net_source_import_path();

# %%
#|export
def test_net_source_config_file():
    """Test ConfigFileNetSource deserialization."""
    data = {"type": "config_file", "path": "config.netrun.toml"}
    config = ConfigFileNetSource.model_validate(data)
    assert config.type == "config_file"
    assert config.path == "config.netrun.toml"

# %%
test_net_source_config_file();

# %%
#|export
def test_net_source_file_var():
    """Test FileVarNetSource deserialization."""
    data = {"type": "file_var", "path": "config.py::my_net_config"}
    config = FileVarNetSource.model_validate(data)
    assert config.type == "file_var"
    assert config.path == "config.py::my_net_config"

# %%
test_net_source_file_var();

# %% [markdown]
# ## PoolServerConfig Tests

# %%
#|export
def test_pool_server_config_defaults():
    """Test PoolServerConfig defaults."""
    config = PoolServerConfig()
    assert config.host == "0.0.0.0"
    assert config.port == 8080
    assert config.worker_name == "execution_manager"
    assert config.log_file == "pool_server.log"

# %%
test_pool_server_config_defaults();

# %%
#|export
def test_pool_server_config_custom():
    """Test PoolServerConfig with custom values."""
    config = PoolServerConfig(host="127.0.0.1", port=9090, worker_name="custom_worker", log_file=None)
    assert config.host == "127.0.0.1"
    assert config.port == 9090
    assert config.worker_name == "custom_worker"
    assert config.log_file is None

# %%
test_pool_server_config_custom();

# %% [markdown]
# ## DeployConfig Tests

# %%
#|export
def test_deploy_config_minimal():
    """Test DeployConfig with minimal required fields."""
    config = DeployConfig(
        ssh=SSHConfig(host="example.com", user="deploy"),
        repo=RepoConfig(git_url="git@github.com:user/repo.git", remote_dir="/opt/app"),
        net_source=ImportPathNetSource(path="myapp.config.net"),
    )
    assert config.env_setup is None
    assert config.file_uploads == []
    assert config.pool_server.port == 8080
    assert config.pre_deploy_commands == []
    assert config.post_deploy_commands == []

# %%
test_deploy_config_minimal();

# %%
#|export
def test_deploy_config_full():
    """Test DeployConfig with all fields."""
    config = DeployConfig(
        ssh=SSHConfig(host="10.0.0.1", port=2222, user="admin", ssh_key="~/.ssh/id_ed25519"),
        repo=RepoConfig(git_url="git@github.com:org/project.git", remote_dir="/opt/project", branch="main"),
        env_setup=UvEnvSetup(python_version="3.12"),
        file_uploads=[FileUpload(local_path=".env", remote_path="/opt/project/.env")],
        net_source=ConfigFileNetSource(path="config.netrun.toml"),
        pool_server=PoolServerConfig(port=9090),
        pre_deploy_commands=["systemctl stop myapp"],
        post_deploy_commands=["systemctl start myapp"],
    )
    assert config.ssh.port == 2222
    assert config.env_setup.type == "uv"
    assert len(config.file_uploads) == 1
    assert config.pool_server.port == 9090

# %%
test_deploy_config_full();

# %% [markdown]
# ## DeployConfig.from_file Tests

# %%
#|export
def test_deploy_config_from_file_json():
    """Test DeployConfig.from_file() with a JSON file."""
    data = {
        "ssh": {"host": "example.com", "user": "deploy"},
        "repo": {"git_url": "git@github.com:user/repo.git", "remote_dir": "/opt/app"},
        "net_source": {"type": "import_path", "path": "myapp.config.net"},
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "deploy.json"
        path.write_text(json.dumps(data))

        config = DeployConfig.from_file(path)
        assert config.ssh.host == "example.com"
        assert config.repo.git_url == "git@github.com:user/repo.git"

# %%
test_deploy_config_from_file_json();

# %%
#|export
def test_deploy_config_from_file_toml():
    """Test DeployConfig.from_file() with a TOML file."""
    toml_content = """
[ssh]
host = "example.com"
user = "deploy"

[repo]
git_url = "git@github.com:user/repo.git"
remote_dir = "/opt/app"

[net_source]
type = "config_file"
path = "config.netrun.toml"
"""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "deploy.toml"
        path.write_text(toml_content)

        config = DeployConfig.from_file(path)
        assert config.ssh.host == "example.com"
        assert config.net_source.type == "config_file"

# %%
test_deploy_config_from_file_toml();

# %%
#|export
def test_deploy_config_from_file_not_found():
    """Test DeployConfig.from_file() raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        DeployConfig.from_file("/nonexistent/deploy.json")

# %%
test_deploy_config_from_file_not_found();

# %%
#|export
def test_deploy_config_from_file_unsupported_format():
    """Test DeployConfig.from_file() raises ValueError for unsupported format."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "deploy.yaml"
        path.write_text("ssh:\n  host: example.com\n")

        with pytest.raises(ValueError, match="Unsupported config file format"):
            DeployConfig.from_file(path)

# %%
test_deploy_config_from_file_unsupported_format();

# %% [markdown]
# ## DeployResult Tests

# %%
#|export
def test_deploy_result():
    """Test DeployResult creation."""
    result = DeployResult(
        host="example.com",
        port=8080,
        pool_server_url="ws://example.com:8080",
        pid_file="/opt/app/.netrun_serve_pool.pid",
        success=True,
    )
    assert result.pool_server_url == "ws://example.com:8080"
    assert result.success is True
    assert result.errors == []

# %%
test_deploy_result();

# %%
#|export
def test_deploy_result_with_errors():
    """Test DeployResult with errors."""
    result = DeployResult(
        host="example.com",
        port=8080,
        pool_server_url="ws://example.com:8080",
        pid_file="/opt/app/.netrun_serve_pool.pid",
        success=False,
        errors=["Connection refused", "Timeout"],
    )
    assert result.success is False
    assert len(result.errors) == 2

# %%
test_deploy_result_with_errors();

# %% [markdown]
# ## JSON Serialization Roundtrip Tests

# %%
#|export
def test_deploy_config_json_roundtrip():
    """Test DeployConfig JSON serialization roundtrip."""
    config = DeployConfig(
        ssh=SSHConfig(host="example.com", user="deploy"),
        repo=RepoConfig(git_url="git@github.com:user/repo.git", remote_dir="/opt/app"),
        env_setup=UvEnvSetup(python_version="3.12"),
        net_source=ImportPathNetSource(path="myapp.config.net"),
        pool_server=PoolServerConfig(port=9090),
    )
    json_str = config.model_dump_json()
    loaded = DeployConfig.model_validate_json(json_str)

    assert loaded.ssh.host == config.ssh.host
    assert loaded.repo.git_url == config.repo.git_url
    assert loaded.env_setup.type == "uv"
    assert loaded.env_setup.python_version == "3.12"
    assert loaded.net_source.path == "myapp.config.net"
    assert loaded.pool_server.port == 9090

# %%
test_deploy_config_json_roundtrip();
