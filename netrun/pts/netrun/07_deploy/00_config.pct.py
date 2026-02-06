# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Deploy Configuration Models

# %%
#|default_exp deploy._config

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Literal
from pathlib import Path
import json
import subprocess
import tomllib

# %% [markdown]
# ## SSH Configuration

# %%
#|export
class SSHConfig(BaseModel):
    """SSH connection configuration."""
    host: str
    port: int = 22
    user: str
    ssh_key: str | None = None
    ssh_key_password: str | None = None

# %% [markdown]
# ## Repository Configuration

# %%
#|export
class RepoConfig(BaseModel):
    """Repository configuration for deployment target."""
    git_url: str | None = None
    local_repo_path: str | None = None
    local_folder_path: str | None = None
    """Path to a local folder to upload directly (no git required)."""
    branch: str | None = None
    remote_dir: str
    ssh_keyscan: bool = True

    @model_validator(mode='after')
    def validate_git_source(self) -> "RepoConfig":
        """Exactly one of git_url, local_repo_path, or local_folder_path must be set."""
        sources = [self.git_url, self.local_repo_path, self.local_folder_path]
        set_count = sum(1 for s in sources if s is not None)
        if set_count != 1:
            raise ValueError(
                "Must specify exactly one of 'git_url', 'local_repo_path', or 'local_folder_path'."
            )
        return self

    def resolve_git_url(self) -> str | None:
        """Return the git URL, inferring from local_repo_path if needed.

        Returns None when local_folder_path is set (no git URL to resolve).
        """
        if self.local_folder_path is not None:
            return None
        if self.git_url is not None:
            return self.git_url
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=self.local_repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

# %% [markdown]
# ## Environment Setup Configuration

# %%
#|export
class InlineScriptEnvSetup(BaseModel):
    """Environment setup via an inline shell script."""
    type: Literal["inline_script"] = "inline_script"
    script: str


class ScriptFileEnvSetup(BaseModel):
    """Environment setup via a local script file uploaded to remote."""
    type: Literal["script_file"] = "script_file"
    local_path: str


class UvEnvSetup(BaseModel):
    """Environment setup via uv."""
    type: Literal["uv"] = "uv"
    python_version: str | None = None
    extra_args: str = ""


class PixiEnvSetup(BaseModel):
    """Environment setup via pixi."""
    type: Literal["pixi"] = "pixi"
    environment: str | None = None


class CondaEnvSetup(BaseModel):
    """Environment setup via conda."""
    type: Literal["conda"] = "conda"
    env_name: str
    env_file: str = "environment.yml"


EnvSetupConfig = Annotated[
    InlineScriptEnvSetup | ScriptFileEnvSetup | UvEnvSetup | PixiEnvSetup | CondaEnvSetup,
    Field(discriminator="type")
]

# %% [markdown]
# ## File Upload Configuration

# %%
#|export
class FileUpload(BaseModel):
    """A file to upload from local to remote."""
    local_path: str
    remote_path: str

# %% [markdown]
# ## Net Source Configuration

# %%
#|export
class ImportPathNetSource(BaseModel):
    """Load Net config from a Python import path."""
    type: Literal["import_path"] = "import_path"
    path: str


class ConfigFileNetSource(BaseModel):
    """Load Net config from a config file (TOML/JSON)."""
    type: Literal["config_file"] = "config_file"
    path: str


class FileVarNetSource(BaseModel):
    """Load Net config from a Python file variable (path::var_name)."""
    type: Literal["file_var"] = "file_var"
    path: str


NetSourceConfig = Annotated[
    ImportPathNetSource | ConfigFileNetSource | FileVarNetSource,
    Field(discriminator="type")
]

# %% [markdown]
# ## Pool Server Configuration

# %%
#|export
class PoolServerConfig(BaseModel):
    """Configuration for the remote pool server process."""
    host: str = "0.0.0.0"
    port: int = 8080
    worker_name: str = "execution_manager"
    log_file: str | None = "pool_server.log"

# %% [markdown]
# ## Deploy Configuration (top-level)

# %%
#|export
class DeployConfig(BaseModel):
    """Top-level deployment configuration."""
    ssh: SSHConfig
    repo: RepoConfig
    env_setup: EnvSetupConfig | None = None
    file_uploads: list[FileUpload] = Field(default_factory=list)
    net_source: NetSourceConfig
    pool_server: PoolServerConfig = Field(default_factory=PoolServerConfig)
    pre_deploy_commands: list[str] = Field(default_factory=list)
    post_deploy_commands: list[str] = Field(default_factory=list)

    @classmethod
    def from_file(cls, path: str | Path) -> "DeployConfig":
        """Load a DeployConfig from a JSON or TOML file.

        Args:
            path: Path to the config file (.json or .toml).

        Returns:
            A DeployConfig instance.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file extension is not .json or .toml.
        """
        path = Path(path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        content = path.read_text()
        suffix = path.suffix.lower()

        if suffix == ".json":
            data = json.loads(content)
        elif suffix == ".toml":
            data = tomllib.loads(content)
        else:
            raise ValueError(f"Unsupported config file format: {suffix}. Use .json or .toml.")

        return cls.model_validate(data)

# %% [markdown]
# ## Deploy Result

# %%
#|export
class DeployResult(BaseModel):
    """Result of a deployment operation."""
    host: str
    port: int
    pool_server_url: str
    pid_file: str
    success: bool
    errors: list[str] = Field(default_factory=list)
