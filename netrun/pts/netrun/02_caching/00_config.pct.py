# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp caching.config

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import netrun.caching.config as this_module

# %%
#|export
from enum import Enum
from pydantic import Field

from netrun.net.config._base import EnvVar, EnvVarResolvableModel
from netrun._iutils.hashing import HashMethod
from netrun._iutils.pickling import PicklingMethod

# %%
#|hide
show_doc(this_module.CacheWhat)

# %%
#|export
class CacheWhat(Enum):
    """What to cache for each epoch."""
    BOTH = "both"
    OUTPUT_ONLY = "output"
    INPUT_ONLY = "input"

# %%
#|hide
show_doc(this_module.CacheConfig)

# %%
#|export
class CacheConfig(EnvVarResolvableModel):
    """Net-level cache configuration."""
    enabled: bool | EnvVar = False
    version: int | EnvVar = Field(default=0, description="Cache version. Changing this invalidates all cached entries.")
    storage_path: str | EnvVar | None = Field(default=None, description="Directory for cache storage. None = auto-generated temp directory.")
    include_nodes: list[str] | EnvVar | None = Field(default=None, description="Glob patterns for node names to cache.")
    exclude_nodes: list[str] | EnvVar | None = Field(default=None, description="Glob patterns for node names to exclude from caching.")
    include_all_nodes: bool | EnvVar = Field(default=False, description="Cache all nodes (overrides include_nodes).")
    cache_what: CacheWhat | EnvVar = Field(default=CacheWhat.BOTH, description="What to cache: both (memoization), output only, or input only.")
    hash_method: HashMethod | EnvVar = Field(default=HashMethod.xxh64, description="Hash algorithm for input salvo hashing.")
    pickling_method: PicklingMethod | EnvVar = Field(default=PicklingMethod.pickle, description="Pickling method for serialization.")
    pickling_args: dict | EnvVar = Field(default_factory=dict, description="Arguments passed to the pickler.")
    evaluate_lazy_value_for_cache: bool | EnvVar = Field(default=False, description="Evaluate lazy values before hashing/caching.")
    sample_size: int | EnvVar | None = Field(default=None, description="Max cached entries per node (reservoir sampling). None = unlimited.")

# %%
#|hide
show_doc(this_module.NodeCacheConfig)

# %%
#|export
class NodeCacheConfig(EnvVarResolvableModel):
    """Per-node cache overrides. None values inherit from CacheConfig."""
    enabled: bool | EnvVar | None = None
    version: int | EnvVar | None = None
    cache_what: CacheWhat | EnvVar | None = None
    hash_method: HashMethod | EnvVar | None = None
    pickling_method: PicklingMethod | EnvVar | None = None
    pickling_args: dict | EnvVar | None = None
    evaluate_lazy_value_for_cache: bool | EnvVar | None = None
    sample_size: int | EnvVar | None = None

# %% [markdown]
# ## Quick validation

# %%
config = CacheConfig()
assert config.enabled is False
assert config.cache_what == CacheWhat.BOTH
assert config.hash_method == HashMethod.xxh64

node_config = NodeCacheConfig()
assert node_config.enabled is None
assert node_config.version is None
