"""Config schema introspection endpoint."""
from __future__ import annotations

from fastapi import APIRouter

from netrun.net.config import NetConfig, NodeConfig, NodeExecutionConfig
from netrun.net.config._base import VALID_SIGNAL_TYPES, SIGNAL_PORT_PREFIX, SIGNAL_PORT_SUFFIX
from netrun.net.config._base import VALID_CONTROL_TYPES, CONTROL_PORT_PREFIX, CONTROL_PORT_SUFFIX
from netrun.net.config._net_config import OutputQueueConfig
from netrun.storage.config import (
    CacheConfig,
    GCSBackendConfig,
    LocalBackendConfig,
    NodeCacheConfig,
    NodeFileStorageConfig,
    NodeStorageConfig,
    RcloneBackendConfig,
    S3BackendConfig,
    SSHBackendConfig,
    StorageConfig,
)

from ..schema import ConfigSchemaResponse, get_model_schema

router = APIRouter()

# Pre-compute schemas at import time (model definitions are static)
_SCHEMAS = ConfigSchemaResponse(models={
    # Top-level models
    "NetConfig": get_model_schema(NetConfig, "NetConfig"),
    "NodeConfig": get_model_schema(NodeConfig, "NodeConfig"),
    "NodeExecutionConfig": get_model_schema(NodeExecutionConfig, "NodeExecutionConfig"),
    # Storage sub-models
    "StorageConfig": get_model_schema(StorageConfig, "StorageConfig"),
    "CacheConfig": get_model_schema(CacheConfig, "CacheConfig"),
    "NodeStorageConfig": get_model_schema(NodeStorageConfig, "NodeStorageConfig"),
    "NodeCacheConfig": get_model_schema(NodeCacheConfig, "NodeCacheConfig"),
    "NodeFileStorageConfig": get_model_schema(NodeFileStorageConfig, "NodeFileStorageConfig"),
    # Backend configs
    "LocalBackendConfig": get_model_schema(LocalBackendConfig, "LocalBackendConfig"),
    "S3BackendConfig": get_model_schema(S3BackendConfig, "S3BackendConfig"),
    "GCSBackendConfig": get_model_schema(GCSBackendConfig, "GCSBackendConfig"),
    "SSHBackendConfig": get_model_schema(SSHBackendConfig, "SSHBackendConfig"),
    "RcloneBackendConfig": get_model_schema(RcloneBackendConfig, "RcloneBackendConfig"),
    # Output queues
    "OutputQueueConfig": get_model_schema(OutputQueueConfig, "OutputQueueConfig"),
})


@router.get("/schema", response_model=ConfigSchemaResponse)
async def get_config_schema() -> ConfigSchemaResponse:
    """Return field schemas for NetConfig, NodeConfig, and NodeExecutionConfig."""
    return _SCHEMAS


# Pre-compute signal types info at import time (static data)
_SIGNAL_TYPES_INFO = {
    "valid_types": sorted(VALID_SIGNAL_TYPES),
    "port_prefix": SIGNAL_PORT_PREFIX,
    "port_suffix": SIGNAL_PORT_SUFFIX,
}


@router.get("/signal-types")
async def get_signal_types() -> dict:
    """Return valid signal types and naming convention."""
    return _SIGNAL_TYPES_INFO


# Pre-compute control types info at import time (static data)
_CONTROL_TYPES_INFO = {
    "valid_types": sorted(VALID_CONTROL_TYPES),
    "port_prefix": CONTROL_PORT_PREFIX,
    "port_suffix": CONTROL_PORT_SUFFIX,
}


@router.get("/control-types")
async def get_control_types() -> dict:
    """Return valid control types and naming convention."""
    return _CONTROL_TYPES_INFO
