"""Config schema introspection endpoint."""
from __future__ import annotations

from fastapi import APIRouter

from netrun.net.config import NetConfig, NodeConfig, NodeExecutionConfig
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
