"""Config schema introspection endpoint."""
from __future__ import annotations

from fastapi import APIRouter

from netrun.net.config import NetConfig, NodeConfig, NodeExecutionConfig

from ..schema import ConfigSchemaResponse, get_model_schema

router = APIRouter()

# Pre-compute schemas at import time (model definitions are static)
_SCHEMAS = ConfigSchemaResponse(models={
    "NetConfig": get_model_schema(NetConfig, "NetConfig"),
    "NodeConfig": get_model_schema(NodeConfig, "NodeConfig"),
    "NodeExecutionConfig": get_model_schema(NodeExecutionConfig, "NodeExecutionConfig"),
})


@router.get("/schema", response_model=ConfigSchemaResponse)
async def get_config_schema() -> ConfigSchemaResponse:
    """Return field schemas for NetConfig, NodeConfig, and NodeExecutionConfig."""
    return _SCHEMAS
