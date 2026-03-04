"""netrun-ui backend API."""

# ---------------------------------------------------------------------------
# Monkey-patch NodeVariable to allow missing `value` field.
#
# In the core netrun package, NodeVariable.value is required — this is
# intentional so that runtime validation catches unfilled vars early.
# However, the UI editor needs to save configs that declare vars *without*
# values (to be injected later via NetConfig.from_file overrides).
#
# We patch the model here — before any route module imports it — so that
# pydantic accepts {"type": "str"} without a "value" key.
# ---------------------------------------------------------------------------
def _patch_node_variable_optional_value():
    from netrun.net.config._nodes import (
        NodeVariable, NodeExecutionConfig, NodeConfig, SubgraphConfig,
    )
    from netrun.net.config._graph import GraphConfig
    from netrun.net.config._net_config import NetConfig
    from netrun.net.config._base import VarRef

    # Make value optional (default None)
    field = NodeVariable.model_fields['value']
    field.default = None
    field.annotation = str | VarRef | None
    NodeVariable.model_rebuild(force=True)

    # Rebuild all models that (transitively) reference NodeVariable
    NodeExecutionConfig.model_rebuild(force=True)
    NodeConfig.model_rebuild(force=True)
    SubgraphConfig.model_rebuild(force=True)
    GraphConfig.model_rebuild(force=True)
    NetConfig.model_rebuild(force=True)

_patch_node_variable_optional_value()
