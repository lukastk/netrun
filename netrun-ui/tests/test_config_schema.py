"""Tests for config schema introspection.

These tests ensure that:
1. The schema endpoint returns correct field classifications
2. Every model field has a registration in configFieldRegistrations.ts
3. No stale registrations exist for removed fields
"""
import pathlib
import re

import pytest

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
from netrun_ui_backend.schema import FieldCategory, get_model_schema

# ------------------------------------------------------------------
# Model classes that the UI must handle.
#
# When you add a field to any of these models, the registration tests
# will fail until you register the field in:
#   src/lib/configFieldRegistrations.ts
# ------------------------------------------------------------------
MODEL_CLASSES: dict[str, type] = {
    "NetConfig": NetConfig,
    "NodeConfig": NodeConfig,
    "NodeExecutionConfig": NodeExecutionConfig,
    "StorageConfig": StorageConfig,
    "CacheConfig": CacheConfig,
    "NodeStorageConfig": NodeStorageConfig,
    "NodeCacheConfig": NodeCacheConfig,
    "NodeFileStorageConfig": NodeFileStorageConfig,
    "LocalBackendConfig": LocalBackendConfig,
    "S3BackendConfig": S3BackendConfig,
    "GCSBackendConfig": GCSBackendConfig,
    "SSHBackendConfig": SSHBackendConfig,
    "RcloneBackendConfig": RcloneBackendConfig,
    "OutputQueueConfig": OutputQueueConfig,
}


class TestSchemaClassification:
    """Test that get_model_schema correctly classifies field types."""

    def test_net_config_field_count(self):
        schema = get_model_schema(NetConfig, "NetConfig")
        assert len(schema.fields) == len(NetConfig.model_fields)

    def test_node_execution_config_field_count(self):
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        assert len(schema.fields) == len(NodeExecutionConfig.model_fields)

    def test_bool_field(self):
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "dead_letter_queue")
        assert field.category == FieldCategory.BOOL
        assert field.default is True

    def test_bool_or_null_field(self):
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "type_checking_enabled")
        assert field.category == FieldCategory.BOOL_OR_NULL
        assert field.default is None

    def test_enum_field(self):
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "default_pool_allocation_method")
        assert field.category == FieldCategory.ENUM
        assert "round-robin" in field.enum_values
        assert field.default == "round-robin"

    def test_enum_or_null_field(self):
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "pool_allocation_method")
        assert field.category == FieldCategory.ENUM_OR_NULL
        assert field.default is None
        assert "round-robin" in field.enum_values

    def test_int_or_null_field(self):
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "max_parallel_epochs")
        assert field.category == FieldCategory.INT_OR_NULL

    def test_float_field(self):
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "retry_wait")
        assert field.category == FieldCategory.FLOAT

    def test_complex_field(self):
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "dead_letter_callback")
        assert field.category == FieldCategory.COMPLEX

    def test_str_or_null_field(self):
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "project_root_override")
        assert field.category == FieldCategory.STR_OR_NULL

    def test_error_on_undeclared_output(self):
        """Verify the correct field name (not 'undeclared_output_behavior')."""
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "error_on_undeclared_output")
        assert field.category == FieldCategory.BOOL
        assert field.default is False


class TestEnvVarStripping:
    """Test that EnvVar is stripped from unions before classification."""

    def test_bool_with_envvar_stays_bool(self):
        """bool | EnvVar should classify as BOOL, not COMPLEX."""
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "dead_letter_queue")
        assert field.category == FieldCategory.BOOL

    def test_bool_or_null_with_envvar(self):
        """bool | EnvVar | None should classify as BOOL_OR_NULL."""
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "type_checking_enabled")
        assert field.category == FieldCategory.BOOL_OR_NULL

    def test_int_or_null_with_envvar(self):
        """int | EnvVar | None should classify as INT_OR_NULL."""
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "max_parallel_epochs")
        assert field.category == FieldCategory.INT_OR_NULL

    def test_enum_with_envvar(self):
        """RunAllocationMethod | EnvVar should classify as ENUM."""
        schema = get_model_schema(NetConfig, "NetConfig")
        field = next(f for f in schema.fields if f.name == "default_pool_allocation_method")
        assert field.category == FieldCategory.ENUM

    def test_float_with_envvar(self):
        """float | EnvVar should classify as FLOAT."""
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        field = next(f for f in schema.fields if f.name == "retry_wait")
        assert field.category == FieldCategory.FLOAT

    def test_env_var_supported_flag_true(self):
        """Fields with | EnvVar should have env_var_supported=True."""
        schema = get_model_schema(NetConfig, "NetConfig")
        # dead_letter_queue has | EnvVar in its annotation
        field = next(f for f in schema.fields if f.name == "dead_letter_queue")
        assert field.env_var_supported is True

    def test_env_var_supported_flag_false_for_complex(self):
        """Fields without | EnvVar should have env_var_supported=False."""
        schema = get_model_schema(NetConfig, "NetConfig")
        # graph is a complex type without EnvVar
        field = next(f for f in schema.fields if f.name == "graph")
        assert field.env_var_supported is False

    def test_env_var_supported_on_node_execution_config(self):
        """NodeExecutionConfig fields with EnvVar should be flagged."""
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        # max_epochs should have EnvVar support
        field = next(f for f in schema.fields if f.name == "max_epochs")
        assert field.env_var_supported is True


# ------------------------------------------------------------------
# Registration completeness tests
#
# These derive the expected fields directly from the Pydantic models,
# so adding a new field to a model will automatically fail until you
# register it in configFieldRegistrations.ts.
# ------------------------------------------------------------------

REGISTRATIONS_FILE = pathlib.Path(__file__).parent.parent / "src/lib/configFieldRegistrations.ts"


def _parse_registrations() -> dict[str, set[str]]:
    """Parse registerField() calls from TypeScript file."""
    text = REGISTRATIONS_FILE.read_text()
    pattern = r"registerField\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*,"
    result: dict[str, set[str]] = {}
    for model, field in re.findall(pattern, text):
        result.setdefault(model, set()).add(field)
    return result


class TestRegistrationCompleteness:
    """Ensure every field in every model has a registration in configFieldRegistrations.ts."""

    @pytest.mark.parametrize("model_name", MODEL_CLASSES.keys())
    def test_all_fields_registered(self, model_name: str):
        """Every field in the model must have a registerField() call."""
        registrations = _parse_registrations()
        model_cls = MODEL_CLASSES[model_name]
        actual_fields = set(model_cls.model_fields.keys())
        registered = registrations.get(model_name, set())
        missing = actual_fields - registered
        assert not missing, (
            f"{model_name} has unregistered field(s): {sorted(missing)}. "
            f"Register them in src/lib/configFieldRegistrations.ts"
        )

    @pytest.mark.parametrize("model_name", MODEL_CLASSES.keys())
    def test_no_stale_registrations(self, model_name: str):
        """No registrations for fields that no longer exist on the model."""
        registrations = _parse_registrations()
        model_cls = MODEL_CLASSES[model_name]
        actual_fields = set(model_cls.model_fields.keys())
        registered = registrations.get(model_name, set())
        stale = registered - actual_fields
        assert not stale, (
            f"{model_name} has stale registration(s): {sorted(stale)}. "
            f"Remove them from src/lib/configFieldRegistrations.ts"
        )
