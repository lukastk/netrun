"""Tests for config schema introspection.

These tests ensure that:
1. The schema endpoint returns correct field classifications
2. Every model field is accounted for in KNOWN_FIELDS (catches new/removed fields)
3. Every field has a registration in configFieldRegistrations.ts
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
# Known fields per model.
#
# When you add or remove a field from a model, this test will fail.
# Update the set below AND register the field in
#   src/lib/configFieldRegistrations.ts
# ------------------------------------------------------------------
KNOWN_FIELDS: dict[str, set[str]] = {
    "NetConfig": {
        "project_root",
        "pools",
        "graph",
        "extra",
        "default_pool_allocation_method",
        "node_vars",
        "dead_letter_queue",
        "dead_letter_path",
        "dead_letter_callback",
        "output_queues",
        "error_on_undeclared_output",
        "type_checking_enabled",
        "propagate_exceptions",
        "print_exceptions",
        "storage",
    },
    "NodeConfig": {
        "type",
        "name",
        "in_ports",
        "out_ports",
        "in_salvo_conditions",
        "out_salvo_conditions",
        "execution_config",
        "extra",
        "factory",
        "factory_args",
    },
    "NodeExecutionConfig": {
        "pools",
        "exec_node_func",
        "start_node_func",
        "stop_node_func",
        "on_node_failure",
        "defer_startup",
        "max_parallel_epochs",
        "max_epochs",
        "rate_limit_per_second",
        "defer_net_actions",
        "retries",
        "retry_wait",
        "timeout",
        "capture_prints",
        "print_flush_interval",
        "print_buffer_max_size",
        "print_echo_stdout",
        "pool_allocation_method",
        "node_vars",
        "type_checking_enabled",
        "propagate_exceptions",
        "print_exceptions",
        "storage",
        "run_on_startup",
    },
    # --- Sub-models ---
    "StorageConfig": {"backends", "cache", "file_storage_metadata_path"},
    "CacheConfig": {
        "enabled", "version", "storage_path", "include_nodes", "exclude_nodes",
        "include_all_nodes", "cache_what", "hash_method", "pickling_method",
        "pickling_args", "evaluate_lazy_value_for_cache", "sample_size",
    },
    "NodeStorageConfig": {"cache", "file_storage"},
    "NodeCacheConfig": {
        "enabled", "version", "cache_what", "hash_method", "pickling_method",
        "pickling_args", "evaluate_lazy_value_for_cache", "sample_size",
    },
    "NodeFileStorageConfig": {
        "enabled", "backend", "serialization", "pickling_method",
        "pickling_args", "compression", "on_hash_change", "store_inputs",
        "hash_method", "hash_pickling_method", "hash_pickling_args",
        "evaluate_lazy_value_for_hash", "bundle", "bundle_format",
        "output_names", "version",
    },
    "LocalBackendConfig": {"type", "base_path"},
    "S3BackendConfig": {"type", "bucket", "prefix", "region", "endpoint_url", "access_key", "secret_key"},
    "GCSBackendConfig": {"type", "bucket", "prefix", "credentials_path"},
    "SSHBackendConfig": {"type", "host", "base_path", "port", "username", "key_path", "password"},
    "RcloneBackendConfig": {"type", "remote", "config_path"},
    "OutputQueueConfig": {"ports"},
}

MODEL_CLASSES = {
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


class TestKnownFields:
    """Ensure KNOWN_FIELDS stays in sync with actual model definitions."""

    @pytest.mark.parametrize("model_name", KNOWN_FIELDS.keys())
    def test_no_new_fields(self, model_name: str):
        """Fail if a model has fields not listed in KNOWN_FIELDS."""
        model_cls = MODEL_CLASSES[model_name]
        actual = set(model_cls.model_fields.keys())
        unknown = actual - KNOWN_FIELDS[model_name]
        assert not unknown, (
            f"{model_name} has new field(s) not in KNOWN_FIELDS: {unknown}. "
            f"Add them to KNOWN_FIELDS in tests/test_config_schema.py and "
            f"register them in src/lib/configFieldRegistrations.ts"
        )

    @pytest.mark.parametrize("model_name", KNOWN_FIELDS.keys())
    def test_no_stale_fields(self, model_name: str):
        """Fail if KNOWN_FIELDS lists fields that no longer exist."""
        model_cls = MODEL_CLASSES[model_name]
        actual = set(model_cls.model_fields.keys())
        stale = KNOWN_FIELDS[model_name] - actual
        assert not stale, (
            f"KNOWN_FIELDS[{model_name!r}] lists field(s) that no longer exist: {stale}. "
            f"Remove them from KNOWN_FIELDS and from configFieldRegistrations.ts"
        )


class TestSchemaClassification:
    """Test that get_model_schema correctly classifies field types."""

    def test_net_config_field_count(self):
        schema = get_model_schema(NetConfig, "NetConfig")
        assert len(schema.fields) == len(KNOWN_FIELDS["NetConfig"])

    def test_node_execution_config_field_count(self):
        schema = get_model_schema(NodeExecutionConfig, "NodeExecutionConfig")
        assert len(schema.fields) == len(KNOWN_FIELDS["NodeExecutionConfig"])

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
        field = next(f for f in schema.fields if f.name == "project_root")
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
    """Ensure every field in every known model has a registration in configFieldRegistrations.ts."""

    def test_all_fields_registered(self):
        """Every field in every known model must have a registration."""
        registrations = _parse_registrations()
        missing = []
        for model, fields in KNOWN_FIELDS.items():
            registered = registrations.get(model, set())
            for field in sorted(fields):
                if field not in registered:
                    missing.append(f"{model}.{field}")
        assert not missing, f"Unregistered fields: {missing}"

    def test_no_stale_registrations(self):
        """No registrations for fields that don't exist in any model."""
        registrations = _parse_registrations()
        stale = []
        for model, fields in registrations.items():
            known = KNOWN_FIELDS.get(model)
            if known is None:
                stale.extend(f"{model}.{f}" for f in sorted(fields))
            else:
                for f in sorted(fields):
                    if f not in known:
                        stale.append(f"{model}.{f}")
        assert not stale, f"Stale registrations: {stale}"
