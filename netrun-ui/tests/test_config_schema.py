"""Tests for config schema introspection.

These tests ensure that:
1. The schema endpoint returns correct field classifications
2. Every model field is accounted for in KNOWN_FIELDS (catches new/removed fields)
"""
import pytest

from netrun.net.config import NetConfig, NodeConfig, NodeExecutionConfig
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
    },
}

MODEL_CLASSES = {
    "NetConfig": NetConfig,
    "NodeConfig": NodeConfig,
    "NodeExecutionConfig": NodeExecutionConfig,
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
