# Tests for NodeConfig factory support
import pytest
from netrun.net.config import (
    NodeConfig,
    NodeExecutionConfig,
    PortConfig,
    SalvoConditionConfig,
    MaxSalvosFiniteConfig,
    PacketCountAllConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermTrueConfig,
    PortStateNonEmptyConfig,
)


# Test factory module is at tests.net.sample_factory
FACTORY_MODULE_PATH = "tests.net.sample_factory"


class TestFromFactoryWithImportPath:
    """Tests for NodeConfig.from_factory() using import path strings."""

    def test_from_factory_basic(self):
        """Test from_factory creates config with execution functions."""
        config = NodeConfig.from_factory(
            factory=FACTORY_MODULE_PATH,
            args={"name": "TestNode", "threshold": 0.7},
        )

        assert config.name == "TestNode"
        assert "task" in config.in_ports
        assert "result" in config.out_ports
        assert "trigger" in config.in_salvo_conditions
        assert "send" in config.out_salvo_conditions
        assert config.execution_config is not None
        assert config.execution_config.exec_node_func is not None

    def test_from_factory_with_default_args(self):
        """Test from_factory with factory default arguments."""
        config = NodeConfig.from_factory(
            factory=FACTORY_MODULE_PATH,
            args={"name": "DefaultNode"},
        )

        assert config.name == "DefaultNode"
        assert config.execution_config is not None


class TestFromFactoryWithModule:
    """Tests for NodeConfig.from_factory() using module objects."""

    def test_from_factory_with_module_object(self):
        """Test from_factory works with imported module object."""
        import tests.net.sample_factory as factory_module

        config = NodeConfig.from_factory(
            factory=factory_module,
            args={"name": "ModuleNode", "threshold": 0.3},
        )

        assert config.name == "ModuleNode"
        assert config.execution_config is not None


class TestFactoryFieldExpansion:
    """Tests for factory expansion via resolve() method.

    With deferred config resolution, factory fields are NOT expanded at creation
    time. The factory and factory_args are preserved until resolve() is called.
    """

    def test_factory_field_deferred(self):
        """Test that factory fields are NOT expanded at creation time."""
        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "FieldNode", "threshold": 0.8},
        )

        # Before resolve(): config stays raw
        assert config.name == ""  # Not expanded yet
        assert config.in_ports == {}  # Not expanded yet
        assert config.execution_config is None  # Not expanded yet
        # Factory and factory_args are preserved
        assert config.factory == FACTORY_MODULE_PATH
        assert config.factory_args == {"name": "FieldNode", "threshold": 0.8}

    def test_factory_field_expands_on_resolve(self):
        """Test that resolve() expands the factory field."""
        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "FieldNode", "threshold": 0.8},
        )

        # Call resolve() to expand
        resolved = config.resolve()

        assert resolved.name == "FieldNode"
        assert "task" in resolved.in_ports
        assert resolved.execution_config is not None
        # Factory is cleared in resolved config
        assert resolved.factory is None

    def test_factory_field_with_overrides(self):
        """Test that explicit fields override factory-generated values."""
        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "OverrideNode", "threshold": 0.5},
            # Override the name
            name="CustomName",
            # Add an extra port
            out_ports={"extra_out": PortConfig()},
        )

        # Call resolve() to expand
        resolved = config.resolve()

        # Name is overridden
        assert resolved.name == "CustomName"
        # Factory-generated ports are merged
        assert "task" in resolved.in_ports  # from factory
        assert "result" in resolved.out_ports  # from factory
        assert "extra_out" in resolved.out_ports  # from override

    def test_factory_field_merge_salvo_conditions(self):
        """Test that salvo conditions from factory and overrides are merged."""
        extra_condition = SalvoConditionConfig(
            max_salvos=MaxSalvosFiniteConfig(max=1),
            ports={"task": PacketCountAllConfig()},
            term=SalvoConditionTermTrueConfig(),
        )

        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "MergeNode"},
            in_salvo_conditions={"extra_trigger": extra_condition},
        )

        # Call resolve() to expand
        resolved = config.resolve()

        # Both factory and override conditions present
        assert "trigger" in resolved.in_salvo_conditions  # from factory
        assert "extra_trigger" in resolved.in_salvo_conditions  # from override


class TestFactorySerialization:
    """Tests for JSON serialization of factory configs.

    With deferred resolution, configs with factory fields can be serialized
    directly because execution_config is not populated until resolve() is called.
    """

    def test_factory_serializes_to_string(self):
        """Test that module objects serialize to import path strings."""
        import tests.net.sample_factory as factory_module

        config = NodeConfig(
            factory=factory_module,
            factory_args={"name": "SerializeNode"},
        )

        # With deferred resolution, execution_config is None so no need to remove it
        assert config.execution_config is None

        # Serialize to JSON
        json_str = config.model_dump_json()

        # Deserialize and check factory is string
        loaded = NodeConfig.model_validate_json(json_str)
        assert isinstance(loaded.factory, str)
        assert loaded.factory == "tests.net.sample_factory"

    def test_factory_config_roundtrip(self):
        """Test that factory configs roundtrip through JSON.

        With deferred resolution, the factory config stays raw and serializable
        until resolve() is called.
        """
        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "RoundtripNode", "threshold": 0.6},
        )

        # With deferred resolution, no need to remove execution_config
        assert config.execution_config is None

        json_str = config.model_dump_json()
        loaded = NodeConfig.model_validate_json(json_str)

        assert loaded.name == config.name
        assert loaded.factory == config.factory
        assert loaded.factory_args == config.factory_args
        # Before resolve(), ports are not populated
        assert loaded.in_ports == {}

        # After resolve(), ports are populated
        resolved = loaded.resolve()
        assert "task" in resolved.in_ports

    def test_resolved_closure_functions_fail_to_serialize(self):
        """Test that resolved configs with closure functions fail to serialize.

        When factories return closures (like the sample factory does),
        the resolved config cannot be serialized to JSON. For serializable
        configs, factories should return string import paths for functions.
        """
        from pydantic import ValidationError

        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "ClosureNode"},
        )

        # Before resolve: can serialize because execution_config is None
        json_str = config.model_dump_json()  # Should succeed

        # After resolve: has closure functions
        resolved = config.resolve()
        assert resolved.execution_config is not None
        assert resolved.execution_config.exec_node_func is not None

        # Attempting to serialize resolved config should fail
        with pytest.raises(Exception) as exc_info:
            resolved.model_dump_json()

        assert "closure" in str(exc_info.value).lower() or "local function" in str(exc_info.value).lower()


class TestFactoryErrors:
    """Tests for error handling in factory usage."""

    def test_invalid_import_path_raises(self):
        """Test that invalid import path raises ImportError."""
        with pytest.raises(ImportError):
            NodeConfig.from_factory(
                factory="nonexistent.module.path",
                args={"name": "Test"},
            )

    def test_missing_get_node_config_raises(self):
        """Test that module without get_node_config raises AttributeError."""
        # Use a module that exists but doesn't have the factory functions
        with pytest.raises(AttributeError):
            NodeConfig.from_factory(
                factory="os",  # os module doesn't have factory functions
                args={"name": "Test"},
            )


class TestFactoryWithNetrunSim:
    """Tests that factory-created configs work with netrun_sim."""

    def test_factory_config_to_netrun_sim(self):
        """Test that factory configs convert to netrun_sim nodes."""
        config = NodeConfig.from_factory(
            factory=FACTORY_MODULE_PATH,
            args={"name": "SimNode", "threshold": 0.5},
        )

        # Should convert without error
        node = config.to_netrun_sim()
        assert node.name == "SimNode"
        assert "task" in node.in_ports
        assert "result" in node.out_ports
