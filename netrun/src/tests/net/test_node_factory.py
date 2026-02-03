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
    """Tests for automatic factory expansion via factory field."""

    def test_factory_field_expands(self):
        """Test that setting factory field auto-expands the config."""
        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "FieldNode", "threshold": 0.8},
        )

        assert config.name == "FieldNode"
        assert "task" in config.in_ports
        assert config.execution_config is not None
        # Factory and factory_args are preserved
        assert config.factory == FACTORY_MODULE_PATH
        assert config.factory_args == {"name": "FieldNode", "threshold": 0.8}

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

        # Name is overridden
        assert config.name == "CustomName"
        # Factory-generated ports are merged
        assert "task" in config.in_ports  # from factory
        assert "result" in config.out_ports  # from factory
        assert "extra_out" in config.out_ports  # from override

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

        # Both factory and override conditions present
        assert "trigger" in config.in_salvo_conditions  # from factory
        assert "extra_trigger" in config.in_salvo_conditions  # from override


class TestFactorySerialization:
    """Tests for JSON serialization of factory configs."""

    def test_factory_serializes_to_string(self):
        """Test that module objects serialize to import path strings.

        Note: To serialize successfully, we remove execution_config which
        contains closures from the sample factory. Closures can't be serialized
        to JSON - use string import paths for serializable configs.
        """
        import tests.net.sample_factory as factory_module

        config = NodeConfig(
            factory=factory_module,
            factory_args={"name": "SerializeNode"},
        )

        # Remove execution_config (contains closures) to allow serialization
        config = config.model_copy(update={"execution_config": None})

        # Serialize to JSON
        json_str = config.model_dump_json()

        # Deserialize and check factory is string
        loaded = NodeConfig.model_validate_json(json_str)
        assert isinstance(loaded.factory, str)
        assert loaded.factory == "tests.net.sample_factory"

    def test_factory_config_roundtrip(self):
        """Test that factory configs roundtrip through JSON.

        Note: execution_config is removed for serialization because the sample
        factory returns closures. For production use, factories should return
        string import paths for functions to enable JSON serialization.
        """
        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "RoundtripNode", "threshold": 0.6},
        )

        # Remove execution_config (contains closures) to allow serialization
        config = config.model_copy(update={"execution_config": None})

        json_str = config.model_dump_json()
        loaded = NodeConfig.model_validate_json(json_str)

        assert loaded.name == config.name
        assert loaded.factory == config.factory
        assert loaded.factory_args == config.factory_args
        assert "task" in loaded.in_ports

    def test_closure_functions_fail_to_serialize(self):
        """Test that configs with closure functions fail to serialize to JSON.

        This is expected behavior - closures can't be converted to import paths.
        Use string import paths for functions that need to be serialized.
        """
        from pydantic import ValidationError

        config = NodeConfig(
            factory=FACTORY_MODULE_PATH,
            factory_args={"name": "ClosureNode"},
        )

        # Verify execution_config has a closure
        assert config.execution_config is not None
        assert config.execution_config.exec_node_func is not None

        # Attempting to serialize to JSON should fail
        with pytest.raises(Exception) as exc_info:
            config.model_dump_json()

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
