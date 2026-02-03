# Tests for port type validation
import pytest
from dataclasses import dataclass
from netrun.net.config import (
    PortConfig,
    PortTypeConfig,
    PortSlotSpecInfiniteConfig,
    NodeExecutionConfig,
)
from netrun.net._net import (
    NodeExecutionContext,
    PacketTypeMismatch,
    DeferredActionQueue,
)
from netrun.storage import LazyPacketValueSpec


class TestPortTypeConfig:
    """Tests for PortTypeConfig model."""

    def test_port_type_config_basic(self):
        """Test basic PortTypeConfig creation."""
        config = PortTypeConfig(name="DataFrame")
        assert config.name == "DataFrame"
        assert config.isinstance_check is False  # Default

    def test_port_type_config_isinstance(self):
        """Test PortTypeConfig with isinstance_check."""
        config = PortTypeConfig(name="MyClass", isinstance_check=True)
        assert config.name == "MyClass"
        assert config.isinstance_check is True


class TestPortConfigWithPortType:
    """Tests for PortConfig with port_type field."""

    def test_port_config_no_type(self):
        """Test PortConfig without port_type (default)."""
        config = PortConfig()
        assert config.port_type is None

    def test_port_config_string_type(self):
        """Test PortConfig with string port_type."""
        config = PortConfig(port_type="DataFrame")
        assert config.port_type == "DataFrame"

    def test_port_config_type_object(self):
        """Test PortConfig with type object port_type."""
        config = PortConfig(port_type=dict)
        assert config.port_type is dict

    def test_port_config_type_config(self):
        """Test PortConfig with PortTypeConfig port_type."""
        type_config = PortTypeConfig(name="list", isinstance_check=False)
        config = PortConfig(port_type=type_config)
        assert config.port_type == type_config


class TestPortConfigSerialization:
    """Tests for PortConfig.port_type serialization."""

    def test_serialize_none(self):
        """Test serialization of None port_type."""
        config = PortConfig()
        data = config.model_dump()
        assert data["port_type"] is None

    def test_serialize_string(self):
        """Test serialization of string port_type."""
        config = PortConfig(port_type="DataFrame")
        data = config.model_dump()
        assert data["port_type"] == "DataFrame"

    def test_serialize_type_object(self):
        """Test serialization of type object to string."""
        config = PortConfig(port_type=dict)
        data = config.model_dump()
        assert data["port_type"] == "dict"

    def test_serialize_type_config(self):
        """Test serialization of PortTypeConfig to dict."""
        type_config = PortTypeConfig(name="list", isinstance_check=True)
        config = PortConfig(port_type=type_config)
        data = config.model_dump()
        assert data["port_type"] == {"name": "list", "isinstance_check": True}

    def test_roundtrip_string(self):
        """Test JSON roundtrip with string port_type."""
        config = PortConfig(port_type="DataFrame")
        json_str = config.model_dump_json()
        loaded = PortConfig.model_validate_json(json_str)
        assert loaded.port_type == "DataFrame"


class TestPacketTypeMismatch:
    """Tests for PacketTypeMismatch exception."""

    def test_exception_attributes(self):
        """Test PacketTypeMismatch has correct attributes."""
        exc = PacketTypeMismatch(
            port_name="out",
            expected="DataFrame",
            actual="str",
            packet_id="packet123",
            node_name="Process",
        )
        assert exc.port_name == "out"
        assert exc.expected == "DataFrame"
        assert exc.actual == "str"
        assert exc.packet_id == "packet123"
        assert exc.node_name == "Process"

    def test_exception_message(self):
        """Test PacketTypeMismatch has descriptive message."""
        exc = PacketTypeMismatch(
            port_name="out",
            expected="DataFrame",
            actual="str",
            packet_id="packet123",
            node_name="Process",
        )
        assert "Process.out" in str(exc)
        assert "DataFrame" in str(exc)
        assert "str" in str(exc)
        assert "packet123" in str(exc)


class TestNodeExecutionContextTypeValidation:
    """Tests for type validation in NodeExecutionContext."""

    def create_context(self, out_ports=None, input_packet_values=None):
        """Helper to create a context for testing."""
        return NodeExecutionContext(
            epoch_id="epoch123",
            node_name="TestNode",
            _out_ports=out_ports or {},
            _input_packet_values=input_packet_values or {},
        )

    def test_load_output_port_no_type(self):
        """Test load_output_port with no type validation."""
        ctx = self.create_context(
            out_ports={"out": PortConfig()},  # No port_type
        )
        # Create a packet
        packet_id = ctx.create_packet("any value")
        # Should not raise
        ctx.load_output_port("out", packet_id)

    def test_load_output_port_string_type_match(self):
        """Test load_output_port with matching string type."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type="str")},
        )
        packet_id = ctx.create_packet("hello")
        ctx.load_output_port("out", packet_id)  # Should pass

    def test_load_output_port_string_type_mismatch(self):
        """Test load_output_port raises on string type mismatch."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type="dict")},
        )
        packet_id = ctx.create_packet("not a dict")

        with pytest.raises(PacketTypeMismatch) as exc_info:
            ctx.load_output_port("out", packet_id)

        assert exc_info.value.expected == "dict"
        assert exc_info.value.actual == "str"
        assert exc_info.value.port_name == "out"
        assert exc_info.value.node_name == "TestNode"

    def test_load_output_port_type_object_match(self):
        """Test load_output_port with matching type object (isinstance)."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type=dict)},
        )
        packet_id = ctx.create_packet({"key": "value"})
        ctx.load_output_port("out", packet_id)  # Should pass

    def test_load_output_port_type_object_subclass(self):
        """Test load_output_port accepts subclass with type object."""

        @dataclass
        class MyDict(dict):
            pass

        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type=dict)},
        )
        packet_id = ctx.create_packet(MyDict())
        ctx.load_output_port("out", packet_id)  # Should pass (isinstance)

    def test_load_output_port_type_object_mismatch(self):
        """Test load_output_port raises on type object mismatch."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type=list)},
        )
        packet_id = ctx.create_packet({"not": "a list"})

        with pytest.raises(PacketTypeMismatch) as exc_info:
            ctx.load_output_port("out", packet_id)

        assert exc_info.value.expected == "list"
        assert exc_info.value.actual == "dict"

    def test_load_output_port_type_config_match(self):
        """Test load_output_port with matching PortTypeConfig."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type=PortTypeConfig(name="list"))},
        )
        packet_id = ctx.create_packet([1, 2, 3])
        ctx.load_output_port("out", packet_id)  # Should pass

    def test_load_output_port_type_config_mismatch(self):
        """Test load_output_port raises on PortTypeConfig mismatch."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type=PortTypeConfig(name="tuple"))},
        )
        packet_id = ctx.create_packet([1, 2, 3])

        with pytest.raises(PacketTypeMismatch) as exc_info:
            ctx.load_output_port("out", packet_id)

        assert exc_info.value.expected == "tuple"
        assert exc_info.value.actual == "list"

    def test_load_output_port_input_packet(self):
        """Test type validation works for input packets loaded to output."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type="int")},
            input_packet_values={"input_packet": 42},
        )
        ctx.load_output_port("out", "input_packet")  # Should pass

    def test_load_output_port_input_packet_mismatch(self):
        """Test type validation catches mismatched input packets."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type="int")},
            input_packet_values={"input_packet": "not an int"},
        )

        with pytest.raises(PacketTypeMismatch):
            ctx.load_output_port("out", "input_packet")

    def test_load_output_port_lazy_value_skipped(self):
        """Test type validation is skipped for lazy values."""
        ctx = self.create_context(
            out_ports={"out": PortConfig(port_type="int")},
        )
        # Create a lazy packet (type can't be checked until materialized)
        packet_id = ctx.create_packet_from_value_func(
            "os.getpid",  # Returns int at runtime
        )
        # Should not raise (skipped for lazy values)
        ctx.load_output_port("out", packet_id)

    def test_load_output_port_unknown_port(self):
        """Test load_output_port with unknown port (no validation)."""
        ctx = self.create_context(
            out_ports={"known": PortConfig(port_type="int")},
        )
        packet_id = ctx.create_packet("anything")
        # Unknown port - no validation, deferred action is queued
        ctx.load_output_port("unknown", packet_id)


class TestTypeCheckMethod:
    """Tests for _check_type helper method."""

    def create_context(self):
        return NodeExecutionContext(
            epoch_id="epoch123",
            node_name="TestNode",
        )

    def test_check_type_string_match(self):
        """Test _check_type with matching string."""
        ctx = self.create_context()
        expected, matches = ctx._check_type("str", "hello")
        assert expected == "str"
        assert matches is True

    def test_check_type_string_no_match(self):
        """Test _check_type with non-matching string."""
        ctx = self.create_context()
        expected, matches = ctx._check_type("int", "hello")
        assert expected == "int"
        assert matches is False

    def test_check_type_type_isinstance(self):
        """Test _check_type with type uses isinstance."""
        ctx = self.create_context()
        expected, matches = ctx._check_type(dict, {"a": 1})
        assert expected == "dict"
        assert matches is True

    def test_check_type_type_config(self):
        """Test _check_type with PortTypeConfig."""
        ctx = self.create_context()
        config = PortTypeConfig(name="list")
        expected, matches = ctx._check_type(config, [1, 2])
        assert expected == "list"
        assert matches is True
