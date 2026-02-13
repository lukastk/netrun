"""Tests for import_utils module (file-path-based function/factory references)."""

import sys
import textwrap
import pytest
from pathlib import Path

from netrun_ui_backend.import_utils import (
    is_file_path_ref,
    import_module_from_ref,
    reload_module,
    get_attr_from_ref,
)


class TestIsFilePathRef:
    """Test is_file_path_ref detection logic."""

    def test_dotted_import_paths_are_not_file_refs(self):
        assert is_file_path_ref("myapp.utils.my_function") is False
        assert is_file_path_ref("netrun.node_factories.from_function") is False
        assert is_file_path_ref("json") is False

    def test_double_colon_is_file_ref(self):
        assert is_file_path_ref("nodes.py::my_func") is True
        assert is_file_path_ref("./factory.py::get_node_config") is True
        assert is_file_path_ref("path/to/file.py::attr") is True

    def test_forward_slash_is_file_ref(self):
        assert is_file_path_ref("path/to/file.py") is True
        assert is_file_path_ref("./nodes.py") is True
        assert is_file_path_ref("../lib/factory.py") is True

    def test_backslash_is_file_ref(self):
        assert is_file_path_ref("path\\to\\file.py") is True

    def test_dot_prefix_is_file_ref(self):
        assert is_file_path_ref("./nodes.py") is True
        assert is_file_path_ref("../factory.py") is True
        assert is_file_path_ref(".hidden_module.py") is True

    def test_empty_and_simple_strings(self):
        assert is_file_path_ref("") is False
        assert is_file_path_ref("mymodule") is False


class TestImportModuleFromRef:
    """Test import_module_from_ref with both dotted paths and file-path references."""

    def test_dotted_import_path(self):
        """Standard dotted import should work unchanged."""
        module, attr_name = import_module_from_ref("json")
        assert module is not None
        assert attr_name is None
        assert hasattr(module, "dumps")

    def test_dotted_import_submodule(self):
        module, attr_name = import_module_from_ref("os.path")
        assert module is not None
        assert attr_name is None
        assert hasattr(module, "join")

    def test_file_path_ref_without_attr(self, tmp_path):
        """Import a .py file without :: attribute."""
        factory_file = tmp_path / "my_factory.py"
        factory_file.write_text(textwrap.dedent("""\
            VALUE = 42

            def get_node_config():
                return "config"
        """))

        module, attr_name = import_module_from_ref(str(factory_file))
        assert attr_name is None
        assert hasattr(module, "VALUE")
        assert module.VALUE == 42
        assert hasattr(module, "get_node_config")

    def test_file_path_ref_with_attr(self, tmp_path):
        """Import a .py file with :: to specify attribute."""
        factory_file = tmp_path / "my_funcs.py"
        factory_file.write_text(textwrap.dedent("""\
            def my_function():
                return "hello"

            def other_function():
                return "world"
        """))

        module, attr_name = import_module_from_ref(f"{factory_file}::my_function")
        assert attr_name == "my_function"
        assert hasattr(module, "my_function")

    def test_relative_path_with_base_dir(self, tmp_path):
        """Relative paths should resolve against base_dir."""
        factory_file = tmp_path / "nodes.py"
        factory_file.write_text("GREETING = 'hello'\n")

        module, attr_name = import_module_from_ref("./nodes.py", base_dir=str(tmp_path))
        assert attr_name is None
        assert module.GREETING == "hello"

    def test_relative_path_with_attr_and_base_dir(self, tmp_path):
        """Relative path with :: should resolve correctly."""
        factory_file = tmp_path / "nodes.py"
        factory_file.write_text("GREETING = 'hello'\n")

        module, attr_name = import_module_from_ref(
            "./nodes.py::GREETING", base_dir=str(tmp_path)
        )
        assert attr_name == "GREETING"
        assert module.GREETING == "hello"

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Module file not found"):
            import_module_from_ref(
                "./nonexistent.py", base_dir=str(tmp_path)
            )

    def test_invalid_dotted_import_raises_import_error(self):
        with pytest.raises(ModuleNotFoundError):
            import_module_from_ref("this_module_does_not_exist_xyz")

    def test_nested_relative_path(self, tmp_path):
        """Test importing from a subdirectory."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        mod_file = subdir / "helper.py"
        mod_file.write_text("X = 99\n")

        module, attr_name = import_module_from_ref(
            "./subdir/helper.py", base_dir=str(tmp_path)
        )
        assert module.X == 99

    def test_parent_relative_path(self, tmp_path):
        """Test importing with ../ path."""
        parent_dir = tmp_path / "parent"
        parent_dir.mkdir()
        child_dir = tmp_path / "parent" / "child"
        child_dir.mkdir()
        mod_file = parent_dir / "shared.py"
        mod_file.write_text("SHARED = True\n")

        module, attr_name = import_module_from_ref(
            "../shared.py", base_dir=str(child_dir)
        )
        assert module.SHARED is True


class TestReloadModule:
    """Test reload_module clears the cache."""

    def test_reload_file_path_ref(self, tmp_path):
        """After reload, re-import should pick up changes."""
        factory_file = tmp_path / "reloadable.py"
        factory_file.write_text("VERSION = 1\n")

        module1, _ = import_module_from_ref(str(factory_file))
        assert module1.VERSION == 1

        # Modify the file
        factory_file.write_text("VERSION = 2\n")

        # Reload clears cache
        reload_module(str(factory_file))

        module2, _ = import_module_from_ref(str(factory_file))
        assert module2.VERSION == 2

    def test_reload_relative_file_path(self, tmp_path):
        factory_file = tmp_path / "reloadable2.py"
        factory_file.write_text("VAL = 'a'\n")

        import_module_from_ref("./reloadable2.py", base_dir=str(tmp_path))

        factory_file.write_text("VAL = 'b'\n")
        reload_module("./reloadable2.py", base_dir=str(tmp_path))

        module, _ = import_module_from_ref("./reloadable2.py", base_dir=str(tmp_path))
        assert module.VAL == "b"

    def test_reload_dotted_path(self):
        """reload_module should clear dotted path modules from sys.modules."""
        import json
        # json is in sys.modules
        assert "json" in sys.modules

        reload_module("json")
        assert "json" not in sys.modules

        # Can reimport
        import json
        assert "json" in sys.modules

    def test_reload_with_double_colon(self, tmp_path):
        """Reload should work with :: syntax (strips the attr part)."""
        factory_file = tmp_path / "with_attr.py"
        factory_file.write_text("ATTR = 10\n")

        import_module_from_ref(f"{factory_file}::ATTR")

        factory_file.write_text("ATTR = 20\n")
        reload_module(f"{factory_file}::ATTR")

        module, _ = import_module_from_ref(str(factory_file))
        assert module.ATTR == 20


class TestGetAttrFromRef:
    """Test get_attr_from_ref convenience function."""

    def test_dotted_path_returns_module(self):
        result = get_attr_from_ref("json")
        import json
        assert result is json

    def test_file_path_with_attr(self, tmp_path):
        mod_file = tmp_path / "attrs.py"
        mod_file.write_text("MY_VAL = 'found'\n")

        result = get_attr_from_ref(f"{mod_file}::MY_VAL")
        assert result == "found"

    def test_file_path_with_default_attr(self, tmp_path):
        mod_file = tmp_path / "factory_mod.py"
        mod_file.write_text(textwrap.dedent("""\
            def get_node_config():
                return "the_config"
        """))

        result = get_attr_from_ref(
            str(mod_file), default_attr="get_node_config"
        )
        assert callable(result)
        assert result() == "the_config"

    def test_file_path_without_attr_returns_module(self, tmp_path):
        mod_file = tmp_path / "plain.py"
        mod_file.write_text("X = 1\n")

        result = get_attr_from_ref(str(mod_file))
        assert hasattr(result, "X")

    def test_missing_attr_raises(self, tmp_path):
        mod_file = tmp_path / "no_attr.py"
        mod_file.write_text("X = 1\n")

        with pytest.raises(AttributeError):
            get_attr_from_ref(f"{mod_file}::NONEXISTENT")


class TestFactoryEndpointWithFilePaths:
    """Test the factory API endpoints with file-path references."""

    @pytest.fixture
    def factory_dir(self, tmp_path):
        """Create a temporary directory with a factory module."""
        factory_file = tmp_path / "test_factory.py"
        factory_file.write_text(textwrap.dedent("""\
            from dataclasses import dataclass, field

            @dataclass
            class Port:
                port_type: str | None = None

            @dataclass
            class NodeConfig:
                name: str = "TestFactoryNode"
                in_ports: dict = field(default_factory=lambda: {"input": Port()})
                out_ports: dict = field(default_factory=lambda: {"output": Port()})
                in_salvo_conditions: dict = field(default_factory=dict)
                out_salvo_conditions: dict = field(default_factory=dict)

            def get_node_config(name: str = "TestFactoryNode") -> NodeConfig:
                \"\"\"Create a test node config.\"\"\"
                return NodeConfig(name=name)
        """))
        return tmp_path

    def test_validate_import_file_path(self, factory_dir):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        factory_path = str(factory_dir / "test_factory.py")
        response = client.post("/api/factories/validate-import", json={
            "import_path": factory_path,
        })

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["is_factory"] is True

    def test_validate_import_file_path_with_attr(self, factory_dir):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        factory_path = f"{factory_dir / 'test_factory.py'}::get_node_config"
        response = client.post("/api/factories/validate-import", json={
            "import_path": factory_path,
        })

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["is_factory"] is True

    def test_validate_import_missing_file(self, tmp_path):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        response = client.post("/api/factories/validate-import", json={
            "import_path": str(tmp_path / "nonexistent.py"),
        })

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is False
        assert "not found" in data["error"].lower() or "Module file not found" in data["error"]

    def test_signature_file_path(self, factory_dir):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        factory_path = str(factory_dir / "test_factory.py")
        response = client.post("/api/factories/signature", json={
            "factory_path": factory_path,
        })

        assert response.status_code == 200
        data = response.json()
        assert data["factory_path"] == factory_path
        assert len(data["parameters"]) == 1
        assert data["parameters"][0]["name"] == "name"
        assert data["parameters"][0]["has_default"] is True
        assert data["docstring"] is not None

    def test_preview_file_path(self, factory_dir):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        factory_path = str(factory_dir / "test_factory.py")
        response = client.post("/api/factories/preview", json={
            "factory_path": factory_path,
            "factory_args": {"name": "MyNode"},
        })

        assert response.status_code == 200
        data = response.json()
        assert data["error"] is None
        assert data["name"] == "MyNode"
        assert len(data["in_ports"]) == 1
        assert data["in_ports"][0]["name"] == "input"
        assert len(data["out_ports"]) == 1
        assert data["out_ports"][0]["name"] == "output"

    def test_preview_with_double_colon(self, factory_dir):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        factory_path = f"{factory_dir / 'test_factory.py'}::get_node_config"
        response = client.post("/api/factories/preview", json={
            "factory_path": factory_path,
            "factory_args": {},
        })

        assert response.status_code == 200
        data = response.json()
        assert data["error"] is None
        assert data["name"] == "TestFactoryNode"

    def test_relative_path_with_project_root(self, factory_dir):
        from fastapi.testclient import TestClient
        from netrun_ui_backend.main import app
        client = TestClient(app)

        response = client.post("/api/factories/validate-import", json={
            "import_path": "./test_factory.py",
            "project_root": str(factory_dir),
        })

        assert response.status_code == 200
        data = response.json()
        assert data["valid"] is True
        assert data["is_factory"] is True


class TestConverterWithFilePaths:
    """Test converter.py resolve_factory_ports with file-path refs."""

    def test_resolve_factory_ports_file_path(self, tmp_path):
        """Test that resolve_factory_ports works with file-path references."""
        factory_file = tmp_path / "converter_factory.py"
        factory_file.write_text(textwrap.dedent("""\
            from dataclasses import dataclass, field

            @dataclass
            class Port:
                port_type: str | None = None

            @dataclass
            class NodeConfig:
                name: str = "ConverterTestNode"
                in_ports: dict = field(default_factory=lambda: {"data_in": Port(port_type="str")})
                out_ports: dict = field(default_factory=lambda: {"data_out": Port(port_type="int")})
                in_salvo_conditions: dict = field(default_factory=dict)
                out_salvo_conditions: dict = field(default_factory=dict)

            def get_node_config():
                return NodeConfig()
        """))

        from netrun_ui_backend.converter import resolve_factory_ports

        result = resolve_factory_ports(
            str(factory_file),
            factory_args={},
            working_dir=str(tmp_path),
        )

        assert result is not None
        in_ports, out_ports, _description, _extra = result
        assert "data_in" in in_ports
        assert in_ports["data_in"]["port_type"] == "str"
        assert "data_out" in out_ports
        assert out_ports["data_out"]["port_type"] == "int"

    def test_resolve_factory_ports_file_path_with_attr(self, tmp_path):
        """Test resolve_factory_ports with :: syntax."""
        factory_file = tmp_path / "multi_factory.py"
        factory_file.write_text(textwrap.dedent("""\
            from dataclasses import dataclass, field

            @dataclass
            class Port:
                port_type: str | None = None

            @dataclass
            class NodeConfig:
                name: str
                in_ports: dict = field(default_factory=dict)
                out_ports: dict = field(default_factory=dict)
                in_salvo_conditions: dict = field(default_factory=dict)
                out_salvo_conditions: dict = field(default_factory=dict)

            def make_source():
                return NodeConfig(name="Source", out_ports={"out": Port()})

            def make_sink():
                return NodeConfig(name="Sink", in_ports={"in": Port()})
        """))

        from netrun_ui_backend.converter import resolve_factory_ports

        result = resolve_factory_ports(
            f"{factory_file}::make_source",
            factory_args={},
            working_dir=str(tmp_path),
        )

        assert result is not None
        in_ports, out_ports, _description, _extra = result
        assert len(in_ports) == 0
        assert "out" in out_ports
