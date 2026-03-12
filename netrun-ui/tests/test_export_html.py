"""Tests for the export-html feature (module, CLI, and API endpoint)."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from netrun_ui_backend.main import app
from netrun_ui_backend.export_html import build_html, find_vis_assets_dir

client = TestClient(app)

TESTING_DIR = Path(__file__).parent.parent / "testing"
SAMPLE_CONFIG = {
    "graph": {
        "nodes": [
            {
                "name": "Source",
                "in_ports": [{"name": "in", "type": "any"}],
                "out_ports": [{"name": "out", "type": "any"}],
                "type": "node",
            }
        ],
        "edges": [],
    }
}


class TestFindVisAssetsDir:
    """Test asset directory resolution."""

    def test_finds_assets(self):
        """Assets dir should resolve via node_modules symlink."""
        assets_dir = find_vis_assets_dir()
        assert assets_dir.is_dir()
        # Should contain at least a JS file
        js_files = list(assets_dir.glob("*.js"))
        assert len(js_files) >= 1


class TestBuildHtml:
    """Test the core HTML assembly function."""

    @pytest.fixture
    def assets_dir(self):
        return find_vis_assets_dir()

    def test_produces_valid_html(self, assets_dir):
        """Output should be a complete HTML document."""
        html = build_html(SAMPLE_CONFIG, assets_dir)
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_embeds_config(self, assets_dir):
        """Config JSON should be embedded as window.__NETRUN_CONFIG__."""
        html = build_html(SAMPLE_CONFIG, assets_dir)
        assert "window.__NETRUN_CONFIG__" in html
        assert '"Source"' in html

    def test_escapes_script_close_tag(self, assets_dir):
        """The </script> sequence must be escaped to prevent HTML parser breakout."""
        config = {
            "graph": {"nodes": [], "edges": []},
            "extra": {"description": "</script><script>alert(1)</script>"},
        }
        html = build_html(config, assets_dir)
        # Raw </script> must not appear in the config JSON block
        # (the escaped version <\/script> is fine)
        config_line = [l for l in html.split("\n") if "__NETRUN_CONFIG__" in l][0]
        assert "</script>" not in config_line.split("__NETRUN_CONFIG__")[1].split("</script>")[0]
        assert "<\\/" in html  # escaped form present

    def test_title_uses_description(self, assets_dir):
        """Title should come from extra.description."""
        config = {"graph": {"nodes": [], "edges": []}, "extra": {"description": "My Flow"}}
        html = build_html(config, assets_dir)
        assert "<title>Netrun: My Flow</title>" in html

    def test_title_escapes_html(self, assets_dir):
        """Title should HTML-escape special characters."""
        config = {"graph": {"nodes": [], "edges": []}, "extra": {"description": '<b>"test"</b>'}}
        html = build_html(config, assets_dir)
        assert "&lt;b&gt;" in html
        assert "&quot;test&quot;" in html

    def test_title_default(self, assets_dir):
        """Missing description should fall back to 'Netrun Graph'."""
        config = {"graph": {"nodes": [], "edges": []}}
        html = build_html(config, assets_dir)
        assert "<title>Netrun: Netrun Graph</title>" in html

    def test_inlines_css(self, assets_dir):
        """CSS bundle should be inlined in a <style> tag."""
        html = build_html(SAMPLE_CONFIG, assets_dir)
        # The built CSS should be present (it's ~45KB)
        assert html.count("<style>") >= 2  # base styles + inlined CSS bundle

    def test_inlines_js(self, assets_dir):
        """JS bundle should be inlined in a <script type="module"> tag."""
        html = build_html(SAMPLE_CONFIG, assets_dir)
        assert '<script type="module">' in html

    def test_missing_js_raises(self, tmp_path):
        """Should raise if no JS bundle is found in the assets dir."""
        with pytest.raises(FileNotFoundError, match="No JS bundle"):
            build_html(SAMPLE_CONFIG, tmp_path)


class TestExportHtmlFromFile:
    """Test the file-based export wrapper."""

    def test_writes_output_file(self, tmp_path):
        from netrun_ui_backend.export_html import export_html_from_file

        input_file = tmp_path / "test.netrun.json"
        input_file.write_text(json.dumps(SAMPLE_CONFIG))
        output_file = tmp_path / "test.html"

        export_html_from_file(input_file, output_file)

        assert output_file.exists()
        content = output_file.read_text()
        assert "<!DOCTYPE html>" in content
        assert "Source" in content

    def test_creates_parent_dirs(self, tmp_path):
        from netrun_ui_backend.export_html import export_html_from_file

        input_file = tmp_path / "test.netrun.json"
        input_file.write_text(json.dumps(SAMPLE_CONFIG))
        output_file = tmp_path / "sub" / "dir" / "test.html"

        export_html_from_file(input_file, output_file)
        assert output_file.exists()

    def test_missing_input_raises(self, tmp_path):
        from netrun_ui_backend.export_html import export_html_from_file

        with pytest.raises(FileNotFoundError, match="Input file not found"):
            export_html_from_file(tmp_path / "nope.json", tmp_path / "out.html")


class TestExportHtmlCli:
    """Test the CLI subcommand."""

    def _run_cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "netrun_ui_backend.cli", "export-html", *args],
            capture_output=True,
            text=True,
        )

    def test_export_from_sample_file(self, tmp_path):
        """CLI should produce an HTML file from a real sample config."""
        input_file = TESTING_DIR / "broadcast.netrun.json"
        output_file = tmp_path / "broadcast.html"
        result = self._run_cli(str(input_file), "-o", str(output_file))
        assert result.returncode == 0, result.stderr
        assert output_file.exists()
        content = output_file.read_text()
        assert "<!DOCTYPE html>" in content

    def test_default_output_path(self, tmp_path):
        """Without -o, output should be <input>.html."""
        input_file = tmp_path / "my.netrun.json"
        input_file.write_text(json.dumps(SAMPLE_CONFIG))
        result = self._run_cli(str(input_file))
        assert result.returncode == 0, result.stderr
        expected = tmp_path / "my.netrun.html"
        assert expected.exists()

    def test_missing_file_error(self):
        result = self._run_cli("/nonexistent/file.netrun.json")
        assert result.returncode != 0
        assert "not found" in result.stderr.lower() or "error" in result.stderr.lower()

    def test_help(self):
        result = self._run_cli("--help")
        assert result.returncode == 0
        assert "export-html" in result.stdout.lower() or "export" in result.stdout.lower()


class TestExportHtmlEndpoint:
    """Test the POST /api/files/export-html endpoint."""

    def test_export_success(self, tmp_path):
        output_path = tmp_path / "export.html"
        response = client.post("/api/files/export-html", json={
            "nodes": [
                {
                    "id": "node-1",
                    "type": "netrunNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "MyNode",
                        "nodeType": "regular",
                        "inPorts": [{"name": "in"}],
                        "outPorts": [{"name": "out"}],
                    },
                }
            ],
            "edges": [],
            "extra": {"description": "Test Export"},
            "extra_data": {},
            "output_path": str(output_path),
        })
        assert response.status_code == 200, response.json()
        data = response.json()
        assert data["success"] is True
        assert output_path.exists()
        content = output_path.read_text()
        assert "<title>Netrun: Test Export</title>" in content

    def test_export_with_edges(self, tmp_path):
        output_path = tmp_path / "edges.html"
        response = client.post("/api/files/export-html", json={
            "nodes": [
                {
                    "id": "a",
                    "type": "netrunNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "A",
                        "nodeType": "regular",
                        "inPorts": [],
                        "outPorts": [{"name": "out"}],
                    },
                },
                {
                    "id": "b",
                    "type": "netrunNode",
                    "position": {"x": 200, "y": 0},
                    "data": {
                        "label": "B",
                        "nodeType": "regular",
                        "inPorts": [{"name": "in"}],
                        "outPorts": [],
                    },
                },
            ],
            "edges": [
                {
                    "id": "e1",
                    "source": "a",
                    "target": "b",
                    "sourceHandle": "out",
                    "targetHandle": "in",
                },
            ],
            "output_path": str(output_path),
        })
        assert response.status_code == 200
        content = output_path.read_text()
        assert "<!DOCTYPE html>" in content
