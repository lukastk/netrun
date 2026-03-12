"""Build a standalone HTML file from a netrun config.

Shared by the CLI ``export-html`` subcommand and the ``/api/files/export-html``
endpoint. The heavy lifting is done by :func:`build_html`, which inlines the
pre-built JS/CSS from ``netrun-ui-vis/dist/app/assets/`` into a single HTML
page with the config embedded as ``window.__NETRUN_CONFIG__``.
"""

import html
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def find_vis_assets_dir() -> Path:
    """Locate the built app assets from netrun-ui-vis.

    Resolution order:
    1. Bundled inside the package: ``<netrun_ui_backend>/vis_assets/``
    2. Dev layout: ``<netrun_ui_backend>/../node_modules/netrun-ui-vis/dist/app/assets``
    """
    backend_dir = Path(__file__).parent.resolve()

    # 1. Bundled assets (installed package)
    bundled = backend_dir / "vis_assets"
    if bundled.is_dir():
        return bundled

    # 2. Dev layout via node_modules symlink
    project_dir = backend_dir.parent
    dev = project_dir / "node_modules" / "netrun-ui-vis" / "dist" / "app" / "assets"
    if dev.is_dir():
        return dev

    raise FileNotFoundError(
        "Built vis app assets not found. "
        "Run 'npm run build:app' in netrun-ui-vis first.\n"
        f"  Checked: {bundled}\n"
        f"  Checked: {dev}"
    )


def build_html(
    config_data: dict[str, Any],
    vis_assets_dir: Path,
    *,
    minimap: bool = False,
    expand_descriptions: bool = False,
) -> str:
    """Assemble a self-contained HTML string from *config_data*.

    Parameters
    ----------
    config_data:
        The raw netrun config dict (as it would appear in a ``.netrun.json``).
    vis_assets_dir:
        Directory containing the built ``index-*.js`` and ``index-*.css`` files.
    minimap:
        Whether to show the minimap overlay. Defaults to ``False``.
    expand_descriptions:
        Whether to keep node descriptions expanded. Defaults to ``False``
        (descriptions are collapsed regardless of config state).

    Returns
    -------
    str
        Complete HTML document ready to be written to a file.
    """
    # Find JS and CSS bundles
    js_file = css_file = None
    for f in vis_assets_dir.iterdir():
        if f.suffix == ".js" and js_file is None:
            js_file = f
        elif f.suffix == ".css" and css_file is None:
            css_file = f

    if js_file is None:
        raise FileNotFoundError(f"No JS bundle found in {vis_assets_dir}")

    js_content = js_file.read_text(encoding="utf-8")
    css_content = css_file.read_text(encoding="utf-8") if css_file else ""

    # Title from config — check both top-level and graph-level extra
    extra = config_data.get("extra", {}) or {}
    if not extra.get("description"):
        extra = (config_data.get("graph", {}) or {}).get("extra", {}) or {}
    description = extra.get("description") or "Netrun Graph"

    # Escape config JSON: prevent </script> breakout
    config_json = json.dumps(config_data, separators=(",", ":")).replace("</", "<\\/")

    # Build options object
    options: dict[str, Any] = {}
    if minimap:
        options["minimap"] = True
    if expand_descriptions:
        options["expandDescriptions"] = True
    options_json = json.dumps(options, separators=(",", ":"))

    css_block = f"\t<style>{css_content}</style>\n" if css_content else ""

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
\t<meta charset="UTF-8" />
\t<meta name="viewport" content="width=device-width, initial-scale=1.0" />
\t<title>Netrun: {html.escape(description)}</title>
\t<style>
\t\thtml, body {{
\t\t\tmargin: 0;
\t\t\tpadding: 0;
\t\t\twidth: 100%;
\t\t\theight: 100%;
\t\t\toverflow: hidden;
\t\t\tbackground: #1a1a1a;
\t\t\tfont-family: 'SF Mono', Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
\t\t}}
\t\t#app {{
\t\t\twidth: 100%;
\t\t\theight: 100%;
\t\t}}
\t</style>
{css_block}</head>
<body>
\t<script>window.__NETRUN_CONFIG__ = {config_json}; window.__NETRUN_OPTIONS__ = {options_json};</script>
\t<div id="app"></div>
\t<script type="module">{js_content}</script>
</body>
</html>"""


def _resolve_factories_in_config(config_data: dict[str, Any], working_dir: str) -> None:
    """Resolve factory nodes in-place, baking descriptions and ports into the config.

    This calls ``resolve_factory_ports`` for each factory node so that the
    standalone JS converter (which can't call Python factories) sees the
    resolved descriptions and ports.
    """
    from .converter import resolve_factory_ports

    graph = config_data.get("graph", config_data)
    nodes = graph.get("nodes", [])

    for node in nodes:
        factory = node.get("factory")
        if not factory:
            continue

        resolved = resolve_factory_ports(
            factory,
            node.get("factory_args", {}),
            working_dir=working_dir,
        )
        if resolved is None:
            continue

        in_ports, out_ports, description, factory_extra = resolved

        # Bake description into config (factory default, don't override explicit)
        if description and not node.get("description"):
            node["description"] = description

        # Bake resolved ports if node doesn't have explicit ones
        if in_ports and not node.get("in_ports"):
            node["in_ports"] = in_ports
        if out_ports and not node.get("out_ports"):
            node["out_ports"] = out_ports

        logger.debug("Resolved factory %s for node %s", factory, node.get("name"))


def export_html_from_file(
    input_path: Path,
    output_path: Path,
    vis_assets_dir: Path | None = None,
    *,
    minimap: bool = False,
    expand_descriptions: bool = False,
) -> None:
    """Read a ``.netrun.json`` file, build HTML, and write to *output_path*.

    Parameters
    ----------
    input_path:
        Path to the input ``.netrun.json`` file.
    output_path:
        Where to write the resulting HTML.
    vis_assets_dir:
        Override for built asset location (auto-detected if ``None``).
    minimap:
        Whether to show the minimap overlay.
    expand_descriptions:
        Whether to keep node descriptions expanded.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    raw = input_path.read_text(encoding="utf-8")
    config_data = json.loads(raw)

    # Resolve factory descriptions and ports before embedding
    _resolve_factories_in_config(config_data, working_dir=str(input_path.parent))

    if vis_assets_dir is None:
        vis_assets_dir = find_vis_assets_dir()

    html_content = build_html(config_data, vis_assets_dir, minimap=minimap, expand_descriptions=expand_descriptions)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_content, encoding="utf-8")
