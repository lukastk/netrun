# netrun-ui Project Specification

## Overview

A visual editor for `NetConfig` files, built with Svelte + FastAPI. Features an infinite-canvas graph editor powered by SvelteFlow, with support for node factories, subgraphs, and recipes.

## Tech Stack

- **Frontend**: Svelte/SvelteKit + SvelteFlow
- **Backend**: FastAPI (Python)
- **File Formats**: `.netrun.json`, `.netrun.toml`

## Project Location

- Located at `netrun-ui/` in the monorepo root (alongside `netrun/` and `netrun-sim/`)
- Backend imports from `netrun` for config types and validation

---

## Core Features

### 1. Graph Editor (Infinite Canvas)

- Excalidraw-style infinite canvas with pan/zoom
- SvelteFlow for graph rendering and interaction
- Nodes display input ports (left) and output ports (right)
  - Port layout configurable at net-level, overridable per-node
- Drag edges between ports to create connections
  - 1:1 connections only (one output port → one input port, matching netrun-sim design)
  - Allow any connection, but show validation warnings for type mismatches
- Configurable edge styles (straight, smooth, orthogonal/elbow)
- Multi-select for moving nodes together
- Validation feedback: red border on ill-configured nodes
- Undo/Redo support
- Copy/paste nodes (without edges, works across tabs)

### 2. File Management

- Initialize UI into a folder (root for file explorer, like Jupyter Lab)
- Tree-view file explorer to browse and open `.netrun.json`/`.netrun.toml` files
- Multiple files open as tabs
- Explicit save (no auto-save)

### 3. Properties Sidebar (Left)

- Collapsible sections: "Ports", "Salvo Conditions", "Execution Config", etc.
- Edit properties of selected node
- Edit `NetConfig`, `GraphConfig` settings (including pools)
- Edit UI-specific settings
- Field-level validation indicators for missing/invalid fields
- Complex structures (salvo conditions): Form-based editing (dropdowns, dynamic sub-forms) with toggle to switch to raw JSON/TOML mode for power users

### 4. Node Creation

Two modes:
- **Regular nodes**: Full control over `NodeConfig`
- **Factory nodes**:
  - Select factory from net-level registered list OR type import path directly
  - Configure `factory_args` (fields derived by inspecting `get_node_config` signature)
  - Backend calls `get_node_config` to preview generated config
  - Can edit `NodeExecutionConfig` (except function fields)
  - Cannot edit the generated `NodeConfig` fields

### 5. Subgraphs (High Priority)

Subgraphs are groups of nodes that behave as a single node. This is a native `netrun` concept (not just UI).

**Data Model** (changes to `netrun.net.config`):
- `GraphConfig.nodes: list[NodeConfig | SubgraphConfig]`
- `SubgraphConfig` can define nodes inline OR reference a `.netrun.json` file by path
- When referencing a file, only the graph is used (other NetConfig fields ignored)
- Must specify which input/output ports to expose (with optional renaming)
- Nested subgraphs supported
- At runtime, subgraph resolves to its constituent nodes
- Node names become `subgraph_name.node_name` (e.g., `foo.bar`)

**UI Features**:
- **Creation**: Select nodes → "Create Subgraph"
  - Edges between selected nodes automatically included
  - Edges crossing the boundary become exposed ports (auto-detected)
- **Configuration**: Specify exposed ports with optional rename
- **Appearance**: Renders as a single node with exposed ports
- **Navigation**: Double-click to enter subgraph, opens as new tab
- **Breadcrumb**: Shows hierarchy at top: `net_name > subgraph_name > ...`
- **Editing**: Subgraph editor UI identical to net editor
- **Copy/Paste**: Subgraphs can be copied like nodes
- **Runtime**: No subgraph concept at runtime; resolves to flat net with prefixed node names

### 6. Meta Fields

Add `meta: dict[str, Any]` to config models for storing arbitrary metadata:
- `meta.ui` - UI-specific data (node positions, colors, edge style, etc.)
- Net-level meta for global UI settings and defaults

### 7. Code Location URIs

- `code_location_uri`: Custom URI for "Open code location" action
- `code_location`: Path formatted with default URI pattern (e.g., `vscode:{code_location}`)
- `project_root`: Base path for relative `code_location` values (defaults to net file location)
- Default URI format configurable in net-level meta

### 8. Toolbar & Command Palette

- Top toolbar for common actions
- Command palette (keyboard shortcut invocable)
- Right-click context menus
- Keyboard shortcuts configurable in `netrun-ui.toml`

### 9. Right Sidebar (Minimap/Outline)

- Minimap showing overview of entire graph
- Outline view for quick navigation
- Collapsible/toggleable

---

## Configuration Hierarchy

```
~/.config/netrun-ui.toml     # Global defaults
  ↓ (overridden by)
{workspace}/netrun-ui.toml   # Workspace/project defaults
  ↓ (overridden by)
{net_file}.meta.ui           # Per-net settings
  ↓ (overridden by)
{node}.meta.ui               # Per-node settings
```

---

## Recipes

Python scripts that generate nodes, edges, or subgraphs.

- **Discovery**: Specified via file paths, globs, or folders in config
- **Invocation**: Command palette, right-click menu, or toolbar
- **Arguments**: Recipes can request user input via modals
- **Placement**: Created nodes placed at center of viewport
- **Return**: Config objects (nodes, edges, subgraphs) to add to the net

Example signature:
```python
def my_recipe(ui: RecipeContext) -> RecipeResult:
    name = ui.prompt("Node name?")
    return RecipeResult(
        nodes=[NodeConfig(name=name, ...)],
        edges=[EdgeConfig(...)],
        subgraphs=[SubgraphConfig(...)],
    )
```

---

## Backend API (FastAPI)

Responsibilities:
- File I/O (read/write `.netrun.json`, `.netrun.toml`)
- Factory calls (`get_node_config`, signature inspection)
- Validation (type checking, import resolution)
- Recipe execution

Communication:
- **WebSocket** for real-time updates (validation feedback, factory previews)
- REST endpoints for simple operations where appropriate

Communication:
- WebSocket for real-time updates (validation, factory previews)
- REST endpoints for file operations

---

## MVP Phases

### Phase 1 (Core)
- Graph editor with nodes/edges (SvelteFlow)
- File open/save (single file, `.netrun.json` and `.netrun.toml`)
- Properties sidebar for node editing
- Regular node creation
- Factory node creation (critical for usability)
- Basic backend API (file I/O, factory calls)

### Phase 2
- File explorer (tree view) + tabs
- Validation (backend + UI feedback)
- Undo/redo
- Copy/paste nodes
- NetConfig/pool editing in sidebar

### Phase 3
- Subgraphs (requires `netrun` config changes first)
- Recipes
- Command palette + keyboard shortcuts
- Minimap/outline (right sidebar)
- Code location URIs

---

## Future Features

### Observability & Telemetry (Deferred)

- Running Nets send execution data to UI server
- Visualize epochs, packets, execution flow

### Run Nets from UI (Deferred)

- Execute nets via FastAPI backend

### Desktop App via Tauri (Deferred)

- Optional desktop deployment

---

## Implementation Notes

### Changes Required in `netrun.net.config`

1. **Node Name Uniqueness Validation** (TODO)
   - Node names must be unique within a net
   - Must account for subgraph expansion: if subgraph `foo` contains node `bar`, resolved name is `foo.bar`
   - Validation must prevent conflicts (e.g., can't have node `foo.bar` at top level if subgraph `foo` contains `bar`)
   - Add validation in `GraphConfig` or `NetConfig`

2. **SubgraphConfig** (TODO)
   - New config type: `SubgraphConfig`
   - Fields:
     - `name: str` - subgraph name (used as prefix for internal node names)
     - `nodes: list[NodeConfig | SubgraphConfig]` - inline definition, OR
     - `path: Path` - reference to external `.netrun.json` file
     - `exposed_in_ports: dict[str, PortRef]` - maps exposed port name → internal port
     - `exposed_out_ports: dict[str, PortRef]` - maps exposed port name → internal port
     - `meta: dict[str, Any]` - for UI position, etc.
   - Update `GraphConfig.nodes` type to `list[NodeConfig | SubgraphConfig]`
   - Add `resolve()` method to flatten subgraphs into nodes with prefixed names

3. **Meta Fields** (TODO)
   - Add `meta: dict[str, Any] = Field(default_factory=dict)` to:
     - `NetConfig`
     - `GraphConfig`
     - `NodeConfig`
     - `SubgraphConfig`
     - (possibly others as needed)

---

## Current Implementation Status

### Phase 1 - Complete

**Backend (`netrun-ui/backend/`):**
- FastAPI server with CORS for local development
- `POST /api/files/read` - Read `.netrun.json` or `.netrun.toml` files
- `POST /api/files/save` - Save files
- `POST /api/files/convert` - Convert between JSON and TOML
- `POST /api/factories/signature` - Get factory function signature
- `POST /api/factories/preview` - Preview factory-generated config
- `POST /api/factories/validate-import` - Validate import paths
- Converter module to translate between UI format and `GraphConfig` format

**Frontend:**
- SvelteFlow-based graph editor with custom `NetrunNode` component
- Nodes display input ports (left, green) and output ports (right, orange)
- Drag to create edges between ports
- Properties sidebar with collapsible sections (General, Factory, Input Ports, Output Ports)
- Toolbar with Open, Save, Undo, Redo, Add Node, Add Factory buttons
- Keyboard shortcuts: Cmd+O (open), Cmd+S (save), Cmd+Z (undo), Cmd+Shift+Z (redo)
- Factory nodes with editable factory path and "Refresh Preview" button
- Validation error display on nodes

### Current UX Limitations (to address in Phase 2)

1. **No workspace/directory concept**: The app doesn't "open into" a folder. Users must provide full file paths.

2. **Primitive file dialogs**: Uses browser `prompt()` for file paths instead of a proper file picker. This is because:
   - Browsers can't access the filesystem directly
   - A proper solution requires either:
     - File explorer UI that lists files from the backend
     - Tauri for native file dialogs (desktop app)

3. **Demo nodes on startup**: Currently shows 3 demo nodes when opening the app (for testing). Should instead show:
   - Empty canvas with "Open file" or "Create new" prompts
   - Or remember last opened file

4. **No "New File" action**: Can't easily clear the canvas and start fresh without refreshing the page.

5. **Single file only**: No tabs for multiple open files.

6. **No recent files**: No way to quickly reopen previously edited files.

### Phase 2 Priorities

1. **File explorer sidebar** (left, collapsible)
   - Backend endpoint to list directory contents
   - Tree view UI component
   - Click to open files
   - Context menu for new file, rename, delete

2. **Proper empty state**
   - Remove demo nodes
   - Show welcome screen with "Open File" / "New File" options
   - Or show file explorer by default

3. **New File action**
   - Toolbar button and Cmd+N shortcut
   - Clears canvas, sets "Untitled" state

4. **Tabs for multiple files**
   - Tab bar below toolbar
   - Close button on tabs
   - Dirty indicator on unsaved tabs

5. **Recent files**
   - Store in localStorage or backend config
   - Show in welcome screen or File menu

---

## Open Questions

(To be resolved through discussion)

---

## Technical Details

(To be filled in as design progresses)
