# netrun-ui

A visual editor for creating and editing [netrun](../netrun/) flow configurations. Built with SvelteKit (Svelte 5) and [SvelteFlow](https://svelteflow.dev/), with a FastAPI Python backend for file operations, factory resolution, and config validation.

## Features

- **Flow graph editor** — Drag-and-drop nodes, connect ports, auto-layout (ELK), minimap, pan/zoom
- **Node types** — Regular nodes, factory nodes (template-generated), subgraph nodes (nested flows), decoration nodes (annotations)
- **Sidebar configuration** — Edit ports, salvo conditions, execution config, variables, storage per node
- **Subgraph support** — Inline expansion/collapse, file-referenced subgraphs, breadcrumb navigation
- **Multi-tab editing** — Multiple files open with isolated undo/redo per tab
- **Node variables** — Typed variables with inheritance (net-level defaults, node-level overrides), env var support
- **Salvo condition editor** — Visual editor for boolean expressions over port states
- **Factory nodes** — Select from built-in factories or import custom ones, preview generated ports
- **Pool management** — Configure main, thread, multiprocess, or remote execution pools
- **Storage config** — Per-node caching and file storage with multiple backend types
- **File management** — Open/save `.netrun.json` and `.netrun.toml`, format conversion, file explorer
- **Command palette** — Cmd+K with 80+ commands, fuzzy search, keyboard shortcuts
- **Actions & recipes** — Shell command templates with variable resolution, interactive recipe prompts
- **Validation** — Pydantic-based config validation via backend
- **Undo/redo** — Full history per tab
- **Copy/paste** — Nodes across tabs

## Installation

### As a Python package (recommended)

```bash
pip install netrun-ui
```

### From source

```bash
cd netrun-ui
npm install          # Frontend dependencies
pip install -e .     # Backend + auto-builds frontend
```

## Usage

```bash
# Native window (default, uses pywebview)
netrun-ui

# Browser mode
netrun-ui --server

# Specify working directory
netrun-ui -C /path/to/project

# Development mode with hot reload
netrun-ui --dev
```

See [BACKEND_README.md](BACKEND_README.md) for full CLI options.

## Architecture

```
┌─────────────────────────────────┐
│  Frontend (SvelteKit + Svelte 5)│
│  ┌───────────┐ ┌──────────────┐│
│  │ FlowEditor│ │   Sidebar    ││
│  │ (XYFlow)  │ │ (config UI)  ││
│  └───────────┘ └──────────────┘│
│  ┌───────────┐ ┌──────────────┐│
│  │  TabBar   │ │ FileExplorer ││
│  └───────────┘ └──────────────┘│
│         │ REST API              │
├─────────┼───────────────────────┤
│  Backend (FastAPI + Python)     │
│  ┌─────────────────────────────┐│
│  │ File I/O, Factories,       ││
│  │ Validation, Actions,       ││
│  │ Recipes, Schema            ││
│  └─────────────────────────────┘│
└─────────────────────────────────┘
```

**Frontend** (`src/`): 35 Svelte components, 20 stores, TypeScript throughout. State management via Svelte 5 runes (`$state`, `$derived`, `$effect`).

**Backend** (`netrun_ui_backend/`): FastAPI endpoints for file read/write, factory resolution, config validation, action execution, and recipe support. Serves the built frontend as static files.

## Project Structure

```
netrun-ui/
├── src/                          # Frontend source
│   ├── routes/                   # SvelteKit pages (single-page app)
│   └── lib/
│       ├── components/           # 35 Svelte components
│       ├── stores/               # 20 state stores
│       ├── utils/                # Salvo parser, auto-layout, port groups
│       ├── types/                # TypeScript type definitions
│       └── api.ts                # REST API client
├── netrun_ui_backend/            # Python backend
│   ├── main.py                   # FastAPI app setup
│   ├── cli.py                    # CLI entry point
│   ├── converter.py              # UI ↔ netrun config conversion
│   └── routes/                   # API endpoints
│       ├── files.py              # File read/write/list
│       ├── factories.py          # Factory resolution
│       ├── actions.py            # Action execution
│       ├── recipes.py            # Recipe support
│       └── schema.py             # Config schema
├── tests/                        # Backend tests
├── examples/                     # Example projects
├── package.json                  # Frontend dependencies (SvelteFlow, ELK)
└── pyproject.toml                # Python package config (FastAPI, pywebview)
```

## Development

### Frontend only

```bash
npm install
npm run dev           # Vite dev server at localhost:5173
npm run build         # Production build to build/
npm run check         # Type checking
```

### Full stack (frontend + backend)

```bash
pip install -e .
netrun-ui --dev       # Vite dev server + FastAPI backend
```

### Key technologies

- **Svelte 5** with runes for reactivity
- **SvelteKit 2** with static adapter
- **SvelteFlow/XYFlow** for graph visualization
- **ELKjs** for auto-layout
- **FastAPI** + Uvicorn for backend
- **pywebview** for native window mode
- **Pydantic 2** for config validation
