# netrun

A flow-based development (FBD) runtime system. Define networks of interconnected nodes, and netrun handles packet routing, execution orchestration, caching, and lifecycle management.

## Components

### [netrun-sim](netrun-sim/) — Simulation Engine

A Rust library (with Python bindings) that simulates packet flow through a network. Handles packet locations, flow conditions, and epoch lifecycles — but not actual execution or data storage. This separation allows execution and storage to be implemented independently of the flow logic.

### [netrun](netrun/) — Runtime

A pure Python package built on netrun-sim. Provides flow-based network execution (`Net`), RPC channels, worker pools (thread/process/remote), node factories, caching, file storage, a CLI, and more.

### [netrun-ui](netrun-ui/) — Visual Editor

A SvelteKit + FastAPI visual editor for creating and editing netrun flow configurations. Supports graph editing, subgraphs, node variables, salvo conditions, factory nodes, multi-tab editing, and a command palette.

## Repository Structure

```
repo/
├── netrun-sim/             # Simulation engine (Rust + Python bindings)
│   ├── core/               # Rust library (netrun-sim crate)
│   └── python/             # Python bindings (PyO3 + Maturin)
├── netrun/                 # Runtime (pure Python, nblite project)
│   ├── pts/                # Source code (.pct.py files)
│   └── src/                # Auto-generated Python modules
└── netrun-ui/              # Visual editor
    ├── src/                # Frontend (Svelte 5, SvelteFlow)
    └── netrun_ui_backend/  # Backend (FastAPI)
```

## Quick Start

### netrun-sim (Rust)

```bash
cd netrun-sim
cargo build -p netrun-sim
cargo test -p netrun-sim
cargo run -p netrun-sim --example linear_flow
```

### netrun-sim (Python bindings)

```bash
cd netrun-sim/python
uv venv .venv && uv sync
uv run maturin develop
uv run python examples/linear_flow.py
```

### netrun

```bash
cd netrun
uv sync
uv run pytest src/tests/ -v
```

### netrun-ui

```bash
# As a Python package
pip install netrun-ui
netrun-ui

# From source (development)
cd netrun-ui
npm install
pip install -e .
netrun-ui --dev
```

## Requirements

- **netrun-sim**: Rust 1.85+ (edition 2024), Python 3.8+ (for bindings)
- **netrun**: Python 3.11+, [uv](https://github.com/astral-sh/uv)
- **netrun-ui**: Node.js 18+, Python 3.11+

## Documentation

- [CLAUDE.md](CLAUDE.md) — Full architecture and API documentation
- [netrun-sim/README.md](netrun-sim/README.md) — Simulation engine docs
- [netrun/README.md](netrun/README.md) — Runtime docs
- [netrun-ui/README.md](netrun-ui/README.md) — Visual editor docs
- [netrun/PROJECT_SPEC.md](netrun/PROJECT_SPEC.md) — Runtime specification

## License

MIT
