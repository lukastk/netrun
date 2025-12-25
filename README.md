# netrun

A flow-based development (FBD) runtime system.

## Overview

This repository contains two main components:

- **netrun-sim**: A Rust library (with Python bindings) that simulates packet flow through a network. It handles flow mechanics, packet locations, and epoch lifecycles—but not actual execution or data storage.

- **netrun**: A pure Python runtime (coming soon) built on `netrun-sim` that handles actual node execution and packet data management.

This separation allows the execution and storage layers to be implemented independently of the flow logic.

## Repository Structure

```
repo/
├── CLAUDE.md               # Detailed documentation
├── README.md               # This file
├── netrun-sim/            # Simulation engine
│   ├── Cargo.toml          # Rust workspace root
│   ├── core/               # Rust library
│   │   ├── src/
│   │   ├── tests/
│   │   └── examples/
│   └── python/             # Python bindings (PyO3)
│       ├── pyproject.toml
│       ├── python/netrun_sim/
│       └── examples/
└── netrun/                 # Runtime (coming soon)
```

## Requirements

- Rust 1.85+ (edition 2024)
- Python 3.8+ (for Python bindings)
- [uv](https://github.com/astral-sh/uv) (recommended for Python)

## Building

### Rust Library

```bash
cd netrun-sim
cargo build -p netrun-sim
cargo build -p netrun-sim --release
```

### Python Bindings

```bash
cd netrun-sim/python
uv venv .venv && uv sync
uv run maturin develop
```

## Running Tests

```bash
# Rust tests
cd netrun-sim
cargo test -p netrun-sim

# Python examples
cd netrun-sim/python
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

## Running Examples

### Rust

```bash
cd netrun-sim
cargo run -p netrun-sim --example linear_flow
cargo run -p netrun-sim --example diamond_flow
```

### Python

```bash
cd netrun-sim/python
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

## Documentation

- [CLAUDE.md](CLAUDE.md) - Detailed architecture and API documentation
- [netrun-sim/python/README.md](netrun-sim/python/README.md) - Python bindings documentation

## License

MIT
