# netrun

A flow-based development (FBD) runtime system.

## Overview

This repository contains two main components:

- **netrun-core**: A Rust library (with Python bindings) that simulates packet flow through a network. It handles flow mechanics, packet locations, and epoch lifecycles—but not actual execution or data storage.

- **netrun**: A pure Python runtime (coming soon) built on `netrun-core` that handles actual node execution and packet data management.

This separation allows the execution and storage layers to be implemented independently of the flow logic.

## Repository Structure

```
repo/
├── CLAUDE.md               # Detailed documentation
├── README.md               # This file
├── netrun-core/            # Simulation engine
│   ├── Cargo.toml          # Rust workspace root
│   ├── core/               # Rust library
│   │   ├── src/
│   │   ├── tests/
│   │   └── examples/
│   └── python/             # Python bindings (PyO3)
│       ├── pyproject.toml
│       ├── python/netrun_core/
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
cd netrun-core
cargo build -p netrun-core
cargo build -p netrun-core --release
```

### Python Bindings

```bash
cd netrun-core/python
uv venv .venv && uv sync
uv run maturin develop
```

## Running Tests

```bash
# Rust tests
cd netrun-core
cargo test -p netrun-core

# Python examples
cd netrun-core/python
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

## Running Examples

### Rust

```bash
cd netrun-core
cargo run -p netrun-core --example linear_flow
cargo run -p netrun-core --example diamond_flow
```

### Python

```bash
cd netrun-core/python
uv run python examples/linear_flow.py
uv run python examples/diamond_flow.py
```

## Documentation

- [CLAUDE.md](CLAUDE.md) - Detailed architecture and API documentation
- [netrun-core/python/README.md](netrun-core/python/README.md) - Python bindings documentation

## License

MIT
