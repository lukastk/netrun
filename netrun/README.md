# netrun

A flow-based development (FBD) runtime for Python. Define networks of interconnected nodes, and netrun handles packet routing, worker pool execution, caching, and lifecycle management — powered by [netrun-sim](../netrun-sim/) for deterministic packet flow simulation.

## Key Features

- **Flow-based execution** — Define graphs of nodes connected by ports and edges; packets flow automatically based on salvo conditions
- **Multiple pool types** — Execute nodes across threads, processes, or remote workers via WebSockets
- **Node factories** — Create nodes from regular Python functions (`function` factory) or fan-out patterns (`broadcast` factory)
- **Signals and controls** — Pause, resume, and control network execution at runtime
- **Caching and storage** — Cache epoch results, store packet data to local/S3/SSH/GCS backends
- **Node variables** — Typed, inheritable configuration variables with environment variable support
- **Output queues** — Collect results from terminal nodes
- **CLI** — Validate configs, inspect graphs, convert formats, and query factory info
- **Config formats** — Define networks in JSON or TOML with `NetConfig.from_file()`

## Installation

```bash
cd netrun
uv sync
```

## Quick Example

Define a network in TOML:

```toml
# my_net.toml
[pools.main]
id = "main"
[pools.main.spec]
type = "main"

[[graph.nodes]]
factory = "netrun.node_factories.function"
[graph.nodes.factory_args]
func = "mymodule.process"
[graph.nodes.execution_config]
pools = ["main"]

[output_queues.results]
ports = [["process", "out"]]
```

Run it:

```python
import asyncio
from netrun.net.config import NetConfig
from netrun.net._net import Net

async def main():
    config = NetConfig.from_file("my_net.toml")
    async with Net(config) as net:
        net.inject_data("process", "x", [1, 2, 3])
        await net.run_until_blocked()
        for epoch_id in net.get_startable_epochs():
            await net.execute_epoch(epoch_id)
        results = net.flush_output_queue("results")
        print(results)

asyncio.run(main())
```

## Documentation

- [CLAUDE.md](../CLAUDE.md) — Full module documentation and API reference
- [PROJECT_SPEC.md](PROJECT_SPEC.md) — Detailed project specification
- [NBLITE_INSTRUCTIONS.md](NBLITE_INSTRUCTIONS.md) — How to write code (nblite workflow)

## Development

This project uses [nblite](https://github.com/lukastk/nblite) for literate programming. Source code lives in `.pct.py` files under `pts/`, not in `src/` (which is auto-generated).

```bash
# Edit source
vim pts/netrun/06_net/01_net/02_net.pct.py

# Export changes
nbl export --reverse && nbl export

# Run tests
uv run pytest src/tests/ -v

# Run specific tests
uv run pytest src/tests/net/ -v
uv run pytest src/tests/pool/test_thread.py -v
```

**Never edit files in `src/` directly** — they are auto-generated from `pts/`.
