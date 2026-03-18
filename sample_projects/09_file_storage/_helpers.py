"""Shared helpers for all file storage demo scripts."""

import os
import time
from pathlib import Path

from netrun.core import Net, NetConfig, NodeExecutionConfig
from netrun.storage.config import (
    NodeFileStorageConfig,
    NodeStorageConfig,
    StorageConfig,
)

CONFIG_PATH = Path(__file__).parent / "main.netrun.json"


async def run_pipeline(net: Net, seed: int) -> tuple[list, float]:
    """Inject seed, run to completion, return (results, elapsed_seconds)."""
    t0 = time.monotonic()
    net.inject_data("generate_data", "seed", [seed])
    await net.run_until_blocked()
    results = net.flush_output_queue("results")
    elapsed = time.monotonic() - t0
    return results, elapsed


def set_file_storage_on_all_nodes(config: NetConfig, fs_config: NodeFileStorageConfig):
    """Set NodeStorageConfig(file_storage=fs_config) on every node's execution_config."""
    for node_config in config.graph.nodes:
        if node_config.execution_config is None:
            node_config.execution_config = NodeExecutionConfig()
        node_config.execution_config.storage = NodeStorageConfig(file_storage=fs_config)


def make_net(fs_config: NodeFileStorageConfig, storage: StorageConfig) -> Net:
    """Load config from JSON, apply file storage to all nodes, return Net."""
    config = NetConfig.from_file(CONFIG_PATH)
    config.storage = storage
    set_file_storage_on_all_nodes(config, fs_config)
    return Net(config)


def list_stored_files(base_path: str) -> list[str]:
    """Walk directory tree, print relative paths with sizes. Returns list of relative paths."""
    paths = []
    for root, _dirs, files in os.walk(base_path):
        for f in sorted(files):
            full = os.path.join(root, f)
            rel = os.path.relpath(full, base_path)
            size = os.path.getsize(full)
            print(f"    {rel}  ({size} bytes)")
            paths.append(rel)
    return paths
