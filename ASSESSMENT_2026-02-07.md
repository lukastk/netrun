## 2. HIGH Issues

### H2. `_ServerLogCallback` file handle never closed ✅ ALREADY FIXED

**File:** `netrun/pts/netrun/05_net/01_net/02_net.pct.py`

The `_ServerLogCallback` class opens a file handle on first `__call__` but provides no mechanism to close it -- no `__del__`, no `close()` method, and no context manager support. The file handle will leak until garbage collection.

> Already has `close()` method (line 87) which is called from `_PoolServerContext.stop()` (line 141). Subprocess copies rely on OS cleanup at process exit.

---

### H3. `NodeInfo.cfg` property does linear scan + deep copy on every access ✅ FIXED

**File:** `netrun/pts/netrun/05_net/01_net/01_info.pct.py`, lines 59-68

```python
@property
def cfg(self) -> "NodeConfig":
    for node_config in self._net._config_resolved.graph.nodes:
        if node_config.name == self._name:
            return node_config.model_copy(deep=True)
    raise KeyError(...)
```

Every access iterates over all nodes and creates a deep copy. Since `in_ports` and `out_ports` both call `self.cfg`, accessing either does 2 linear scans and 2 deep copies. O(n) per access with expensive copies where O(1) dict lookup would suffice.

For this, let's make it so that it stores the config in `self._cfg` upon `__init__`, to avoid this lookup.

---

### H5. `NameError` bug: `ChannelBroken` not imported in test workers ✅ ALREADY FIXED

**File:** `netrun/pts/tests/02_rpc/workers.pct.py`, line 63

`robust_worker` catches `ChannelBroken`, but only `ChannelClosed` is imported at line 20. Any `ChannelBroken` exception in a subprocess using this worker will crash with `NameError` instead of being handled.

---

### H8. Overly broad `pytest.raises(Exception)` in multiple tests ✅ FIXED

**Files:**
- `pts/tests/06_node_factories/test_from_function.pct.py`, lines 182, 185
- `pts/tests/03_pool/test_exceptions_remote.pct.py`, line 384
- `pts/tests/05_net/test_config.pct.py`, line 1912

```python
with pytest.raises(Exception):  # Pydantic ValidationError
```

Catching bare `Exception` masks regressions -- tests pass even if a completely different error occurs. The comments indicate specific expected types that should be used.

---

### H9. Project 00 uses `json.loads` + `model_validate` instead of `from_file` ✅ FIXED

**File:** `sample_projects/00_basic_net_project/main.py`, lines 22-24

All other sample projects use `NetConfig.from_file(config_path)`, which sets `_file_path` for relative path resolution. Project 00's manual approach means `_file_path` is `None`, making path resolution fragile and inconsistent.

---

### H10. Project 01 has no `main.py` -- only a notebook ⏭️ SKIPPED

**File:** `sample_projects/01_thread_and_process_pools/`

Unlike every other sample project, project 01 lacks a standalone `main.py`. Users cannot run `python main.py` for this project.

---

### H11. Project 00 imports `PortConfig` but never uses it ✅ FIXED

**File:** `sample_projects/00_basic_net_project/nodes.py`, line 13

Dead import in an example project that could confuse users reading the code.

---

### H13. `from_index` may be stale after earlier moves in `run_step` (netrun-sim) ✅ FIXED

**File:** `netrun-sim/core/src/net.rs`, lines 546-560

In `try_trigger_input_salvo`, the `from_index` for each packet is captured during iteration. After the first packet is moved out of an input port, subsequent packets' indices shift. The stale `from_index` values affect undo correctness -- `undo_packet_moved` uses `from_index` to `shift_insert` packets back, potentially restoring them in wrong FIFO order.

---

## 3. MEDIUM Issues

### M1. Duplicate `datetime` import shadows module ✅ FIXED

**File:** `netrun/pts/netrun/04_execution_manager.pct.py`, lines 27, 29

```python
import datetime              # line 27
from datetime import datetime  # line 29
```

Second import shadows the module with the class. `datetime.datetime.now()` would fail.

---

### M2. Deprecated `SHUTDOWN_KEY` alias still used internally ✅ FIXED

**File:** `netrun/pts/netrun/02_rpc/00_base.pct.py`, lines 149-151

Explicitly deprecated but still actively imported in `01_aio.pct.py`, `02_thread.pct.py`, and other RPC modules.

If it is deprecated the definition itself should also be removed.

---

### M3. Thread-channel `_closed` flag checked/set without lock ✅ FIXED

**File:** `netrun/pts/netrun/02_rpc/02_thread.pct.py`, lines 87-118

`SyncThreadChannel.send()` and `recv()` check/modify `self._closed` without acquiring `self._lock`. Race condition on the check-then-act pattern.

---

### M4. `PacketStore.consume()` evaluates lazy values outside lock ✅ FIXED

**File:** `netrun/pts/netrun/01_storage.pct.py`, lines 183-193

Lock released before lazy evaluation. If `_evaluate_lazy_value` raises, the packet has already been removed from the store with no recovery.

---

### M5. Massive `test_exception_hierarchy` duplication (8 files) ⏭️ SKIPPED

**Files:** Duplicated across `test_exceptions_aio.pct.py`, `test_exceptions_thread.pct.py`, `test_exceptions_multiprocess.pct.py`, `test_exceptions_remote.pct.py` in both `02_rpc/` and `03_pool/` directories.

Same test copy-pasted 8 times. If the exception hierarchy changes, 8 files need updating.

---

### M6. Tests accessing private attributes extensively ⏭️ SKIPPED

**Files:** Throughout `tests/04_execution_manager/` and `tests/03_pool/`

```python
assert manager._started is True
assert "pool" in manager._pools
assert pool._redirect_output is True
```

Tight coupling to private implementation. Tests should verify observable behavior through public API.

---

### M7. Hardcoded port numbers in remote/WebSocket tests ⏭️ SKIPPED

**Files:** `tests/02_rpc/test_remote.pct.py` (18881-18890), `tests/03_pool/test_remote.pct.py` (19001-19027), and exception test variants (29801-29913).

Will fail with "address already in use" if ports are occupied. Should bind to port 0.

---

### M8. Global mutable state for port allocation in tests ⏭️ SKIPPED

**Files:** `tests/04_execution_manager/test_execution_manager_remote.pct.py` (line 62), `tests/05_net/test_net.pct.py` (line 3506)

```python
_test_port = 19100
def _next_test_port():
    global _test_port
    _test_port += 1
    return _test_port
```

Not safe with `pytest-xdist` parallel execution.

---

### M10. Duplicated worker function definitions between workers modules and test files ⏭️ SKIPPED

**Files:** `tests/03_pool/test_thread.pct.py` and `tests/03_pool/workers.pct.py` define identical workers. Same in exception test files.

Divergence between copies would introduce subtle bugs.

---

### M12. Panics in production code paths (netrun-sim, 7 instances) ⏭️ SKIPPED

**File:** `netrun-sim/core/src/net.rs`, various locations

`move_packet()` (line 458-472), `consume_packet()`/`destroy_packet()` (lines 806-813, 837-844), `finish_epoch()` (line 897), and several `.expect()` calls throughout `run_step` and `send_output_salvo`. These guard internal invariants but are called through the public `do_action` API, meaning corrupted state crashes the host process instead of returning an error.

---

### M13. Massive ULID parsing code duplication in Python bindings (netrun-sim) ✅ FIXED

**File:** `netrun-sim/python/src/net.rs`, lines 559-631, 933-1020

The pattern `ulid::Ulid::from_string(id).map_err(...)` is duplicated ~20 times. A `str_to_ulid()` helper would eliminate this. `python_to_ulid()` already exists but doesn't cover the string case.

---

### M14. Test fixture duplication between unit and integration tests (netrun-sim)

**Files:** `netrun-sim/core/src/test_fixtures.rs` (281 lines) and `netrun-sim/core/tests/common/mod.rs` (187 lines)

Near-identical helper functions: `infinite_port()`, `finite_port()`, `simple_node()`, `edge()`, `linear_graph_3()`, etc. Integration tests should reuse the library's test fixtures.

---

### M15. `PyPortStateNumeric` uses fragile string-based dispatch (netrun-sim)

**File:** `netrun-sim/python/src/graph.rs`, lines 278-306

The `kind` field is a public `String` that must match one of five values. `to_core()` dispatches on this string, panicking on unknown values. A private enum would be safer.

---

### M16. `Salvo::to_core()` panics instead of returning PyResult (netrun-sim) ✅ FIXED

**File:** `netrun-sim/python/src/net.rs`, line 243

`.expect("Invalid ULID in salvo")` will crash the Python interpreter. Should return `PyResult` and raise a Python exception.

---

### M17. `Graph::new()` silently discards duplicate nodes (netrun-sim)

**File:** `netrun-sim/core/src/graph.rs`, lines 432-436

When two nodes share the same name, `.collect()` into HashMap silently overwrites the first. No error or warning, and `validate()` doesn't check for this.

---

### M18. `NetActionResponse` is not a standard Result type (netrun-sim)

**File:** `netrun-sim/core/src/net.rs`, lines 392-398

Custom `Success | Error` enum instead of `Result<T, E>`, preventing use of `?` operator and standard combinators.

---

### M19. Recipes feature uses UI data model, not NetConfig model

**Files:** `sample_projects/01_thread_and_process_pools/recipes/add_node.py`, `sample_projects/06_actions_and_recipes/recipes/`

Recipe `run()` functions operate on raw dicts with UI-specific keys (`"id"`, `"position"`, `"data": {"label", "nodeType"}`) -- the SvelteFlow data model, NOT `NodeConfig`. No documentation clarifies this.

---

### M20. Project 05 batch_processor salvo condition docs/behavior mismatch

**File:** `sample_projects/05_advanced_flow_control/nodes.py` (line 4)

Docstring says "fires only when the 'data' port has exactly 3 packets" but the salvo uses `equals_or_greater_than` (>= 3). Would also fire at 4+ packets.

---

### M22. Project 05 nodes.py comment references removed feature

**File:** `sample_projects/05_advanced_flow_control/nodes.py`, line 32

Docstring says "'debug_out' and 'extra_out' go to the catch-all queue" but `catch_all_output_queue` was refactored out. Outdated documentation.

---

### M23. Version dependency mismatch between netrun-ui and netrun

**File:** `netrun-ui/pyproject.toml`, line 15

Declares `netrun>=0.2.1` but actual version is `0.3.3`. Wide gap could mask compatibility issues.

---

### M24. netrun-ui has TODO comments for unimplemented context menus

**File:** `netrun-ui/src/lib/components/FlowEditor.svelte`, lines 244, 250

```
// TODO: Show pane context menu
// TODO: Show node context menu
```

Please remove.

---

### M25. `_bak` directory in project 02 contains dead code

**File:** `sample_projects/02_remote_deployment/_bak/`

Old monolithic `deploy_to_hetzner.py` (19KB) left behind from refactoring.

Please remove.

---

## 4. LOW Issues

### L2. Redundant `Path` import in config base

**File:** `netrun/pts/netrun/05_net/00_config/00_base.pct.py`, line 642

Imported twice in separate `#|export` cells.

---

### L3. `ConfigOpt`/`PrettyOpt` type aliases duplicated across 3 CLI modules

**Files:** `pts/netrun/09_cli/02_commands.pct.py`, `03_actions.pct.py`, `04_recipes.pct.py`

Could be defined once in `_helpers`.

---

### L4. No-op callback stubs are dead code

**File:** `netrun/pts/netrun/05_net/01_net/02_net.pct.py`

`_net_func_done_callback_noop` and `create_net_func_done_callback` are stubs with no functionality.

---

### L5. `node_id=node_name` conflation in actions CLI

**File:** `netrun/pts/netrun/09_cli/03_actions.pct.py`, line 112

`node_id` set to same value as `node_name`. If conceptually different, could cause confusion.

Just use `node_name`. This must be changed across all the sample projects as well, and 'netrun-ui/examples/actions_and_recipes.netrun.json'.

---

### L6. Duplicate edge detection in `Graph::validate()` is dead code (netrun-sim)

**File:** `netrun-sim/core/src/graph.rs`, lines 490-504

Iterates over `self.edges` (a `HashSet`) using a `seen_edges` HashSet. Since edges are already deduplicated by the HashSet, the `DuplicateEdge` error can never be produced.

---

### L7. `load_packet_into_output_port` takes `&String` instead of `&str` (netrun-sim)

**File:** `netrun-sim/core/src/net.rs`, line 1124

Non-idiomatic Rust. Only method in the file using `&String`.

---

### L8. `EventUTC` type is `i128` but timestamps are always positive (netrun-sim)

**File:** `netrun-sim/core/src/net.rs`, line 134

`u64` or `u128` would better express the constraint.

---

### L9. `Graph.edges()` returns `HashSet`, losing insertion order (netrun-sim)

**File:** `netrun-sim/core/src/graph.rs`, lines 464-467

If deterministic ordering is ever needed, this would need to be `IndexSet`.

---

### L11. No `Display` implementation for most netrun-sim types

Only `PortRef`, `Edge`, and error types have `Display`. Makes debugging harder.

---

### L12. `evaluate_salvo_condition` silently treats missing ports as count 0 (netrun-sim)

**File:** `netrun-sim/core/src/graph.rs`, line 109

A typo in a condition name would silently evaluate as "empty port" rather than error.

---

### L13. `_node_to_epochs` accumulates empty Vec entries (netrun-sim)

**File:** `netrun-sim/core/src/net.rs`, lines 933-935

When epochs finish, the Vec is retained in HashMap even when empty. Minor memory waste.

---

### L14. `UnconnectedOutputPortError` exception is never raised (netrun-sim)

**File:** `netrun-sim/python/src/errors.rs`, line 36

Dead code -- `send_output_salvo` handles unconnected ports by moving packets to `OutsideNet`.

---

### L15. `EdgeRef` in Python `__all__` does not exist (netrun-sim)

**File:** `netrun-sim/python/python/netrun_sim/__init__.py`, line 42

Would fail with `AttributeError` on import.

---

### L16. Heavy string-based ULID conversion in Python bindings (netrun-sim)

**File:** `netrun-sim/python/src/net.rs`, lines 20-42

All ULID conversions go through string serialization. Raw 128-bit integer conversion would be more efficient.

---

### L18. `Graph.validate()` loses structured error information (netrun-sim)

**File:** `netrun-sim/python/src/graph.rs`, lines 914-922

Converts errors to strings, losing node/port names and error variant.

---

### L19. Graph not validated on construction (netrun-sim)

**File:** `netrun-sim/core/src/graph.rs`, lines 432-457

`Graph::new()` does not call `validate()`. Invalid graphs can reach `NetSim::new()`.

---

### L20. Verbose manual Port cloning in `try_trigger_input_salvo` (netrun-sim)

**File:** `netrun-sim/core/src/net.rs`, lines 488-502

Manual field-by-field clone when `Port` derives `Clone`.

---

### L22. `tempfile` with manual cleanup instead of pytest fixtures in recipe tests

**File:** `netrun/pts/tests/08_tools/test_recipes.pct.py`, lines 35-100

Uses `delete=False` + manual `unlink()`. If assertion fails, temp file leaks.

---

### L23. `test_random_allocation` statistical flakiness

**File:** `netrun/pts/tests/04_execution_manager/test_execution_manager_thread.pct.py`

Sends 20 jobs to 3 workers and asserts all were used. ~0.003% chance of false failure.

---

### L25. Sample project 01 pyproject.toml has wrong project name

**File:** `sample_projects/01_thread_and_process_pools/pyproject.toml`, line 2

`name = "basic-net-project"` -- copied from project 00 and never updated.

