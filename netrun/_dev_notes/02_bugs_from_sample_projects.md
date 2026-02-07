# Bugs Found During Sample Project Implementation

## Bug 1: `from_function` factory uses `PacketCountAllConfig` for non-list parameters — FIXED

### Problem

The `from_function` factory generates input salvo conditions that grab ALL packets from every port (`PacketCountAllConfig`), regardless of the parameter's type annotation. However, the execution wrapper only consumes the **first** packet for non-list parameters. Any remaining packets stay in the epoch, causing `CannotFinishNonEmptyEpochError` when `finish_epoch` is called.

This only manifests when multiple packets accumulate on a port before the salvo fires (e.g., multiple `inject_data` calls before `run_until_blocked`).

### Fix (applied)

In `pts/netrun/06_node_factories/00_from_function.pct.py`:
1. Extracted `_is_list_type()` to module level
2. Added `in_port_annotations` parameter to `_generate_input_salvo_condition()`
3. Non-list params → `PacketCountNConfig(count=1)`, list params → `PacketCountAllConfig()`
4. Updated call site in `_from_function` to pass annotations

### Sample project workarounds removed

- **Project 04** (`04_error_handling/main.py`): Now injects both "normal" and "cancel" canceller data upfront.
- **Project 05** (`05_advanced_flow_control/main.netrun.json`): Removed explicit `in_salvo_conditions` overrides from `rate_limited_worker` and `multi_output`.
- **Project 06** (`06_actions_and_recipes/main.py`): Now injects both names upfront.

---

## Bug 2: `timeout` on `NodeExecutionConfig` is never enforced — FIXED

### Problem

The `timeout` field exists on `NodeExecutionConfig` and is propagated through to `NetFuncPreprocessorNodeConfig`, but the execution path never applied it.

### Fix (applied)

In `pts/netrun/05_net/01_net/02_net.pct.py`, `_execute_epoch_with_retry()`:
- Wrapped the `run_allocate` call with `asyncio.wait_for(coro, timeout=config.timeout)` when `config.timeout is not None`
- `asyncio.TimeoutError` is caught and routed through `_handle_epoch_failure` as a `TimeoutError`, which means retries, dead letter queue, on_node_failure callbacks, and propagate_exceptions all work with timeouts

Note: For the `main` pool (SingleWorkerPool), synchronous blocking calls won't be interrupted — the timeout only fires between async yields. For thread/process pools, the timeout works because `run_allocate` awaits a message from the worker thread/process.

### Tests added

- `test_timeout_enforcement_raises_epoch_error` — ThreadPool, verifies EpochError with TimeoutError cause
- `test_timeout_goes_to_dead_letter_queue` — ThreadPool, propagate_exceptions=False, verifies DLQ + exception queue
- `test_timeout_none_no_limit` — MainPool, verifies timeout=None (default) doesn't interfere

### Impact on sample projects

- **Project 04** (`04_error_handling`): Re-added `slow_node` demo with a thread pool and `timeout: 0.5`.

---

## Unimplemented Features (config stubs only)

The following `NodeExecutionConfig` fields exist in the config model but are **never enforced** by the runtime. They were planned for sample project 05 but cannot be demonstrated.

| Config Field | Location | Description |
|---|---|---|
| `max_parallel_epochs` | `01_nodes.pct.py:203` | Limit concurrent running epochs per node. Config parsed but never checked when starting epochs. |
| `start_node_func` | `01_nodes.pct.py:193` | Function called when a node starts up. Config parsed but never called during Net lifecycle. |
| `stop_node_func` | `01_nodes.pct.py:194` | Function called when a node shuts down. Config parsed but never called during Net lifecycle. |
| `defer_startup` | `01_nodes.pct.py:198` | Delay `start_node_func` until first epoch. Companion to `start_node_func`, equally unenforced. |

### Features implemented but not in sample projects

These features work but don't naturally fit `from_function`-based sample projects:

- **`create_packet_from_value_func`** (lazy packets): Implemented on `NodeExecutionContext` but designed for raw `exec_node_func` usage. With `from_function`, the factory handles packet creation from return values — using lazy packets requires bypassing that, which goes against the factory pattern.
- **`undeclared_output_behavior`**: Implemented on `NetConfig` (both `"discard"` and `"error"` modes work). But with `from_function`, all output ports are auto-declared from the return annotation, so it never triggers. Only relevant for raw `exec_node_func` nodes.
