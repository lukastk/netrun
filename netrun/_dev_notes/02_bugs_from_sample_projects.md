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

- **Project 04** (`04_error_handling`): Could re-add a `slow_node` demo using a thread pool so the timeout actually interrupts. Not done yet — would need adding a thread pool to the project config.
