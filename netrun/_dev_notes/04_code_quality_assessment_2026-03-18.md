# Netrun Package Code Quality Assessment

**Date:** 2026-03-18

## Executive Summary

The netrun package is ~22,500 lines of source code across 47 files, with ~36,800 lines of tests (1.6:1 test-to-source ratio). The core architecture is sound — the layered design (RPC -> Pool -> ExecutionManager -> Net) is well-motivated and the separation from netrun-sim is clean. However, there is meaningful over-engineering and bloat concentrated in specific areas. An estimated 25-30% of the codebase is speculative code that serves no current use case.

---

## 1. OVER-ENGINEERING (Most Significant)

### Storage Layer: Built for a Future That Hasn't Arrived

**Severity: HIGH**

- **13 serialization methods** (json, pickle, str, binary, msgpack, numpy, pandas_csv, pandas_parquet, polars_csv, polars_parquet, feather, torch, safetensors). Only `pickle` and `json` are ever used — in tests, samples, and docs.
- **6 compression methods** (gzip, bz2, lzma, zlib, zstd, lz4). Only `gzip` appears in one test. No sample project uses compression.
- **5 storage backends** (Local, S3, SSH, Rclone, GCS). Only `LocalBackendConfig` is used anywhere — sample projects, tests, everything. The other 4 backends are ~300 lines of untested-in-practice code with external dependencies (boto3, paramiko, google-cloud-storage).
- **5 hash algorithms** (adler32, crc32, sha256, blake2b, xxh64). Only `xxh64` is ever used. The other 4 are dead weight (~150 lines).
- **3 pickling methods** (pickle, dill, cloudpickle). Only `pickle` is used. The others add optional dependencies for zero benefit.

This is the clearest case of building for hypothetical requirements. The storage layer would be ~40-50% smaller if scoped to what's actually needed.

### RPC Layer: 4 Implementations with 80-90% Duplication

**Severity: MEDIUM-HIGH**

Four channel implementations (Async, Thread, Process, WebSocket) share nearly identical structure:
- Same `send()`/`recv()`/`try_recv()`/`close()` patterns
- Same `RPC_KEY_SHUTDOWN` handling (duplicated 12+ times)
- Same close/cleanup logic
- Inconsistent sync/async pairs — Thread and Process have `SyncXChannel` variants; Async and WebSocket don't

No base class or mixin extracts the common 80%. Each implementation is a near-copy with the transport swapped out. This is ~600 lines of redundant code.

### Pool Layer: Protocol Misalignment + Stdout Capture Overhead

**Severity: MEDIUM**

- The `Pool` protocol defines 8 methods, but `MultiprocessPool` has 35 methods and `RemotePoolClient` has 9 extra methods not in the protocol. The `ExecutionManager` imports concrete classes, not the protocol — defeating the abstraction's purpose.
- `MultiprocessPool`'s stdout/stderr capture adds ~200 lines of complexity (custom file-like objects, per-subprocess flusher threads, timestamp injection, buffer serialization). This is a real feature but disproportionately complex.
- 70-80% structural duplication across pool implementations (recv loop, monitor tasks, error checking).

---

## 2. CODE SMELLS

### Net Class: 4,244 Lines, 119 Methods

**Severity: HIGH**

This is the biggest single-file concern. The `Net` class is a god object handling:
- Pool lifecycle management
- Epoch execution and scheduling
- Packet routing and output queues (6 method variants)
- Logging (8+ method variants for different views)
- Controls and signals
- Caching and file storage
- Rate limiting and retries
- Variable resolution

The logging methods alone (`get_epoch_log`, `get_node_logs`, `print_epoch_logs`, `print_node_logs`, `print_all_logs`, etc.) could be extracted into a query interface. Output queue methods repeat similar logic with different filters.

### Tests Assert on Private State

**Severity: MEDIUM**

300+ assertions on `_`-prefixed attributes in test_net.pct.py:
```python
assert ctx._print_buffer == []
assert ctx._created_packets == []
assert ctx._cancelled is True
assert len(ctx._deferred_actions.actions) == 1
```
These tests are brittle — any internal refactor breaks them even if behavior is preserved.

### Unused Methods and Dead Code

**Severity: LOW-MEDIUM**

- `patch_to()` in `_iutils.base` — defined, never used
- `PacketStore._get()` — redundant alias for `peek()`
- `PacketStore.list_ids()` — never called outside its own file
- `PacketStore.save()`/`load()` — only used in tests, unclear purpose
- `try_recv()` on RPC channels — only 2 call sites
- `RPC_KEYS` list, `RPC_KEY_ERROR`, `RPC_KEY_BROKEN` — barely used (only in remote.py)
- Multiple WebSocket server helpers (`serve()`, `serve_background()`, `connect()`, `connect_channel()`) — only 2 are actually used

### Remote Pool Tests: Timing-Dependent

**Severity: MEDIUM**

Remote pool tests use `asyncio.sleep(10)` (10 instances) to keep servers alive. No event-based synchronization. This is a flaky test pattern that will break on slow CI.

---

## 3. WHAT'S WELL-DESIGNED

- **ExecutionManager** — Clean protocol, appropriate complexity, no bloat. Zero issues found.
- **Node Factories** — Function factory's signature introspection is thorough but proportionate. Broadcast and Join factories are focused and minimal.
- **Tools module** — Textbook "do one thing well." Template resolution, action execution, recipes — all clean and minimal.
- **CLI** — Well-partitioned, growing but managed. Commands are useful and orthogonal.
- **VarRef system** — Complex but justified (91 call sites). Three-phase resolution is a good design.
- **Factory protocol** — Three-phase resolution (config-time -> init-time -> worker-time) elegantly avoids pickling issues.
- **Config <-> netrun-sim bridge** — Clean `to_netrun_sim()` methods, proper separation of concerns.

---

## 4. BLOAT QUANTIFICATION

| Area | Estimated Bloat | Lines Affected |
|------|----------------|----------------|
| Unused serialization methods (11 of 13) | ~300 LOC | storage/_serialization |
| Unused storage backends (4 of 5) | ~300 LOC | storage/_backends |
| Unused hash algorithms (4 of 5) | ~150 LOC | _iutils/hashing |
| Unused pickling methods (2 of 3) | ~80 LOC | _iutils/pickling |
| RPC duplication | ~600 LOC | rpc/* |
| Pool duplication | ~400 LOC | pool/* |
| Net class logging/output bloat | ~200 LOC | net/02_net |
| Dead methods and unused features | ~100 LOC | scattered |
| **Total estimated removable** | **~2,100 LOC** | **~9% of total** |

If you also factor in code that's correctly implemented but serving speculative needs (full file storage replay infrastructure, bundle modes, remote pool server), the "speculative" total is closer to 5,000-6,000 LOC (~25%).

---

## 5. RECOMMENDATIONS (Prioritized)

### High Priority
1. **Slim the storage layer** — Keep pickle/json serialization, gzip compression, Local backend. Move everything else to a plugin/contrib pattern or remove it.
2. **Extract common RPC logic** into a base mixin to eliminate the 80% duplication.
3. **Break up the Net class** — Extract logging query, output queue management, and possibly control/signal handling into separate collaborator objects.

### Medium Priority
4. **Remove dead code** — `patch_to()`, `PacketStore._get()`, `list_ids()`, unused RPC keys.
5. **Fix test brittleness** — Replace private-state assertions with behavioral tests. Fix `asyncio.sleep(10)` patterns in remote tests.
6. **Align Pool protocol with reality** — Either enrich the protocol to cover `flush_stdout()` etc., or drop the pretense and just use concrete types.
7. **Add tests for `08_logging`** — It's the only module with zero test coverage.

### Low Priority
8. **Extract pool duplication** — Common recv loop, monitor task, error checking into shared utilities.
9. **Simplify PortState config** — 9 discriminated union classes could be one class with a `kind` field.
10. **Consolidate VarRef resolution helpers** — Three nearly-identical recursive patterns could share a generic traversal.

---

## Bottom Line

The architecture is fundamentally sound. The layered design, factory protocol, and netrun-sim integration are all well-thought-out. The problems are:

1. **Building for imaginary users** — 13 serialization formats, 5 backends, 5 hash algorithms, 3 pickling methods, when real usage is pickle + local + xxh64.
2. **Copy-paste implementations** — RPC and Pool layers duplicate 70-80% of their code across variants instead of sharing a base.
3. **God object accumulation** — Net at 4,244 lines / 119 methods needs decomposition.

None of these are architectural defects — they're maintenance and discipline issues that compound over time. The core ~75% of the codebase is clean, well-tested, and purposeful.
