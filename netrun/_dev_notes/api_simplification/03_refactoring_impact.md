# Refactoring Impact: Every Call Site

**Date:** 2026-03-18

Every usage of every method being removed, moved, or renamed — across library code, tests, sample projects, and the external `aisi-economy-index-v2` project. The UI backend has zero usages of any affected method.

---

## 1. Methods being REMOVED — call sites to delete/rewrite

### `get_all_logs()` — 0 call sites
Nothing to do.

### `list_epoch_log_ids()` — 4 test call sites
All in `test_log_access.pct.py`: lines 313, 331, 350, 438. Delete these tests.

### `list_node_log_names()` — 5 test call sites + 1 internal
- Tests in `test_log_access.pct.py`: lines 200, 359, 378, 453, 457. Delete these tests.
- Internal: `02_net.pct.py:1263` — `print_all_logs` calls `self.list_node_log_names()`. Inline the logic (iterate `self._epochs` for unique node names).

### `execute_startable_epochs()` — 3 test call sites
- `test_net.pct.py:2077` → rewrite to `await net.run_step()`
- `test_signals.pct.py:303` → rewrite to `await net.run_step()`
- `test_signals.pct.py:1121` → rewrite to `await net.run_step()`

### `clear_cache_by_version()` — 0 call sites
Nothing to do (definition only, no tests even).

### `clear_cached_inputs()` — 1 test call site
- `test_cache.pct.py:869` → delete this test.

### `get_running_epochs()` — 2 test call sites
- `test_net.pct.py:1421` → rewrite to check `net._running_epochs` or remove test
- `test_controls.pct.py:422` → rewrite to `assert not net._running_epochs`

### `has_output()` — 3 test call sites
All in `test_output_queues.pct.py`: lines 179, 285, 430. Delete these tests.

### `output_count()` — 3 test call sites
All in `test_output_queues.pct.py`: lines 201, 286, 433. Delete these tests.

### `list_output_queues()` — 2 test call sites
All in `test_output_queues.pct.py`: lines 155, 369. Delete these tests.

---

## 2. Methods moving to `net.cache` — call sites to update

### `get_cached_entries(name)` → `net.cache.entries(name)`
- `test_cache.pct.py`: lines 382, 420, 533, 867, 870 (5 sites)
- NodeInfo delegation in `01_info.pct.py:310` — remove

### `get_cached_input_salvos(name)` → `net.cache.input_salvos(name)`
- `test_cache.pct.py:463` (1 site)
- `sample_projects/08_caching/main.py`: lines 133, 281 (2 sites)
- NodeInfo delegation in `01_info.pct.py:315` — remove

### `get_cached_output_salvos(name)` → `net.cache.output_salvos(name)`
- `test_cache.pct.py:469` (1 site)
- `sample_projects/08_caching/main.py`: lines 138, 249 (2 sites)
- NodeInfo delegation in `01_info.pct.py:320` — remove

### `get_cached_output_for_input(name, input)` → `net.cache.output_for_input(name, input)`
- `test_cache.pct.py`: lines 475, 480, 914 (3 sites, line 914 is via NodeInfo)
- `sample_projects/08_caching/main.py`: lines 145 (Net), 367 (NodeInfo) (2 sites)
- NodeInfo delegation in `01_info.pct.py:322-326` — remove

### `cache_stats()` → `net.cache.stats()`
- `test_cache.pct.py`: lines 505, 919 (2 sites, line 919 is NodeInfo property)
- `sample_projects/08_caching/main.py`: lines 174 (Net), 359 (NodeInfo) (2 sites)
- NodeInfo delegation in `01_info.pct.py:339-342` — remove

### `clear_cache()` → `net.cache.clear()`
- `test_cache.pct.py`: lines 753, 923 (2 sites, line 923 is via NodeInfo)
- NodeInfo delegation in `01_info.pct.py:328-330` — remove

### `clear_node_cache(name)` → `net.cache.clear(name)`
- `test_cache.pct.py:795` (1 site)
- NodeInfo delegation in `01_info.pct.py:330` — remove

### `clear_cached_output_for_input(name, input)` → `net.cache.clear_for_input(name, input)`
- `test_cache.pct.py:835` (1 site)
- `sample_projects/08_caching/main.py:207` (1 site)
- NodeInfo delegation in `01_info.pct.py:332-336` — remove

**Total cache migration: ~22 call sites across tests + 7 in sample 08**

---

## 3. Methods moving to `net.logs` — call sites to update

### `get_epoch_log(id)` → `net.logs.for_epoch(id)`
- `test_log_access.pct.py`: lines 246, 254 (2 sites)
- `test_net.pct.py`: lines 1432, 1473 (2 sites)
- Internal: `02_net.pct.py:1219` — `print_epoch_logs` calls `self.get_epoch_log()`. Move this logic into the logs sub-object.

### `get_node_logs(name)` → `net.logs.for_node(name)`
- `test_log_access.pct.py`: lines 185, 223, 224 (3 sites)
- `test_net.pct.py:1445` (1 site)
- `sample_projects/00_basic_net_project/main.py:59` (1 site)
- Internal: `02_net.pct.py:1237` — `print_node_logs` calls `self.get_node_logs()`. Move into logs sub-object.

### `get_all_logs_chronological()` → `net.logs.all_chronological()`
- `test_log_access.pct.py`: lines 277, 294, 393, 416 (4 sites)
- Internal: `02_net.pct.py:1256` — `print_all_logs` calls `self.get_all_logs_chronological()`. Move into logs sub-object.

### `print_epoch_logs(id, ...)` → `net.logs.print_epoch(id, ...)`
- 0 external call sites. Only the definition.

### `print_node_logs(name, ...)` → `net.logs.print_node(name, ...)`
- 0 external call sites. Only NodeInfo delegation in `01_info.pct.py:303`.

### `print_all_logs(...)` → `net.logs.print_all(...)`
- 9 sample projects:
  - `02_remote_deployment/main.py:132`
  - `03_subgraphs/main.py:52`
  - `04_error_handling/main.py:92`
  - `05_advanced_flow_control/main.py:69`
  - `06_actions_and_recipes/main.py:46`
  - `07_run_to_targets/main.py:35, 64`
  - `10_controls_and_signals/main.py:71`
  - `11_packet_requests/main.py:93`

**Total logs migration: ~13 call sites in tests + 10 in sample projects**

---

## 4. Methods being PRIVATIZED — call sites to update

### `get_edges_from_port()` → `_get_edges_from_port()`
- Internal: `02_net.pct.py` lines 987, 3866 — update to `self._get_edges_from_port()`
- `test_graph_queries.pct.py`: lines 71, 85, 95, 105, 175, 176 (6 sites) — delete these tests or rewrite to test via public API

### `has_downstream_connection()` → `_has_downstream_connection()`
- `test_graph_queries.pct.py`: lines 115, 125, 135, 145 (4 sites) — delete these tests

### `config_resolved` → private
- 0 external usages. Only `self._config_resolved` internally (dozens of sites — no change needed, it's already the private attribute).

### `create_external_packet()` → `_create_external_packet()`
- Internal: `02_net.pct.py:1019` — `create_external_packets` calls it. Update to `self._create_external_packet()`.
- `test_net.pct.py:3143` (1 site) — rewrite to use `inject_data`
- `test_packet_injection.pct.py`: lines 73, 90-106, 139, 229-230 (~9 sites) — rewrite tests to use `inject_data` or call private method directly

### `create_external_packets()` → `_create_external_packets()`
- Internal: `02_net.pct.py:1064` — `inject_data` calls it. Update to `self._create_external_packets()`.
- `test_packet_injection.pct.py`: lines 116, 128 (2 sites) — rewrite or call private

### `inject_packet()` → `_inject_packet()`
- Internal: `02_net.pct.py:1066` — `inject_data` calls it. Update to `self._inject_packet()`.
- `test_packet_injection.pct.py`: lines 142, 237, 238 (3 sites) — rewrite or call private

---

## 5. NodeInfo/EdgeInfo removal — call sites to update

### `net.nodes[]` access — ~27 call sites
- `test_net.pct.py`: ~23 sites (lines 2777-3320, 6696, 6953) — rewrite entire NodeInfo test section (~500 lines)
- `test_structured_logging.pct.py`: lines 1046-1047, 1539 — `net.nodes[name].epoch_logs` → filter `net.epoch_logs`
- `test_cache.pct.py:901` — `net.nodes["A"]` for cache helpers → rewrite to `net.cache.*`
- `sample_projects/08_caching/main.py`: lines 357, 362 — rewrite to `net.cache.*`
- `sample_projects/12_structured_logging/main.py:137` — rewrite to filter `net.epoch_logs`

### `NodeInfo` class references — ~30 sites
- All in test files — `isinstance` checks, imports. Delete/rewrite.

### `EdgeInfo` class references — ~12 sites
- All in test files. Delete/rewrite.

### `net.edges` property — 7 test call sites
All in `test_net.pct.py`: lines 2803, 3062, 3089, 3116, 3150, 3175, 3221. Delete EdgeInfo tests.

### `net.graph` property — 3 test call sites
- `test_net.pct.py`: lines 1209, 1239, 3144. Rewrite to access via `net._graph` or remove.

### `net.netsim` property — 1 test call site
- `test_net.pct.py:1210`. Rewrite to `net._netsim` or remove.

### `net.pools` property — 0 external call sites
Nothing to do.

---

## 6. Summary by file

### Tests requiring changes

| File | Sites | Effort |
|------|-------|--------|
| `test_net.pct.py` | ~50 | **HIGH** — entire NodeInfo/EdgeInfo section (~500 lines) deleted, graph/netsim tests rewritten, execute_startable_epochs/get_running_epochs rewritten |
| `test_log_access.pct.py` | ~20 | **MEDIUM** — all methods move to `net.logs.*`, dead method tests deleted |
| `test_cache.pct.py` | ~18 | **MEDIUM** — all methods move to `net.cache.*`, NodeInfo section rewritten |
| `test_output_queues.pct.py` | ~8 | **LOW** — delete has_output/output_count/list_output_queues tests |
| `test_graph_queries.pct.py` | ~10 | **LOW** — delete all tests (methods go private) |
| `test_packet_injection.pct.py` | ~14 | **LOW** — rewrite to use inject_data or private methods |
| `test_signals.pct.py` | ~2 | **LOW** — execute_startable_epochs → run_step |
| `test_controls.pct.py` | ~1 | **LOW** — get_running_epochs → private |
| `test_structured_logging.pct.py` | ~3 | **LOW** — net.nodes[].epoch_logs → filter net.epoch_logs |

### Sample projects requiring changes

| File | Sites | Change |
|------|-------|--------|
| `00_basic_net_project/main.py` | 1 | `net.get_node_logs(name)` → `net.logs.for_node(name)` |
| `02_remote_deployment/main.py` | 1 | `net.print_all_logs()` → `net.logs.print_all()` |
| `03_subgraphs/main.py` | 1 | same |
| `04_error_handling/main.py` | 1 | same |
| `05_advanced_flow_control/main.py` | 1 | same |
| `06_actions_and_recipes/main.py` | 1 | same |
| `07_run_to_targets/main.py` | 2 | same |
| `08_caching/main.py` | ~10 | Cache methods → `net.cache.*`, remove NodeInfo usage |
| `10_controls_and_signals/main.py` | 1 | `net.print_all_logs()` → `net.logs.print_all()` |
| `11_packet_requests/main.py` | 1 | same |
| `12_structured_logging/main.py` | 1 | `net.nodes[name].epoch_logs` → filter `net.epoch_logs` |

### Library files requiring changes

| File | Change |
|------|--------|
| `pts/netrun/06_net/01_net/02_net.pct.py` | Remove methods, create NetCacheAPI + NetLogQuery, privatize methods, update internal self-calls, remove `nodes` and `edges` properties |
| `pts/netrun/06_net/01_net/01_info.pct.py` | **Delete entirely** (NodeInfo + EdgeInfo) |
| `pts/netrun/06_net/01_net/00_context.pct.py` | No changes needed |
| `pts/netrun/06_net/01_net/03_run_to_targets.pct.py` | No changes needed |

---

## 7. External project: aisi-economy-index-v2

Located at `/Users/lukas/dev/20260208_tb8war__aisi-economy-index-v2/`. Uses netrun as a dependency. Source files are `.pct.py` in `pts/`, auto-generated `.py` in `src/`.

### Affected usages (2 files)

**`pts/dev_utils/utils.pct.py` — `_get_input_salvo()` function (lines 162-201)**

3 affected call sites:

```python
# Line 171 — net.nodes[] access (NodeInfo removal)
node_info = net.nodes[node_name]
if not node_info.in_port_names:
    ...
```
**Migration:** `net.get_node_config(node_name)` → check `cfg.in_ports`. Or inline: `next(n for n in net._config_resolved.graph.nodes if n.name == node_name).in_ports`.

```python
# Line 177 — get_cached_input_salvos (moving to net.cache)
cached = net.get_cached_input_salvos(node_name)
```
**Migration:** `net.cache.input_salvos(node_name)`

```python
# Lines 193-194 — on_epoch_start / on_epoch_end (no change needed)
net.on_epoch_start(_on_start)
net.on_epoch_end(_on_end)
```
**No change needed** — these methods stay on Net.

**`pts/ai_index/run_pipeline.pct.py` — `run_pipeline()` function**

0 affected call sites. All methods used are staying:
- `Net(config)` ✓
- `net.on_epoch_end(callback)` ✓
- `net.on_net_actions(callback)` ✓
- `net.run_until_blocked()` ✓
- `net.flush_all_output_queues()` ✓

### Summary

| File | Sites | Change |
|------|-------|--------|
| `pts/dev_utils/utils.pct.py` | 2 | `net.nodes[name]` → `net.get_node_config(name)`, `net.get_cached_input_salvos(name)` → `net.cache.input_salvos(name)` |
| `pts/ai_index/run_pipeline.pct.py` | 0 | No changes needed |

**Total impact: 2 call sites in 1 file.** Very minor.

---

### No changes needed
- `netrun-ui/netrun_ui_backend/` — zero usages of any affected method
- `pts/netrun/` outside of `06_net/01_net/` — no usages found
