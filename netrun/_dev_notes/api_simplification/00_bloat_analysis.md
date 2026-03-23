# API Bloat Analysis

**Date:** 2026-03-18

Net has ~80 public methods. NodeInfo has ~35. They overlap heavily. Here are the 7 worst offenders.

---

## 1. Cache API exists in 3 places

Net has 10 cache methods. NodeInfo proxies 8 of them. The underlying `_cache_store` already does the work.

```
net.get_cached_entries("X")          # Net method
net.nodes["X"].cached_entries        # NodeInfo property (calls the Net method)
net._cache_store.get_entries("X")    # actual implementation
```

Three layers for the same operation. `clear_cache_by_version()` and `clear_cached_inputs()` are only ever called in their own tests.

**Net cache methods (10):** `get_cached_entries`, `get_cached_input_salvos`, `get_cached_output_salvos`, `get_cached_output_for_input`, `cache_stats`, `clear_cache`, `clear_node_cache`, `clear_cache_by_version`, `clear_cached_output_for_input`, `clear_cached_inputs`

**NodeInfo cache methods (8):** `cached_entries`, `cached_input_salvos`, `cached_output_salvos`, `get_cached_output_for_input`, `cache_stats`, `is_cache_enabled`, `clear_cache`, `clear_cached_output_for_input`

---

## 2. Log query API — 9 methods, most unused

| Method | Usage |
|---|---|
| `get_all_logs()` | **Zero call sites in entire codebase** |
| `list_epoch_log_ids()` | Only in its own test |
| `list_node_log_names()` | Only in its own test + internally by `print_all_logs` |
| `get_epoch_log()` | 3 test files, no sample projects |
| `get_node_logs()` | 1 sample project (00_basic), test_log_access |
| `get_all_logs_chronological()` | Only test_log_access |
| `print_epoch_logs()` | Never called directly |
| `print_node_logs()` | Only through NodeInfo delegation |
| `print_all_logs()` | **8 sample projects** — the one users actually call |

NodeInfo also has `print_all_logs()` and `print_epoch_logs()`, creating more duplication.

---

## 3. Output queue — 7 methods, 3 are dead weight

| Method | Usage |
|---|---|
| `flush_output_queue()` | Every sample project |
| `flush_all_output_queues()` | Tests and sample 12 |
| `get_output()` | Output queue tests |
| `try_get_output()` | Cache tests |
| `has_output()` | **Only its own test** |
| `output_count()` | **Only its own test** |
| `list_output_queues()` | **Only its own test** |

---

## 4. Execution methods with redundant alternatives

- `execute_startable_epochs()` — redundant with `run_step(auto_start_epochs=True)`. Has worse error handling (silently catches exceptions, prints to stderr). Only used in 2 test files.
- `get_running_epochs()` — only used in self-tests. NodeInfo already has `running_epochs` property.

---

## 5. Escape hatches nobody uses externally

| Property | External usage |
|---|---|
| `config_resolved` | Internal only |
| `graph` | Internal only |
| `netsim` | Internal only |
| `pools` | Tests only |
| `edges` | Tests only (NodeInfo has `incoming_edges`/`outgoing_edges`) |

Low-level packet primitives that `inject_data()` wraps:
- `create_external_packet(value)` — tests only
- `create_external_packets(values)` — tests only
- `inject_packet(packet_id, node, port)` — tests only

---

## 6. NodeExecutionConfig has 28 fields

Many are `T | None` "inherit from NetConfig" fields. Every node-level field is reflected on NetConfig too, leading to duplication across both classes:

**Duplicated across NetConfig and NodeExecutionConfig:**
- `propagate_exceptions`, `print_exceptions`, `type_checking_enabled`, `max_epochs`, `retries`, `retry_wait`, `timeout`, `print_echo_stdout`

This is a wide, flat config surface where the inheritance model adds complexity without clear benefit for simple use cases.

---

## 7. Storage config surface area

| Category | Options | Actually used |
|---|---|---|
| Serialization methods | 13 | pickle, json |
| Compression methods | 6 | gzip (in 1 test) |
| Storage backends | 5 | Local only |
| Hash algorithms | 5 | xxh64 only |
| Pickling methods | 3 | pickle only |

`NodeFileStorageConfig` alone has 16 fields. The storage layer was built for hypothetical requirements — 5 backend types with external dependencies (boto3, paramiko, google-cloud-storage) that no sample project or real usage exercises.
