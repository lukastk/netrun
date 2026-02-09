# Plan: Storage Node Factories (`save`, `load`, `stash`)

> **SUPERSEDED**: This plan has been superseded by native file storage, implemented
> as a core feature alongside caching in `netrun.storage`. File storage is configured
> at the `NodeExecutionConfig.storage.file_storage` level rather than as separate
> node factories. The serialization, compression, and backend infrastructure from
> this plan was reused in the native implementation. See `netrun/storage/` for the
> implementation.

## Context

Netrun nodes process packets in-memory. For large data, checkpointing, or cross-machine sharing, users need a way to persist packet values to external storage. This plan adds **three node factories** that share common serialization and backend infrastructure:

- **`save`** — input only, persists a packet to storage
- **`load`** — loads from storage and emits a lazy packet; always has an input port for triggering
- **`stash`** — receives a packet, saves it, and emits a lazy packet that reads it back on consumption

All three support configurable storage backends (local, S3, GCS, SSH, rclone, memory) and serialization formats (JSON, pickle, numpy, pandas, polars, torch, etc.). At node **startup**, each validates required library imports and tests backend connectivity.

## Module Layout

Three public factory modules + one internal shared package:

```
pts/netrun/07_node_factories/
├── 00_from_function.pct.py          # existing (unchanged)
├── 01__storage/                     # internal shared infrastructure
│   ├── 00_serialization.pct.py      # default_exp node_factories._storage._serialization
│   ├── 01_backends.pct.py           # default_exp node_factories._storage._backends
│   └── 02_retrieval.pct.py          # default_exp node_factories._storage._retrieval
├── 02_save.pct.py                   # default_exp node_factories.save
├── 03_load.pct.py                   # default_exp node_factories.load
└── 04_stash.pct.py                  # default_exp node_factories.stash
```

Exports to:
```
src/netrun/node_factories/
├── __init__.py                      # update: add save, load, stash
├── from_function.py                 # existing
├── _storage/
│   ├── _serialization.py
│   ├── _backends.py
│   └── _retrieval.py
├── save.py                          # get_node_config, get_node_funcs, _factory_desc
├── load.py                          # get_node_config, get_node_funcs, _factory_desc
└── stash.py                         # get_node_config, get_node_funcs, _factory_desc
```

Config usage:
```json
{ "factory": "netrun.node_factories.save" }
{ "factory": "netrun.node_factories.load" }
{ "factory": "netrun.node_factories.stash" }
```

Update `src/netrun/node_factories/__init__.py`:
```python
from . import from_function, save, load, stash
__all__ = ["from_function", "save", "load", "stash"]
```

---

## Shared: `_storage._serialization`

### Exports

```python
class SerializationMethod(str, Enum):
    json = "json"
    pickle = "pickle"
    str = "str"
    binary = "binary"
    msgpack = "msgpack"
    numpy = "numpy"
    pandas_csv = "pandas_csv"
    pandas_parquet = "pandas_parquet"
    polars_csv = "polars_csv"
    polars_parquet = "polars_parquet"
    feather = "feather"           # Arrow IPC (auto-detects pandas vs polars)
    torch = "torch"
    safetensors = "safetensors"

def serialize(value: Any, method: SerializationMethod, **kwargs) -> bytes
def deserialize(data: bytes, method: SerializationMethod, **kwargs) -> Any
def get_file_extension(method: SerializationMethod) -> str
def validate_imports(method: SerializationMethod) -> None
```

- `pickle` delegates to `netrun._iutils.pickling`, respects `pickling_method` kwarg
- `feather`: auto-detects pandas vs polars on serialize; `feather_lib` kwarg on deserialize
- All third-party imports are lazy; `validate_imports()` eagerly imports to check availability
- Extension map: json→`.json`, pickle→`.pkl`, str→`.txt`, binary→`.bin`, msgpack→`.msgpack`, numpy→`.npy`, pandas_csv→`.csv`, pandas_parquet→`.parquet`, polars_csv→`.csv`, polars_parquet→`.parquet`, feather→`.feather`, torch→`.pt`, safetensors→`.safetensors`

---

## Shared: `_storage._backends`

### Protocol

```python
class StorageBackend(Protocol):
    def write(self, key: str, data: bytes) -> None
    def read(self, key: str) -> bytes
    def exists(self, key: str) -> bool
    def delete(self, key: str) -> None
```

### Backend Configs (pydantic discriminated union)

```python
BackendConfig = Annotated[
    MemoryBackendConfig | LocalBackendConfig | S3BackendConfig
    | GCSBackendConfig | SSHBackendConfig | RcloneBackendConfig,
    Field(discriminator="type")
]
```

| Config class | `type` | Fields | Credential fields (all optional) |
|---|---|---|---|
| `MemoryBackendConfig` | `"memory"` | — | — |
| `LocalBackendConfig` | `"local"` | `base_path: str` | — |
| `S3BackendConfig` | `"s3"` | `bucket`, `prefix=""`, `region?`, `endpoint_url?` | `access_key?`, `secret_key?` |
| `GCSBackendConfig` | `"gcs"` | `bucket`, `prefix=""` | `credentials_path?` |
| `SSHBackendConfig` | `"ssh"` | `host`, `base_path`, `port=22`, `username?` | `key_path?`, `password?` |
| `RcloneBackendConfig` | `"rclone"` | `remote` (e.g. `"myremote:bucket/path"`) | — |

All configs with credential fields also have `credentials_var: str | None = None`.

### Path resolution

- `LocalBackendConfig.base_path`: if relative, resolved against `project_root_path` (from `_net_config`). This is the **only** backend where `project_root_path` applies.
- All other backends: paths are on remote systems, used as-is.

`project_root_path` is passed into `create_backend()` and used only for LocalBackend:
```python
def create_backend(
    config: BackendConfig,
    ctx_vars: dict[str, Any] | None = None,
    project_root: Path | None = None,
) -> StorageBackend
```

### Credential resolution order

1. Explicit fields on config (from factory_args)
2. If `credentials_var` is set and `ctx_vars` is provided → `ctx_vars[credentials_var]` (dict with matching field names)
3. SDK defaults (boto3 chain, ssh-agent, ADC, rclone.conf)

### Implementations

- **MemoryBackend**: Class-level dict (shared within process)
- **LocalBackend**: `{base_path}/{key}`, creates dirs via `Path.mkdir(parents=True, exist_ok=True)`
- **S3Backend**: boto3 — `put_object`, `get_object`, `head_object`, `delete_object`. Key = `{prefix}{key}`
- **GCSBackend**: google-cloud-storage blob operations on `{prefix}{key}`
- **SSHBackend**: paramiko SFTP — `{base_path}/{key}`
- **RcloneBackend**: subprocess to rclone CLI — `rcat` (write), `cat` (read), `lsf` (exists), `delete`

### Validation functions

```python
def validate_backend_imports(config: BackendConfig) -> None
    # Eagerly import SDK. Raises ImportError with install instructions.

def validate_backend_connectivity(
    config: BackendConfig,
    ctx_vars: dict[str, Any] | None = None,
    project_root: Path | None = None,
) -> None
    # memory → no-op
    # local → check base_path writable (create dir if needed)
    # s3 → head_bucket
    # gcs → bucket.exists()
    # ssh → connect + stat(base_path)
    # rclone → rclone lsd <remote>
```

---

## Shared: `_storage._retrieval`

Top-level importable functions for `LazyPacketValueSpec.func_import_path`:

```python
def retrieve_value(
    backend_config_json: str,
    key: str,
    serialization: str,
    serialization_kwargs_json: str = "{}",
    cleanup: bool = False,
) -> Any:
    """Read + deserialize. Deletes after read if cleanup=True."""

def peek_value(
    backend_config_json: str,
    key: str,
    serialization: str,
    serialization_kwargs_json: str = "{}",
) -> Any:
    """Read + deserialize. Never deletes."""
```

- All args are JSON-serializable (LazyPacketValueSpec requirement)
- Backend config serialized to JSON; credentials **stripped** (re-resolved at consumption via SDK defaults)
- `cleanup=False` by default

---

## Key/Path Naming (shared logic)

All three factories share the same key-generation logic:

| Setting | Behavior |
|---|---|
| `dynamic_key=True` | Key comes from `key` input port. Used as-is (no extension appended). |
| `key=None` (default, no dynamic) | Auto-generate: `{key_prefix}{ulid}{ext}` |
| `key="results"` (explicit, no dynamic) | `results{ext}` |
| `on_exists="overwrite"` (default) | Overwrite silently |
| `on_exists="error"` | Raise if key exists |
| `on_exists="append_number"` | Try `key{ext}`, `key_1{ext}`, `key_2{ext}`, ... |

`on_exists` only meaningful with explicit `key` (ULID keys can't collide). When `dynamic_key=True`, `on_exists` applies to the dynamically provided key.

---

## Factory 1: `save`

**Purpose**: Takes a packet and saves it to storage. No output.

### factory_args

```python
def get_node_config(_net_config=None, *, backend: dict, serialization: str = "pickle",
    pickling_method: str = "pickle", key: str | None = None, key_prefix: str = "",
    on_exists: str = "overwrite", dynamic_key: bool = False) -> NodeConfig
```

### Ports & salvos

- `dynamic_key=False`:
  - Input: `data` (1 slot)
  - Salvo: trigger when `data` non-empty, take 1
- `dynamic_key=True`:
  - Inputs: `data` (1 slot) + `key` (1 slot)
  - Salvo: trigger when both non-empty, take 1 from each
- No output ports

### exec_func(ctx, packets)

1. Consume `data` packet
2. If `dynamic_key`: consume `key` packet (a string), use as storage key
3. Else: resolve key (ULID or explicit static key, handle `on_exists`)
4. Serialize value → bytes
5. Create backend via `create_backend(config, ctx.vars, project_root)`
6. Write bytes to backend

### start_func(net)

1. `validate_imports(serialization)` — check serialization library
2. `validate_backend_imports(backend_config)` — check SDK
3. `validate_backend_connectivity(backend_config, net_vars, project_root)` — test connection

---

## Factory 2: `load`

**Purpose**: Loads data from storage and emits it as a lazy packet. Always has an input port.

### factory_args

```python
def get_node_config(_net_config=None, *, backend: dict, serialization: str = "pickle",
    pickling_method: str = "pickle", key: str | None = None,
    dynamic_key: bool = False, send_on_startup: bool = False,
    cleanup: bool = False) -> NodeConfig
```

### Ports & salvos

- `dynamic_key=False`:
  - Input: `trigger` (1 slot, any type — value is ignored, just triggers a load of configured `key`)
  - Salvo: trigger when `trigger` non-empty, take 1
- `dynamic_key=True`:
  - Input: `key` (1 slot, str — value IS the storage key to load)
  - Salvo: trigger when `key` non-empty, take 1
- Output: `out` (1 slot) in both modes

### exec_func(ctx, packets)

1. If `dynamic_key`: consume `key` packet, use as storage key
2. Else: consume `trigger` packet (ignore value), use configured `key`
3. Build `LazyPacketValueSpec` pointing to `retrieve_value` (or `peek_value` if `cleanup=False`)
4. `ctx.create_packet_from_value_func(...)` → deferred packet ID
5. Load into `out`, send output salvo

### start_func(net)

1. Validate imports + backend imports + connectivity (same as save)
2. If `send_on_startup=True`: inject a trigger/None packet into the node's own input port to kick off the first load
   - Uses `net` API to create a packet and place it at the input port
   - This causes the normal epoch flow to fire once on startup

### Note on send_on_startup

The `start_func(net)` has access to the full Net instance. To inject a startup trigger:
- Create a packet via `net._packet_store.register(packet_id, None)`
- Use `net._sim.do_action(CreatePacket(...))` + `TransportPacketToLocation(packet_id, InputPort(node_name, "trigger"))`
- Then `run_until_blocked()` will pick it up and trigger the epoch

The exact API will be verified during implementation. If Net doesn't expose a clean injection method, we can add a helper or use `defer_startup=True` + manual trigger as a workaround.

---

## Factory 3: `stash`

**Purpose**: Receives a packet, saves it, and emits a lazy packet that reads it back.

### factory_args

```python
def get_node_config(_net_config=None, *, backend: dict, serialization: str = "pickle",
    pickling_method: str = "pickle", key: str | None = None, key_prefix: str = "",
    on_exists: str = "overwrite", dynamic_key: bool = False,
    cleanup: bool = False) -> NodeConfig
```

### Ports & salvos

- `dynamic_key=False`:
  - Input: `data` (1 slot)
  - Salvo: trigger when `data` non-empty, take 1
- `dynamic_key=True`:
  - Inputs: `data` (1 slot) + `key` (1 slot)
  - Salvo: trigger when both non-empty, take 1 from each
- Output: `out` (1 slot) in both modes

### exec_func(ctx, packets)

1. Consume `data` packet
2. If `dynamic_key`: consume `key` packet, use as storage key
3. Else: resolve key (ULID or explicit, handle `on_exists`)
4. Serialize value → bytes
5. Create backend, write bytes
6. Build `backend_config_json` (credentials stripped)
7. `ctx.create_packet_from_value_func("netrun.node_factories._storage._retrieval.retrieve_value", ...)`
8. Load into `out`, send output salvo

### start_func(net)

Same as save: validate imports + backend imports + connectivity.

---

## Startup Validation (all three factories)

`start_func(net)` runs at node startup (or first epoch if `defer_startup=True`). It performs three checks:

1. **Serialization imports**: `validate_imports(method)` — e.g., `import pandas` for `pandas_parquet`
2. **Backend SDK imports**: `validate_backend_imports(config)` — e.g., `import boto3` for S3
3. **Backend connectivity**: `validate_backend_connectivity(config, net_vars, project_root)` — e.g., `head_bucket` for S3

For credential resolution at startup, `net_vars` comes from `_net_config.node_vars` (net-level only, captured in closure). Full `ctx.vars` merge (net + node level) happens at exec time. This is a best-effort check that catches most issues.

If any check fails, the exception propagates immediately with a descriptive error message.

---

## Example Configs

### Save to local parquet
```json
{
  "name": "save_results",
  "factory": "netrun.node_factories.save",
  "factory_args": {
    "serialization": "pandas_parquet",
    "backend": { "type": "local", "base_path": "./checkpoints" }
  }
}
```

### Load from S3 on startup
```json
{
  "name": "load_model",
  "factory": "netrun.node_factories.load",
  "factory_args": {
    "serialization": "torch",
    "key": "model_weights",
    "send_on_startup": true,
    "backend": { "type": "s3", "bucket": "models", "prefix": "v2/" }
  }
}
```

### Stash with dynamic key
```json
{
  "name": "checkpoint",
  "factory": "netrun.node_factories.stash",
  "factory_args": {
    "serialization": "pickle",
    "dynamic_key": true,
    "backend": { "type": "local", "base_path": "./cache" }
  }
}
```

### Rclone backup
```toml
[[graph.nodes]]
name = "backup"
factory = "netrun.node_factories.save"

[graph.nodes.factory_args]
serialization = "binary"
backend = { type = "rclone", remote = "gdrive:backups/pipeline" }
```

---

## Tests

```
pts/tests/07_node_factories/
├── test_store_serialization.pct.py   # serialize/deserialize round-trips for all methods
├── test_store_backends.pct.py        # memory + local backend ops; S3/GCS/SSH/rclone conditional
├── test_save.pct.py                  # save factory: startup validation, exec, dynamic_key, on_exists
├── test_load.pct.py                  # load factory: exec, dynamic_key, send_on_startup, cleanup
└── test_stash.pct.py                 # stash factory: full round-trip, lazy packet, dynamic_key
```

---

## Implementation Order

1. `01__storage/00_serialization.pct.py` — standalone, no dependencies
2. `01__storage/01_backends.pct.py` — standalone (credential resolution self-contained)
3. `01__storage/02_retrieval.pct.py` — imports from `_serialization` and `_backends`
4. `02_save.pct.py` — simplest factory (no output port, no lazy packets)
5. `03_load.pct.py` — output only, lazy packets, send_on_startup
6. `04_stash.pct.py` — combined save + lazy output
7. Update `src/netrun/node_factories/__init__.py`
8. Tests: serialization → backends → save → load → stash

## Verification

```bash
cd /Users/lukas/dev/20260113_w3pmcj__netrun2/netrun
nbl export --reverse && nbl export
uv run pytest src/tests/node_factories/ -v
```

## Status

- [x] SUPERSEDED — replaced by native file storage in `netrun.storage`
