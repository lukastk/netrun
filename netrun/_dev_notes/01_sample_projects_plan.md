# Plan: Sample Projects + `max_epochs` Feature

## Part 1: `max_epochs` Feature

### Overview

Add a `max_epochs` field to `NodeExecutionConfig` that caps the total number of epochs a node can have over its lifetime. If an input salvo condition triggers and would exceed this limit, the epoch is cancelled and an exception is raised.

**Use case:** Ensure a node only runs once (or N times) per net execution.

### Design

**Config field:**
```python
# In NodeExecutionConfig
max_epochs: int | None = None
"""Maximum total epochs this node can have (across entire net lifetime).
None = unlimited (default). If exceeded, the epoch is cancelled and
MaxEpochsExceeded is raised."""
```

**Exception class:**
```python
class MaxEpochsExceeded(Exception):
    def __init__(self, node_name: str, max_epochs: int):
        self.node_name = node_name
        self.max_epochs = max_epochs
        super().__init__(
            f"Node '{node_name}' exceeded max_epochs={max_epochs}"
        )
```

**Runtime check location:** `Net._execute_epoch()`, between rate limit check and epoch start. Uses a `_node_epoch_counts: dict[str, int]` counter (O(1) lookup).

**Behavior when limit exceeded:**
1. Cancel the epoch in netsim (packets destroyed — consistent with other error paths)
2. Raise `MaxEpochsExceeded`, following normal propagation/queueing rules

**Note on retries:** Retries happen within `_execute_epoch_with_retry` for the same epoch_id, so they don't increment the counter. Only new epoch creations count.

### Files to Modify

| File | Change |
|------|--------|
| `pts/netrun/05_net/00_config/01_nodes.pct.py` | Add `max_epochs` field to `NodeExecutionConfig` |
| `pts/netrun/05_net/01_net/02_net.pct.py` | Add `_node_epoch_counts` dict to `__init__`, add check in `_execute_epoch()`, add `MaxEpochsExceeded` exception |
| `pts/tests/05_net/test_net.pct.py` | Add tests for max_epochs behavior |

### Implementation Steps

1. Add `max_epochs: int | None = None` to `NodeExecutionConfig`
2. Add `MaxEpochsExceeded` exception class (in the net module, near other exceptions)
3. Add `_node_epoch_counts: dict[str, int] = {}` to `Net.__init__`
4. In `Net._execute_epoch()`, after rate limit check:
   - Increment `_node_epoch_counts[node_name]`
   - If `config.max_epochs` is set and count exceeds it:
     - Cancel the epoch via netsim
     - Update epoch record
     - Raise `MaxEpochsExceeded` (through normal propagation/queueing)
5. Write tests:
   - `test_max_epochs_one` — node with `max_epochs=1` runs once, second trigger raises
   - `test_max_epochs_none` — default (None) allows unlimited
   - `test_max_epochs_propagation` — exception follows propagate_exceptions setting
6. `nbl export --reverse && nbl export`
7. Run tests

---

## Part 2: Sample Projects

### Feature Coverage Matrix

#### Project 00: `basic_net_project` (enhance existing)

Currently demonstrates: function factory, linear pipeline, output queues, `print` capture, UI positions, JSON format, type annotations.

**Add:**
- Actions (project-level + node-level) with template variables (`$NODE_NAME`, `$PROJECT_ROOT`)
- Graph-level `extra` with description and viewport state
- `_node_config` attribute override on one function (TOML string)
- Multiple output ports (dict return annotation) on one node

#### Project 01: `thread_and_process_pools` (enhance existing)

Currently demonstrates: all 4 pool types, per-node pool assignment, recipes.

**Add:**
- Pool allocation methods (ROUND_ROBIN, LEAST_BUSY) on different nodes
- Node variables (`node_vars`) at net-level and per-node override
- `print_echo_stdout` on one node
- `max_epochs=1` on one node (showcase new feature)

#### Project 02: `remote_deployment` (keep as-is)

Already demonstrates: TOML format, remote pools, deploy infrastructure, SSH tunneling, systemd.

No changes needed — deployment features are well covered.

#### Project 03: `subgraphs` (new)

**Demonstrates:**
- Inline subgraph with internal nodes and edges
- File-referenced subgraph (loading from external `.netrun.json`)
- Exposed input/output ports on subgraphs
- Nested subgraphs (subgraph containing a subgraph)
- Edges crossing subgraph boundaries
- Subgraph `extra` metadata
- **Subgraph factories** — a factory module whose `get_node_config()` returns a `SubgraphConfig` instead of a `NodeConfig`, allowing parameterized generation of node groups (e.g., a pipeline factory that takes `num_stages`)
- **Port groups** — dot-separated port naming convention (e.g., `features.color`, `features.shape`) that `netrun-ui` renders as collapsible groups. Include nested port groups (e.g., `batch.images.train`, `batch.labels.train`). Port groups are purely a naming convention in `netrun` — they have no runtime effect — but `netrun-ui` detects them and provides:
  - Collapsible/expandable groups in the node UI
  - Group-level connections (connecting all ports in a group at once when the source and target groups have matching port names)
  - Nested group hierarchy

  The README should instruct the user to open the `.netrun.json` file in `netrun-ui` to see port groups in action.

**Structure:**
```
sample_projects/03_subgraphs/
  main.netrun.json          # Main config with inline + file-ref subgraphs
  shared_pipeline.netrun.json  # Reusable subgraph config
  nodes.py                  # Node functions
  pyproject.toml
  README.md
```

#### Project 04: `error_handling` (new)

**Demonstrates:**
- `max_epochs` (the new feature) — node limited to 1 epoch
- Retries with `retry_wait` and `on_node_failure` callback
- `timeout` on node execution
- Dead letter queue (`dead_letter_queue=True`, `dead_letter_path`)
- Exception queue (non-propagating: `propagate_exceptions=False`)
- Epoch cancellation via `ctx.cancel_epoch()`
- Runtime type checking with `type_checking_enabled` and intentional `PacketTypeMismatch`
- `ctx` special parameter (direct `NodeExecutionContext` access)

**Structure:**
```
sample_projects/04_error_handling/
  main.netrun.json
  nodes.py
  pyproject.toml
```

#### Project 05: `advanced_flow_control` (new)

**Demonstrates:**
- Custom salvo conditions (boolean expressions: `and`, `or`, `not`, port state predicates)
- Finite port slots (`slots_spec: finite(1)`)
- `max_parallel_epochs` (concurrent limit)
- `rate_limit_per_second`
- Start/stop node lifecycle functions (`start_node_func`, `stop_node_func`, `defer_startup`)
- Lazy packet values (`create_packet_from_value_func`)
- Catch-all output queue
- `undeclared_output_behavior: "error"` (or "discard")

**Structure:**
```
sample_projects/05_advanced_flow_control/
  main.netrun.json
  nodes.py
  pyproject.toml
```

#### Project 06: `actions_and_recipes` (new)

**Demonstrates:**
- Actions with all template variables (`$NODE_NAME`, `$NODE_ID`, `$NET_FILE_PATH`, `$NET_FILE_DIR`, `$PROJECT_ROOT`, `$DEFAULT_CMD`)
- Node-level actions (per-node overrides)
- Project-level `env` and node-level `node_env` variables
- Recipes with all prompt types (text, number, select, checkbox)
- Recipe with `get_prompts()` and `run()` functions
- File-path imports (`./nodes.py::my_func` syntax)
- TOML config format
- `working_directory` for actions

**Structure:**
```
sample_projects/06_actions_and_recipes/
  main.netrun.toml
  nodes.py
  recipes/
    add_node.py
    set_defaults.py
  pyproject.toml
```

### Full Feature Coverage Checklist

| Feature | Project |
|---------|---------|
| Function factory (`from_function`) | 00 |
| Linear pipeline with edges | 00 |
| Named output queues | 00 |
| `print` special parameter | 00 |
| UI positions (`extra.ui.position`) | 00 |
| Edge shorthand (`source_str`/`target_str`) | 00 |
| JSON config format | 00, 01, 03, 04, 05 |
| Type annotations on ports | 00 |
| Actions (project-level + node-level) | 00, 06 |
| Graph extra (description, viewport) | 00 |
| `_node_config` attribute override | 00 |
| Multiple output ports (dict return) | 00 |
| All 4 pool types | 01 |
| Per-node pool assignment | 01 |
| Recipes with prompts | 01, 06 |
| Pool allocation methods | 01 |
| Node variables (`ctx.vars`) | 01 |
| `print_echo_stdout` | 01 |
| `max_epochs` (new feature) | 01, 04 |
| TOML config format | 02, 06 |
| Remote deployment (SSH, pyinfra) | 02 |
| Inline subgraphs | 03 |
| File-referenced subgraphs | 03 |
| Exposed ports on subgraphs | 03 |
| Nested subgraphs | 03 |
| Subgraph factories (`get_node_config` returns `SubgraphConfig`) | 03 |
| Port groups (dot-separated naming convention for `netrun-ui`) | 03 |
| Nested port groups (`batch.images.train`) | 03 |
| Retries + `retry_wait` | 04 |
| `on_node_failure` callback | 04 |
| `timeout` | 04 |
| Dead letter queue | 04 |
| Exception queue (non-propagating) | 04 |
| Epoch cancellation (`ctx.cancel_epoch()`) | 04 |
| Runtime type checking / `PacketTypeMismatch` | 04 |
| `ctx` special parameter | 04 |
| Custom salvo conditions (boolean) | 05 |
| Finite port slots | 05 |
| `max_parallel_epochs` | 05 |
| `rate_limit_per_second` | 05 |
| Start/stop lifecycle functions | 05 |
| Lazy packet values | 05 |
| Catch-all output queue | 05 |
| `undeclared_output_behavior` | 05 |
| Template variables in actions | 06 |
| Node-level actions | 06 |
| `env` / `node_env` variables | 06 |
| All recipe prompt types | 06 |
| File-path imports | 06 |
| `working_directory` for actions | 06 |
| `defer_startup` | 05 |

### Implementation Order

1. **Implement `max_epochs` feature** (Part 1 above)
2. **Enhance project 00** — add actions, extra metadata, dict return, _node_config
3. **Enhance project 01** — add pool allocation, node vars, print echo, max_epochs
4. **Create project 03** — subgraphs
5. **Create project 04** — error handling
6. **Create project 05** — advanced flow control
7. **Create project 06** — actions and recipes
8. **Verify all projects** — `netrun validate`, `netrun info`, run notebooks
