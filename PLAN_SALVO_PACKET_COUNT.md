# Implementation Plan: Salvo Packet Count Feature

## Overview

Currently, `SalvoCondition.ports` is a `Vec<PortName>` that specifies which ports participate in a salvo. When triggered, **all packets** from each listed port are consumed/sent.

This plan adds the ability to specify **how many packets** to take from each port:
- `All` - take all packets (current behavior)
- `Count(n)` - take up to `n` packets (takes fewer if port has fewer)

## Proposed Data Structures

### New Types (graph.rs)

```rust
/// Specifies how many packets to take from a port in a salvo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PacketCount {
    /// Take all packets from the port.
    All,
    /// Take at most this many packets (takes fewer if port has fewer).
    Count(u64),
}
```

### Updated SalvoCondition

```rust
pub struct SalvoCondition {
    pub max_salvos: u64,
    pub ports: HashMap<PortName, PacketCount>,  // Changed from Vec<PortName>
    pub term: SalvoConditionTerm,
}
```

### Why HashMap instead of Vec?

- **Prevents duplicates**: Each port can only appear once (enforced by type)
- **Cleaner semantics**: A port either participates in the salvo or doesn't
- **O(1) lookup**: Efficient port lookup by name
- **No ordering needed**: Packet collection order from ports doesn't matter semantically

## Files to Modify

### 1. netrun-sim/core/src/graph.rs

| Change | Location | Description |
|--------|----------|-------------|
| Add `PacketCount` enum | After line 51 | New enum: `All`, `Count(u64)` |
| Update `SalvoCondition` | Lines 127-142 | Change `ports: Vec<PortName>` to `ports: HashMap<PortName, PacketCount>` |
| Update validation | Lines 479-491 | Change to iterate `condition.ports.keys()` |
| Update validation | Lines 512-524 | Same pattern for output salvo validation |

### 2. netrun-sim/core/src/net.rs

| Change | Location | Description |
|--------|----------|-------------|
| Update `run_until_blocked()` | Lines 534-546 | Apply `PacketCount` logic when collecting packets |
| Update `send_output_salvo()` | Lines 1043-1065 | Apply `PacketCount` logic when sending packets |

**New packet collection logic:**

```rust
// In run_until_blocked(), replace lines 539-545:
for (port_name, packet_count) in &salvo_cond_data.ports {
    let port_location = PacketLocation::InputPort(
        candidate.target_node_name.clone(),
        port_name.clone(),
    );
    if let Some(packet_ids) = self._packets_by_location.get(&port_location) {
        let take_count = match packet_count {
            PacketCount::All => packet_ids.len(),
            PacketCount::Count(n) => std::cmp::min(*n as usize, packet_ids.len()),
        };
        for pid in packet_ids.iter().take(take_count) {
            salvo_packets.push((port_name.clone(), *pid));
            packets_to_move.push((*pid, port_name.clone()));
        }
    }
}
```

### 3. netrun-sim/core/src/test_fixtures.rs

| Change | Location | Description |
|--------|----------|-------------|
| Update helper functions | Throughout | Use `HashMap<PortName, PacketCount>` instead of `Vec<PortName>` |

### 4. netrun-sim/python/src/graph.rs

| Change | Location | Description |
|--------|----------|-------------|
| Add `PacketCount` Python enum | New | `#[pyclass]` wrapper for `PacketCount` |
| Update `SalvoCondition` | Lines 510-559 | Change `ports: Vec<String>` to `HashMap<String, PacketCount>` |
| Update `to_core()` | Lines 544-549 | Convert Python dict to Rust HashMap |
| Update `from_core()` | Lines 552-558 | Convert Rust HashMap to Python dict |

**Python API design:**

```python
from netrun_sim import PacketCount, SalvoCondition

# Explicit packet counts (dict style)
SalvoCondition(
    max_salvos=1,
    ports={
        "queue.high": PacketCount.all(),
        "queue.low": PacketCount.count(5),
    },
    term=...,
)

# Shorthand: list of port names (all packets from each)
SalvoCondition(
    max_salvos=1,
    ports=["in", "out"],  # Converts to {"in": All, "out": All}
    term=...,
)

# Shorthand: single port name
SalvoCondition(
    max_salvos=1,
    ports="in",  # Converts to {"in": All}
    term=...,
)
```

### 5. netrun-sim/python/src/net.rs

| Change | Location | Description |
|--------|----------|-------------|
| Potentially update `Salvo` | Lines 192-245 | May need to expose packet count info if useful |

### 6. Test Files

| File | Changes |
|------|---------|
| `core/tests/common/mod.rs` | Update `all_ports_non_empty_condition()`, `output_salvo_condition()`, `simple_node()` |
| `core/tests/graph_api.rs` | Update any direct `SalvoCondition` construction |
| `core/tests/net_api.rs` | Update any direct `SalvoCondition` construction |
| `core/tests/workflow.rs` | Update salvo constructions at lines 118-121, 290-293, 343-346, 360-362 |

### 7. Example Files

| File | Changes |
|------|---------|
| `core/examples/linear_flow.rs` | Update salvo conditions at lines 162-190 |
| `core/examples/diamond_flow.rs` | Update salvo conditions |
| `python/examples/linear_flow.py` | Update salvo conditions at lines 38-40, 48-49 |
| `python/examples/diamond_flow.py` | Update salvo conditions |
| `python/examples/*.ipynb` | Update any notebook examples |

### 8. Python Type Stubs

| File | Changes |
|------|---------|
| `python/python/netrun_sim/__init__.pyi` | Add `PacketCount`, `SalvoPort` type hints |

## Implementation Order

1. **Phase 1: Core Rust types**
   - Add `PacketCount` and `SalvoPort` to `graph.rs`
   - Update `SalvoCondition` struct
   - Update validation logic

2. **Phase 2: Core Rust logic**
   - Update `run_until_blocked()` in `net.rs`
   - Update `send_output_salvo()` in `net.rs`

3. **Phase 3: Test fixtures**
   - Update `test_fixtures.rs` helper functions
   - Update `tests/common/mod.rs`

4. **Phase 4: Rust tests**
   - Update all test files to use new types
   - Add new tests for `Count(n)` behavior
   - Run `cargo test` to verify

5. **Phase 5: Rust examples**
   - Update `linear_flow.rs`
   - Update `diamond_flow.rs`
   - Run examples to verify

6. **Phase 6: Python bindings**
   - Add `PacketCount` pyclass
   - Add `SalvoPort` pyclass
   - Update `SalvoCondition` Python wrapper
   - Support both shorthand (`"port"`) and explicit (`SalvoPort(...)`)

7. **Phase 7: Python type stubs**
   - Update `__init__.pyi`

8. **Phase 8: Python examples**
   - Update `linear_flow.py`
   - Update `diamond_flow.py`
   - Update Jupyter notebooks

9. **Phase 9: Documentation**
   - Update `CLAUDE.md` if needed
   - Update `python/README.md`

## New Test Cases to Add

```rust
#[test]
fn test_input_salvo_takes_count_packets() {
    // Port has 5 packets, salvo specifies Count(3)
    // Verify only 3 packets are consumed into epoch
}

#[test]
fn test_input_salvo_takes_all_when_count_exceeds_available() {
    // Port has 2 packets, salvo specifies Count(10)
    // Verify all 2 packets are consumed (not an error)
}

#[test]
fn test_output_salvo_sends_count_packets() {
    // Output port has 5 packets, salvo specifies Count(2)
    // Verify only 2 packets are sent to edge
}

#[test]
fn test_mixed_all_and_count_in_same_salvo() {
    // Salvo has two ports: one All, one Count(3)
    // Verify correct packets taken from each
}
```

## Backwards Compatibility

The Python bindings should support multiple input formats for convenience:

```python
# Old style: list of port names (still works)
ports=["in", "out"]
# Interpreted as: {"in": All, "out": All}

# Single port shorthand
ports="in"
# Interpreted as: {"in": All}

# New explicit style: dict with PacketCount values
ports={"in": PacketCount.all(), "out": PacketCount.count(5)}
```

This is achieved by accepting `Union[str, List[str], Dict[str, PacketCount]]` in the Python constructor:
- `str` → `{port_name: All}`
- `List[str]` → `{name: All for name in list}`
- `Dict[str, PacketCount]` → use as-is

## DSL Syntax Update

Update the TOML DSL example to support packet counts:

```toml
[nodes.Batcher.in_salvo_conditions.batch]
when = "count(queue) >= 10"

# Explicit packet counts (inline table)
ports = { queue = 5, control = "all" }

# Or using dotted keys
[nodes.Batcher.in_salvo_conditions.batch.ports]
queue = 5        # Take 5 packets
control = "all"  # Take all (explicit)

# Shorthand (all packets from each):
ports = ["queue", "control"]
```

## Summary of Changes

| Category | Files | Estimated Scope |
|----------|-------|-----------------|
| Core types | 1 file | ~20 lines new (just `PacketCount` enum) |
| Core logic | 1 file | ~20 lines modified |
| Test fixtures | 2 files | ~30 lines modified |
| Rust tests | 4 files | ~50 lines modified |
| Rust examples | 2 files | ~20 lines modified |
| Python bindings | 2 files | ~60 lines new/modified |
| Python stubs | 1 file | ~15 lines new |
| Python examples | 4 files | ~30 lines modified |
| **Total** | **17 files** | **~245 lines** |

## Key Design Decisions

1. **HashMap over Vec**: Prevents duplicate ports, cleaner semantics
2. **PacketCount enum**: Simple `All` vs `Count(n)` - no complex struct needed
3. **Graceful underflow**: `Count(10)` on a port with 3 packets takes all 3 (not an error)
4. **Python flexibility**: Accept `str`, `List[str]`, or `Dict[str, PacketCount]` for ergonomics
