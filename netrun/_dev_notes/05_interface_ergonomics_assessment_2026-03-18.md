# Netrun Interface & Ergonomics Assessment

**Date:** 2026-03-18

## Executive Summary

The netrun interface is split into two distinct experiences: **writing node functions** (excellent — 9/10) and **orchestrating flows** (decent — 7/10). The function factory is a genuine design win, letting users write plain Python functions that become flow nodes with minimal ceremony. The orchestration API is actually more capable than it appears — `run_until_blocked()` handles the full execution loop in a single call — but this is obscured by sample projects that all use the verbose manual loop pattern. The real friction is in verbose configs, async-only APIs, and overlapping result/error retrieval methods.

---

## 1. THE GOOD: Writing Node Functions

The function factory is netrun's best interface. A user writes:

```python
def double(x: int, print) -> int:
    print(f"Doubling {x}")
    return x * 2
```

And gets a fully wired node with typed input/output ports, print capture, and auto-generated salvo conditions. No decorators, no registration, no boilerplate.

**What works well:**

- **Parameters become input ports** — dead simple, leverages existing Python knowledge
- **Return type becomes output port(s)** — dict return for multi-output is Pythonic
- **Special params are opt-in** — `print`, `log`, `ctx` are injected only when listed in the signature. Users who don't need them never see them.
- **Type hints drive runtime validation** — `list[int]` actually works via beartype, including generics. Surprising positive.
- **Error handling is just Python** — raise an exception, retries happen automatically if configured. `ctx.retry_count` tells you where you are.
- **`ctx.vars`** — simple dict access for per-node configuration. Clean.

**Sharp edges (minor):**

- `Batch(int)` vs `list[int]` is a conceptual split that requires one-time learning. `Batch` collects multiple packets into a list; `list[int]` is a single packet whose value is a list. Not obvious without docs, but clear once explained.
- `PreCreatedPacket` for lazy evaluation is a mental model switch. Rare, but the naming helps.
- `*args` / `**kwargs` are rejected — necessary but not explained inline.
- Default parameter values are silently ignored — could surprise users expecting them to work as defaults for missing packets.

**Verdict: 9/10.** This is excellent API design. For 90% of use cases, users just write functions.

---

## 2. THE PAIN: Config Authoring

### Verbosity

A minimal 3-node pipeline requires ~55 lines of JSON. A realistic one with UI metadata hits ~140 lines. The cost comes from three sources:

**1. Edge declarations are verbose (4 fields each):**
```json
{
  "source_node": "fetch_data",
  "source_port": "out",
  "target_node": "process",
  "target_port": "data"
}
```

No shorthand exists. A 5-node linear pipeline needs 4 of these blocks. Compare to what users might expect:

```
fetch_data.out -> process.data
process.out -> format.input
```

**2. Factory + factory_args repetition:**
Every node repeats the same factory boilerplate:
```json
{
  "name": "my_node",
  "factory": "netrun.node_factories.from_function",
  "factory_args": {"func": "nodes.my_function"},
  "execution_config": {"pools": ["main"]}
}
```

The factory path is 42 characters of noise repeated per node.

**3. UI metadata mixed with logic:**
Sample configs include `extra.ui.position`, `extra.ui.actions`, descriptions — adding ~50 lines that have nothing to do with flow logic. This is clearly intended for netrun-ui consumption, but it clutters hand-written configs.

### Discoverability

- **Factory args are opaque.** Looking at `"factory_args": {"func": "nodes.double"}`, there's no indication what other args exist. Users must run `netrun factory-info <factory>` or read source code.
- **No JSON Schema.** No IDE autocomplete, no inline validation, no hover docs.
- **Salvo conditions are write-only.** When the auto-generated defaults don't work, users must write deeply nested condition trees. The schema is powerful but hostile to hand-authoring:

```json
"term": {
  "type": "port",
  "port_name": "data",
  "state": {"type": "equals_or_greater_than", "value": 3}
}
```

### TOML Is Better But Underused

Only 1 of 13 sample projects uses TOML. It's significantly more readable:
```toml
[[graph.nodes]]
name = "greeter"
factory = "netrun.node_factories.from_function"
factory_args = { func = "./nodes.py::greet" }
```

The file-path import syntax (`./nodes.py::greet`) is more ergonomic than Python import paths. But the ecosystem (samples, docs) pushes JSON.

### Validation Messages: Good

Pydantic errors show full paths (`graph -> nodes -> 0 -> in_ports -> port1 -> slots_spec`). Graph validation catches duplicate names, dangling edges, fan-out violations. Error messages are clear. No "did you mean?" suggestions for typos, though.

**Verdict: 5/10.** Configs are the tax users pay. Verbose, low discoverability, no IDE support. Clearly designed for UI-first authoring, not hand-writing.

---

## 3. THE MISLEADING PAIN: The Execution Loop

Every netrun sample project contains this pattern:

```python
async with Net(config) as net:
    net.inject_data("source", "in", [1, 2, 3])
    while True:
        await net.run_until_blocked()
        startable = net.get_startable_epochs()
        if not startable:
            break
        for epoch_id in startable:
            await net.execute_epoch(epoch_id)
    results = net.flush_output_queue("results")
```

This appears in **11 of 12 sample projects**. But **it's unnecessary for most cases.**

### The API Already Supports Simple Usage

`run_until_blocked()` has `auto_start_epochs=True` by default. It internally loops: moving packets, executing all startable epochs (concurrently via `asyncio.gather`), and repeating until no more progress is possible. So the simplest correct usage is:

```python
async with Net(config) as net:
    net.inject_data("source", "in", [1, 2, 3])
    await net.run_until_blocked()
    results = net.flush_output_queue("results")
```

There's also `start_background()` + `wait_until_done()` for background execution with SIGINT handling.

### The Real Problem: Samples Teach the Wrong Pattern

The manual loop only matters when you need:
- Sequential epoch execution (the auto path runs them concurrently)
- Selective epoch execution (skip certain epochs)
- Mid-flow inspection between epochs

**None of the 12 sample projects use these capabilities.** They all blindly execute every startable epoch. Yet they all write the verbose loop instead of a single `await net.run_until_blocked()`.

This is a **documentation problem, not an API problem.** The simple one-liner exists and works. It's just not shown anywhere.

### Remaining Friction

- **Async-only.** `run_until_blocked()` and `execute_epoch()` are async. There are `start_sync()`/`stop_sync()` lifecycle wrappers, but no sync execution convenience. Users unfamiliar with async Python face a learning curve.
- **No sync one-liner.** Something like `Net.run_sync(config, inputs={"source.in": [1,2,3]})` would lower the barrier significantly.

**Verdict: 7/10.** The API is actually good — the problem is that samples don't demonstrate the simple path. Fixing the samples would immediately improve perceived ergonomics.

---

## 4. THE MIXED: Output & Error Retrieval

### Output Queues: Too Many Ways

There are 6 output queue methods:

| Method | Purpose |
|--------|---------|
| `flush_output_queue(name)` | Get all results from named queue |
| `flush_all_output_queues()` | Get all results from all queues |
| `try_get_output(name)` | Non-blocking single result |
| `has_output(name)` | Check if results exist |
| `output_count(name)` | Count available results |
| `list_output_queues()` | List queue names |

Most users only need `flush_output_queue()`. The rest are useful for streaming/polling patterns but clutter the API surface for beginners.

### Error Handling: Too Many Paths

Users need to understand four separate error mechanisms:

1. **Exception propagation** (default) — `execute_epoch()` raises on failure
2. **Exception queue** — `propagate_exceptions=False` queues errors; retrieve via `net.exception_queue` and `net.propagate_exceptions()`
3. **Dead letter queue** — `net.dead_letter_queue` stores all failed epochs with full context (packets, retry history, worker info)
4. **on_failure callback** — per-node callback invoked on each failure attempt

When should a user use which? Not documented clearly. The 04_error_handling sample covers all four but doesn't explain the mental model for choosing between them. In practice:

- Most users want #1 (exception propagation) — it's the default and works
- Power users want #3 (dead letter queue) for post-mortem analysis
- #2 and #4 are specialized patterns for resilient pipelines

### Logging: Too Many Methods

8+ methods for log retrieval:
```python
net.get_epoch_log(epoch_id)
net.get_node_logs(node_name)
net.get_all_logs()
net.get_all_logs_chronological()
net.print_epoch_logs(epoch_id)
net.print_node_logs(node_name)
net.print_all_logs()
# Plus list_epoch_log_ids(), list_node_log_names()
```

This is a case where API surface expanded organically. A single query interface would serve better:
```python
net.logs(node="X")           # by node
net.logs(epoch=epoch_id)     # by epoch
net.logs(chronological=True) # all, sorted
```

**Verdict: 6/10.** Each individual method is well-designed, but the aggregate API is overwhelming. Users face a "which method do I use?" problem at every turn.

---

## 5. CONCEPT LADDER

### Minimum to Run a Basic Flow (~5 concepts)

1. Functions become nodes (function factory)
2. Configs declare the graph (nodes + edges)
3. Data injection (`inject_data`)
4. The execution loop (run_until_blocked + get_startable + execute_epoch)
5. Output queues (`flush_output_queue`)

**Time to productivity: ~2-3 hours**

### Intermediate Usage (~10 concepts)

6. Pools (thread, multiprocess, main)
7. Node variables (`ctx.vars`)
8. Error handling (retries, propagation, dead letter)
9. Print/log capture
10. Execution config (timeouts, rate limiting, max_epochs)

**Time to productivity: ~1 day**

### Advanced Usage (~15+ concepts)

11. Custom salvo conditions
12. Signals and controls
13. Batch processing
14. Subgraphs
15. Caching and file storage
16. Dependency requests
17. Structured logging

**Time to productivity: ~2-3 days**

The ladder is steep. Features are well-implemented individually but scattered across 12 sample projects with no progressive tutorial that builds from simple to complex.

---

## 6. ASYNC BARRIER

The entire Net API is async. There is no sync execution path for the core loop. This means:

- Every script needs `asyncio.run(main())`
- Users must understand `async with`, `await`, and async context managers
- Jupyter requires special handling (it already has a running event loop)
- Integration with sync codebases requires wrapping in `asyncio.run()` or event loop gymnastics

For a library whose primary audience writes data processing functions (often in sync Python), this is a meaningful barrier. The node functions themselves can be sync — the factory handles the async wrapping — but the orchestration layer is exclusively async.

**Verdict:** Justified technically (pools, I/O, concurrency), but a sync convenience wrapper would significantly lower the barrier to entry.

---

## 7. DEBUGGING EXPERIENCE

### What's Excellent

- **Timestamps on everything.** Print statements, log entries, epoch start/end — all UTC timestamped at the moment they occur.
- **Full tracebacks in EpochLog.** When a node fails, the complete stack trace is preserved.
- **Rich context on failures.** EpochError includes node_name, epoch_id, pool_id, worker_id, retry_count, retry_timestamps, retry_exceptions.
- **Dead letter queue preserves inputs.** Failed epochs store their input packets, enabling replay.
- **Real-time callbacks.** `on_epoch_end` fires immediately, enabling live monitoring.

### What's Missing

- **No deadlock detection.** If packets are stuck at input ports because salvo conditions aren't met, there's no warning. Users must manually inspect `get_startable_epochs()` and wonder why it's empty.
- **No "why didn't this epoch fire?" diagnostics.** When a salvo condition isn't satisfied, there's no way to ask "what's missing?" Users must mentally evaluate the condition tree against the current port state.
- **Silent cache misses.** Users must call `cache_stats()` to confirm caching is working. No log/warning when a cache lookup fails.
- **`retain_epoch_logs` is off by default.** Users debugging a failure discover after the fact that they needed this flag enabled.

**Verdict: 7/10.** When errors occur, the information is rich and well-structured. The gap is in proactive diagnostics — the system doesn't help users understand *why* something didn't happen.

---

## 8. THE BIMODAL EXPERIENCE

A key observation: netrun has **two user experiences**:

**With netrun-ui:** Config is visual. Drag nodes, draw edges, configure in panels. The verbose JSON is generated, not hand-written. UI metadata makes sense. The execution loop is hidden by the UI's run controls.

**Without netrun-ui (CLI/script):** Config is hand-written. Every edge is 4 lines of JSON. The execution loop is manual. UI metadata is noise.

Most of the friction documented here applies to the **CLI/script experience**. The UI experience is presumably much smoother. This suggests netrun was designed UI-first, with the programmatic API as a secondary concern.

This is fine — but it means the programmatic API has ergonomic debt that should be addressed for users who prefer code-first workflows or need to integrate netrun into larger systems.

---

## 9. RECOMMENDATIONS (Prioritized)

### High Impact, Low Effort

1. **Update all sample projects to use `await net.run_until_blocked()`** — The one-liner already works and is the correct simple path. The manual loop should only appear in examples that actually need fine-grained epoch control. This is the single highest-impact change.

2. **Add a sync convenience wrapper** — `Net.run_sync(config, inputs={"node.port": values})` that wraps `asyncio.run()` for simple scripts.

3. **Default `retain_epoch_logs=True`** — Users debugging failures shouldn't have to discover this flag after the fact. The memory cost is minimal for typical flows.

### High Impact, Medium Effort

4. **Add a Python-native config builder** — Let users build configs in code without JSON/TOML:
   ```python
   net = Net.from_functions(
       nodes={"double": double, "add": add},
       edges=[("double.out", "add.a")],
       output_queues={"results": "add.out"},
   )
   ```

5. **Add deadlock/stall diagnostics** — When `get_startable_epochs()` returns empty but packets exist in the net, log a warning showing where packets are stuck and which salvo conditions are unsatisfied.

6. **Consolidate log/output/error methods** — Replace the 8+ log methods and 6 output methods with query-style interfaces:
   ```python
   net.logs(node="X", chronological=True)
   net.outputs("queue_name")
   ```

### Medium Impact

7. **Add edge shorthand in TOML** — Support `edge = "node_a.out -> node_b.in"` as sugar.

8. **Promote TOML over JSON** — Make TOML the recommended config format in docs and samples. It's significantly more readable.

9. **Add factory arg discovery to configs** — Support a `$schema` reference or `netrun schema generate` command for IDE autocomplete.

10. **Document the error handling mental model** — A single page explaining when to use propagation vs. exception queue vs. dead letter queue vs. on_failure callback.

---

## Summary Scorecard

| Dimension | Score | Notes |
|-----------|-------|-------|
| Writing node functions | 9/10 | Excellent. Plain Python, minimal ceremony. |
| Config authoring (by hand) | 5/10 | Verbose, low discoverability, no IDE support. |
| Config authoring (with UI) | 8/10 | Presumably much better (visual editing). |
| Execution loop | 7/10 | API is good (`run_until_blocked()`), but samples teach the verbose pattern. |
| Output retrieval | 6/10 | Works but too many methods. |
| Error handling | 6/10 | Rich data, but too many overlapping mechanisms. |
| Debugging | 7/10 | Great post-mortem info, weak proactive diagnostics. |
| Concept ladder | 6/10 | Well-implemented features, steep learning curve. |
| Async barrier | 5/10 | Justified but no sync escape hatch. |
| **Overall developer experience** | **7/10** | Strong foundation, needs better samples and a sync wrapper. |

## Bottom Line

netrun's interface has a **strong core** (function factory, type system, execution model) that's better than it appears from the samples. The `run_until_blocked()` one-liner already works as a "run to completion" — the samples just don't show it. The real gaps are: verbose configs with low discoverability, no sync convenience wrapper, overlapping output/error/log APIs, and the async-only barrier. A Python config builder, sync wrapper, and consolidated query APIs would round out the ergonomics. But the single highest-impact change is just **fixing the sample projects** to demonstrate the simple path.
