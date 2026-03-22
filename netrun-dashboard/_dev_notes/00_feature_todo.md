# netrun-dashboard Feature TODO

## Current State

The dashboard has: net list sidebar, status bar, graph view with live node status overlays (green=running, yellow=startable, dimmed=disabled), epoch table, log viewer (print buffer only), and a resizable bottom panel.

---

## 1. Layout & Panels

### 1a. Right sidebar (node detail panel)
Clicking a node in the graph opens a right sidebar showing node-level data:
- Node name, enabled state, pool assignment
- Port list (in/out) with type annotations
- Packets waiting at each input port (count + values)
- Running/startable epoch IDs
- Epoch history for this node (filtered epoch table)
- Structured logs for this node
- Cache stats (entry count, if caching enabled)
- Execution config summary (retries, timeout, type checking)

### 1b. Resizable sidebar (left)
Make the existing left sidebar (net list) resizable via drag handle.

### 1c. Resizable right sidebar
The right sidebar (1a) should be resizable via drag handle.

### 1d. Resizable bottom panel
Already implemented. Consider allowing collapse/expand via double-click on handle or a toggle button.

---

## 2. Epoch Table Enhancements

### 2a. Click epoch → highlight node in graph
Clicking an epoch row highlights the corresponding node in the graph view (e.g. blue outline or pulsing effect). Clicking again or clicking elsewhere clears the highlight.

### 2b. Click epoch → show full detail in right sidebar
Clicking an epoch row could also open the right sidebar with that node's detail, scrolled to the relevant epoch.

### 2c. Show queue_time_ms
Add a "Queue" column showing how long the epoch waited before execution started.

### 2d. Show input/output salvo info
Show `in_salvo_ports`, `in_salvo_packet_count`, `out_salvo_count` in the expanded epoch detail.

### 2e. Show orphaned/destroyed packet counts
Show `orphaned_packet_count` and `destroyed_packet_count` in expanded detail — flags data loss.

### 2f. Structured log entries per epoch
In the expanded epoch detail, show `node_log_entries` (from `ctx.log()`) as a table of timestamp + key-value fields. These are richer than print buffer messages.

### 2g. Net action trace per epoch
In the expanded epoch detail, optionally show `net_actions` — the simulation-level action trace (packets created, moved, consumed, salvos triggered). Useful for debugging flow issues.

### 2h. User fields
Show `user_fields` (merged from structured log entries) in epoch detail.

---

## 3. Log Viewer Enhancements

### 3a. Click log entry → expand structured data
Currently logs show `timestamp | node_name | message`. If the log entry has associated structured data (from `ctx.log()`), clicking it should expand to show the key-value fields.

### 3b. Structured log view
Add a separate tab or toggle in the log viewer that shows structured logs (`node_log_entries` from epoch logs) instead of/alongside the print buffer. Each entry has: timestamp, message, level (info/error), and arbitrary key-value fields.

### 3c. Click log → highlight node
Clicking a log entry highlights the corresponding node in the graph.

### 3d. Severity filtering
Filter by log level (info/error) for structured logs.

### 3e. Epoch filtering
Filter logs by epoch ID — show only logs from a specific epoch.

---

## 4. Graph View Enhancements

### 4a. Click node → open right sidebar
Clicking a node opens the right sidebar with full node detail.

### 4b. Show input port packet counts on nodes
Display small badges on input ports showing how many packets are waiting.

### 4c. Show epoch count badge on nodes
Small badge showing total epoch count or running epoch count on each node.

### 4d. Tooltip on hover
Hovering over a node shows a tooltip with quick info: name, state, epoch count, pool.

---

## 5. Net-Level Data

### 5a. Dead letter queue panel
Show `dead_letter_queue` entries — epochs that exhausted retries. Each entry has: epoch_id, node_name, error, retry_count, retry_timestamps, retry_exceptions, packets.

### 5b. Exception queue
Show `exception_queue` — exceptions from nodes with `propagate_exceptions=False`.

### 5c. Output queue viewer
Show output queue names, current packet counts, and optionally the values.

### 5d. Net action log (global)
A trace of all simulation-level actions across the whole net (packet movements, epoch transitions). Could be a separate tab in the bottom panel. Very verbose — needs filtering/pagination.

### 5e. Pool/worker topology
Show which pools exist, their type (main/thread/multiprocess/remote), number of workers, and which nodes are assigned to which pools.

### 5f. Blocked state indicator
Show in the status bar whether the net is blocked (no more progress possible without external input).

---

## 6. Observe API Enhancements

Some features above need new data in the WebSocket payload or new endpoints.

### 6a. Structured logs in WS payload
Currently the WS sends `logs` (print buffer from `get_all_logs()`). We should also send structured log entries from epoch logs, or provide a separate endpoint.

### 6b. Input port packet counts in WS payload
The WS `nodes` payload doesn't include per-port packet counts. Either add `input_port_packets: dict[str, int]` to NodeStatus, or add a new endpoint.

### 6c. Dead letter queue endpoint
Add `GET /dead-letter-queue` endpoint to ObserveServer.

### 6d. Exception queue endpoint
Add `GET /exceptions` endpoint.

### 6e. Output queue info endpoint
Add `GET /output-queues` endpoint showing queue names and counts.

### 6f. Cache stats endpoint
Add `GET /nodes/{name}/cache-stats` endpoint.

---

## 7. Controls (Inject/Control UI)

### 7a. Enable/disable node toggle
Click a button in the right sidebar or context menu to enable/disable a node.

### 7b. Inject data UI
Form to inject data into a node's input port — select node, select port, enter JSON value, submit.

### 7c. Send control signal
Form to send control signals to nodes that have control ports.

### 7d. Pause/resume net
Button in the status bar to pause/resume the net.

---

## Implementation Priority

**Phase 1 — Structured data & interaction:**
- [x] 3a (click log → expand structured data)
- [x] 2a (click epoch → highlight node)
- [x] 2f (structured log entries in epoch detail)
- [x] 2c (queue_time_ms column) — done alongside 2f
- [x] 2d (salvo info in epoch detail) — done alongside 2f
- [x] 2e (orphaned/destroyed packet counts) — done alongside 2f
- [x] 3b (structured log view in log viewer) — done alongside 3a
- [x] 3c (click log → highlight node) — done alongside 3a
- [x] 1a (right sidebar with node detail)

**Phase 2 — Richer observability:**
- [x] 4b (input port packet counts) — done in Phase 1 via NodeStatus
- [x] 5a (dead letter queue) — count shown in status bar
- [x] 5c (output queue viewer) — queue names in NetStatus
- [x] 5f (blocked state indicator) — shown in status bar
- [x] 6a (structured logs in WS) — done in Phase 1
- [x] 6b (input port packet counts in WS) — done in Phase 1

**Phase 3 — Controls:**
- 7a-7d (enable/disable, inject, control, pause/resume)

**Phase 4 — Polish:**
- 1b-1c (resizable sidebars)
- 4c-4d (node badges, tooltips)
- 5d-5e (net action log, pool topology)
- 2g (net action trace per epoch)
