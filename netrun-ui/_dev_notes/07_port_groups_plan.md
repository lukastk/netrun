# Port Groups Implementation Plan

## Overview

Port groups allow nodes with many ports to visually collapse related ports into a single group row. Ports are grouped by dot-separated naming convention (e.g., `my_group.port1`, `my_group.port2`). Nested groups are supported (e.g., `a.b.c` → group `a` containing subgroup `b` containing port `c`).

Groups can be collapsed (showing a single group handle) or expanded (showing individual port handles). When collapsed, dragging a connection from one group handle to another compatible group handle creates edges for all matching sub-ports.

**This is purely a UI feature** — the underlying data model (`inPorts`/`outPorts` arrays with dot-separated names) is unchanged.

## Design Details

### Grouping Logic

- Port names containing dots are split into segments: `a.b.c` → path `["a", "b"]`, leaf name `"c"`
- Ports without dots are standalone (no group)
- Groups can be nested: `a.b.x`, `a.b.y`, `a.c.z` → group `a` contains subgroup `b` (with ports `x`, `y`) and subgroup `c` (with port `z`)
- The grouping tree is computed purely from port names — no new data structures in the config

### Collapse/Expand State

- **Auto-collapse threshold**: Groups with 3+ ports start collapsed by default; groups with fewer start expanded
- Collapse state is **ephemeral UI state** (not saved to file)
- Stored in a Svelte store: `portGroupState` — a `Map<string, boolean>` keyed by `${nodeId}:${side}:${groupPath}`
- Clicking the chevron on a group header toggles its collapsed state

### Rendering

When a group is **collapsed**:
- Show a single row: `▶ group_name (N)` with a group handle
- The group handle has id `group:in:path.to.group` or `group:out:path.to.group`
- Individual port handles are still rendered in the DOM but **hidden via CSS** (`opacity: 0; width: 0; height: 0; pointer-events: none`) so that existing edges still have valid handle targets and render correctly (option b)

When a group is **expanded**:
- Show the group header: `▼ group_name` (no handle on the header)
- Show individual ports indented below, each with their normal handle
- The group handle is hidden (but remains in DOM for the same reason)

### Group Handle Appearance

- Group handles are slightly larger than regular handles (12×12 vs 10×10)
- Use a slightly different shape or style — a rounded rectangle instead of circle, or double-ring
- Same color scheme as regular handles (green for input, orange for output)

### Group-to-Group Connections

When the user drags from a group handle to another group handle:

1. `onbeforeconnect` on `<SvelteFlow>` intercepts the connection
2. Parse both handle IDs to extract group paths
3. Look up the ports in each group (recursively collecting all leaf ports)
4. Check compatibility: source group's leaf port suffixes must exactly match target group's leaf port suffixes (same names, same count)
5. If compatible: create individual edges for each matched leaf port pair, return `false` to suppress the default single edge
6. If incompatible: the connection is rejected

Connection from a group handle to a non-group handle (or vice versa) is **rejected** via `isValidConnection`.

### Validation During Drag

- `isValidConnection` is updated to understand group handles
- When hovering a group handle during drag, it checks compatibility and shows valid/invalid state
- This gives visual feedback before the user releases

## Implementation Steps

### Step 1: Port Group Utility Module

**New file: `src/lib/utils/portGroups.ts`**

```typescript
// Data structures
interface PortGroupTree {
  type: 'group';
  name: string;           // segment name (e.g., "my_group")
  fullPath: string;       // full dot-joined path (e.g., "a.b")
  children: PortDisplayItem[];
  portCount: number;      // total leaf port count (recursive)
}

interface PortLeaf {
  type: 'port';
  port: PortConfig;
  depth: number;          // nesting level (0 = top-level)
}

type PortDisplayItem = PortGroupTree | PortLeaf;

// Core functions
function buildPortTree(ports: PortConfig[]): PortDisplayItem[]
function getLeafPorts(item: PortDisplayItem): PortConfig[]
function getGroupLeafSuffixes(groupPath: string, ports: PortConfig[]): string[]
function areGroupsCompatible(
  sourceNode: FlowNode, sourceGroupPath: string,
  targetNode: FlowNode, targetGroupPath: string
): boolean
function parseGroupHandleId(handleId: string): { side: 'in' | 'out'; groupPath: string } | null
function makeGroupHandleId(side: 'in' | 'out', groupPath: string): string
```

### Step 2: Port Group State Store

**New file: `src/lib/stores/portGroupStore.ts`**

```typescript
import { writable } from 'svelte/store';

// Key: "${nodeId}:${side}:${groupPath}", value: collapsed
const portGroupState = writable<Map<string, boolean>>(new Map());

function isGroupCollapsed(nodeId: string, side: 'in' | 'out', groupPath: string): boolean
function toggleGroup(nodeId: string, side: 'in' | 'out', groupPath: string): void
function setGroupCollapsed(nodeId: string, side: 'in' | 'out', groupPath: string, collapsed: boolean): void
function getDefaultCollapsed(portCount: number): boolean  // true if >= 3
```

### Step 3: Update NetrunNode.svelte

Refactor the port rendering to use the grouped tree structure.

**Changes:**
- Import `buildPortTree`, `makeGroupHandleId` from portGroups utility
- Import `isGroupCollapsed`, `toggleGroup` from portGroupStore
- Replace the flat `{#each data.inPorts as port}` with a recursive rendering of `PortDisplayItem[]`
- Create a `{#snippet}` (Svelte 5 snippet) or a sub-component for rendering a port group tree recursively
- Each group row has:
  - A chevron button (▶/▼) to toggle collapse
  - Group name and port count badge
  - A group `<Handle>` (visible when collapsed, hidden when expanded)
- Each leaf port row has:
  - The normal `<Handle>` (visible when expanded, hidden when collapsed)
  - Port name and type as before
- Hidden handles use CSS: `opacity: 0; pointer-events: none; width: 0; height: 0;` but remain in the DOM
- Handle positioning (`getHandleStyle`) needs to account for the visible items only

### Step 4: Update SubgraphNode.svelte

Apply the same port grouping treatment as NetrunNode. Since both share the same port rendering pattern, consider:
- Either extracting a shared `PortList.svelte` component used by both
- Or duplicating the logic (simpler but less DRY)

Recommendation: Extract a shared component to avoid divergence.

### Step 5: Update FlowEditor.svelte — Group Connection Logic

**Changes:**
- Add `onbeforeconnect` prop to `<SvelteFlow>`
- In the handler:
  - Check if source or target handle is a group handle (starts with `group:`)
  - If both are group handles:
    - Look up both nodes' port data
    - Check compatibility via `areGroupsCompatible()`
    - If compatible: create edges for each matched port pair using `addEdge()` from flowStore
    - Call `pushHistory()` once for the batch
    - Return `false` to suppress default edge
  - If only one is a group handle: return `false` (reject — groups only connect to groups)
  - If neither: return `connection` (normal behavior)

**Update `isValidConnection`:**
- Import group utilities
- When a group handle is being dragged, validate that the target is also a compatible group handle
- This provides real-time hover feedback

### Step 6: Update flowStore.ts — Batch Edge Addition

**Changes:**
- Add a new function `addEdges(edges: NetrunEdge[])` that adds multiple edges in one history entry
- This avoids N history entries when connecting a group of N ports
- The existing `addEdge()` remains for single connections

### Step 7: Update Sidebar.svelte — Port Editing with Groups

The sidebar port editor currently shows a flat list of ports. With port groups, users need to be able to:
- See ports organized by group (indented)
- Still edit individual port names (including their group prefix)
- Add ports with dot-separated names to create groups

**Minimal changes:**
- No structural changes needed in the sidebar initially — ports are still a flat list of names
- The dot-separated naming is just a naming convention; users type `my_group.port1` as the port name
- Future enhancement: add visual grouping in the sidebar editor too

### Step 8: Handle Edge Style for Group Handles

- Group handles get a distinct CSS class (`.group-handle`)
- Slightly larger (12×12 vs 10×10)
- Could use a rounded-square shape or double border to distinguish from regular handles

## File Changes Summary

| File | Action | Description |
|------|--------|-------------|
| `src/lib/utils/portGroups.ts` | **New** | Port grouping logic, compatibility checking, handle ID parsing |
| `src/lib/stores/portGroupStore.ts` | **New** | Collapse/expand state management |
| `src/lib/components/NetrunNode.svelte` | **Modify** | Grouped port rendering with collapse/expand |
| `src/lib/components/SubgraphNode.svelte` | **Modify** | Same grouped port rendering |
| `src/lib/components/PortList.svelte` | **New** (optional) | Shared port rendering component for both node types |
| `src/lib/components/FlowEditor.svelte` | **Modify** | Add `onbeforeconnect` for group connections, update `isValidConnection` |
| `src/lib/stores/flowStore.ts` | **Modify** | Add `addEdges()` batch function, update `isValidConnection` for groups |

## Verification

1. Create a test node with ports: `a.x`, `a.y`, `a.z`, `b`, `c.d.e`, `c.d.f`, `c.g`
   - Should show groups `a` (3 ports, collapsed), `c` (3 ports, collapsed), and standalone port `b`
   - Group `c` when expanded shows subgroup `d` (2 ports) and standalone `g`
2. Toggle collapse/expand on groups — handles should appear/disappear correctly
3. Existing edges to grouped ports should render correctly in both collapsed and expanded states
4. Drag from collapsed group handle on one node to compatible group handle on another — should create all matching edges
5. Drag from group handle to incompatible group handle — should show invalid and reject
6. Drag from group handle to individual port handle — should reject
7. Undo after a group connection should remove all the edges created in that group connection
