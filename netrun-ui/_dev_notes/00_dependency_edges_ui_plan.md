# Dependency Edges & Packet Requests UI Plan

## Overview

Add full visual editing support for dependency edges and packet requests to netrun-ui. This involves changes across the full stack: backend converter, frontend stores, edge rendering, node properties, and a new graph-level sidebar section.

## Current State

### What exists (backend/runtime)
- `EdgeConfig.dependency: bool = False` — marks an edge as a dependency edge
- `NodeConfig.dependency_request: DependencyRequestConfig | None` — auto-request config per node
  - `triggers: list["on_startup" | "on_no_salvo_triggered"]` (default: `["on_startup"]`)
  - `label: str` (default: `"main"`)
- `Net.request(node_name, label)` — manual request API
- Full cascade/resolution in netrun-sim (backward BFS, source node activation, label dedup)

### What exists (UI)
- Edges have no `data` field — they are plain SvelteFlow edges (`{id, source, target, sourceHandle, targetHandle, type}`)
- No edge properties panel in the sidebar — selecting an edge does nothing in the sidebar
- No context menu for edges
- `selectedEdgeIds` store exists but is only used for visual selection highlight
- Node `_config` bag preserves unknown fields through round-trips (so `dependency_request` already survives if set in JSON, it just can't be edited)
- The backend converter (`converter.py`) does NOT preserve edge `dependency` field:
  - `graph_config_to_ui()` drops it (not included in `ui_edge`)
  - `ui_to_graph_config()` creates `EdgeConfig` without it
  - **This is the first bug to fix**

## Implementation Plan

### Step 1: Backend — Preserve `dependency` field on edges

**File: `netrun_ui_backend/converter.py`**

#### `graph_config_to_ui()` — read dependency from config
```python
ui_edge = {
    "id": f"edge-{i}",
    "source": source_id,
    "target": target_id,
    "sourceHandle": source_port,
    "targetHandle": target_port,
    "type": "smoothstep",
}
# NEW: preserve dependency flag
if edge.get("dependency", False):
    ui_edge["data"] = {"dependency": True}
```

#### `ui_to_graph_config()` — write dependency back
```python
config_edges.append(_EdgeConfig(
    source_str=f"{source_name}.{source_handle}",
    target_str=f"{target_name}.{target_handle}",
    dependency=edge.get("data", {}).get("dependency", False),  # NEW
))
```

### Step 2: Frontend Store — Edge data type and helpers

**File: `src/lib/stores/flowStore.ts`**

#### Add edge data interface
```typescript
export interface NetrunEdgeData extends Record<string, unknown> {
    dependency?: boolean;
}
export type NetrunEdge = Edge<NetrunEdgeData>;
```

Note: Currently `NetrunEdge = Edge` (no data). Changing it to `Edge<NetrunEdgeData>` lets us store the dependency flag per edge.

#### Add edge update helper
```typescript
export function updateEdgeData(edgeId: string, data: Partial<NetrunEdgeData>) {
    const tab = get(activeTab);
    if (!tab) return;
    pushHistory();
    const edges = tab.edges.map(e => {
        if (e.id !== edgeId) return e;
        return { ...e, data: { ...e.data, ...data } };
    });
    updateActiveTab({ edges });
}

export function toggleEdgeDependency(edgeId: string) {
    const tab = get(activeTab);
    if (!tab) return;
    const edge = tab.edges.find(e => e.id === edgeId);
    if (!edge) return;
    const current = edge.data?.dependency ?? false;
    updateEdgeData(edgeId, { dependency: !current });
}
```

#### Add derived stores for dependency analysis
```typescript
// All dependency edges in the current graph
export const dependencyEdges = derived(edges, ($edges) =>
    $edges.filter(e => e.data?.dependency)
);

// All unique dependency labels in the graph (from node configs)
export const dependencyLabels = derived(nodes, ($nodes) => {
    const labels = new Map<string, string[]>(); // label -> node names
    for (const node of $nodes) {
        const depReq = (node.data._config as any)?.dependency_request;
        if (depReq?.label) {
            const existing = labels.get(depReq.label) || [];
            existing.push(node.data.label);
            labels.set(depReq.label, existing);
        }
    }
    return labels;
});
```

#### Preserve edge data in `convertApiEdges`
```typescript
function convertApiEdges(apiEdges: UIEdge[]): NetrunEdge[] {
    return apiEdges.map(edge => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
        sourceHandle: edge.sourceHandle,
        targetHandle: edge.targetHandle,
        type: edge.type || 'smoothstep',
        data: edge.data,  // NEW: preserve data (includes dependency)
    }));
}
```

### Step 3: Edge Visual Styling — Dependency edges look different

**File: `src/lib/components/FlowEditor.svelte`**

In the `edgesWithSelection` derived store, apply visual differentiation for dependency edges:

```typescript
const edgesWithSelection = derived(
    [expandedView, selectedEdgeIds, edgeStyle, edgeMarkers],
    ([{ allEdges }, $selectedEdgeIds, $edgeStyle, $edgeMarkers]) => {
        const markers = getMarkers($edgeMarkers);
        return allEdges.map(edge => {
            const isDep = edge.data?.dependency === true;
            return {
                ...edge,
                type: $edgeStyle,
                markerStart: markers.markerStart,
                markerEnd: markers.markerEnd,
                selected: $selectedEdgeIds.has(edge.id),
                // Dependency edges: dashed stroke + different color
                style: isDep
                    ? 'stroke-width: 2px; stroke: #a78bfa; stroke-dasharray: 6 3;'
                    : 'stroke-width: 2px;',
                animated: isDep ? true : false,  // subtle animation for dep edges
            };
        });
    }
);
```

Design choice: Dependency edges use a **purple dashed line** (`#a78bfa` — tailwind violet-400) with subtle animation. This makes them instantly distinguishable from regular edges (solid white/gray) without being overwhelming.

### Step 4: Edge Properties Panel — Show when edge is selected

**File: `src/lib/components/Sidebar.svelte`**

Add a new branch in the sidebar content area. Currently the sidebar shows:
- Node selected → node properties
- Multi-node selected → multi-node properties
- Nothing selected → net-level settings

Add a new case: **edge selected** (between node and multi-node):

```svelte
{:else if selectedEdge}
    <!-- Edge Properties -->
    <EdgeProperties edge={selectedEdge} />
```

**New file: `src/lib/components/EdgeProperties.svelte`**

```svelte
<script lang="ts">
    import { toggleEdgeDependency, nodes, edges, pushHistory } from '$lib/stores/flowStore';
    import type { NetrunEdge } from '$lib/stores/flowStore';

    interface Props {
        edge: NetrunEdge;
    }
    let { edge }: Props = $props();

    let isDependency = $derived(edge.data?.dependency ?? false);

    // Find source and target node names for display
    let sourceName = $derived(/* find node label from edge.source */);
    let targetName = $derived(/* find node label from edge.target */);
</script>

<section class="section">
    <div class="section-content">
        <h3>Edge</h3>
        <p class="edge-endpoints">{sourceName}.{edge.sourceHandle} → {targetName}.{edge.targetHandle}</p>

        <div class="field">
            <label>
                <input type="checkbox"
                    checked={isDependency}
                    onchange={() => toggleEdgeDependency(edge.id)}
                />
                Dependency Edge
            </label>
            <p class="field-help">
                Dependency edges participate in backward request cascades.
                When a downstream node needs data, the request propagates
                backward through dependency edges to find source nodes.
            </p>
        </div>
    </div>
</section>
```

#### Derive `selectedEdge` in Sidebar

```typescript
import { selectedEdgeIds, edges } from '$lib/stores/flowStore';

const selectedEdge = derived(
    [edges, selectedEdgeIds],
    ([$edges, $ids]) => {
        if ($ids.size !== 1) return null;
        const id = [...$ids][0];
        return $edges.find(e => e.id === id) ?? null;
    }
);
```

### Step 5: Node Dependency Request Config in Sidebar

**File: `src/lib/components/Sidebar.svelte`** (or new `DependencyRequestSection.svelte`)

Add a new collapsible section in the node properties area, shown when the selected node has any incoming dependency edges:

```
sectionsOpen.dependencyRequest: false,
```

**New file: `src/lib/components/DependencyRequestSection.svelte`**

This section shows:
- **Triggers** — multi-select checkboxes: `on_startup`, `on_no_salvo_triggered`
- **Label** — text input for the request label
- **"Remove config"** button to clear dependency_request (sets to null)
- **"Add config"** button when the node has dep edges but no config

The config is stored in `node.data._config.dependency_request`. Edits update `_config` via `updateNodeDataLive`.

```svelte
<section class="section">
    <button class="section-header" onclick={() => toggleSection('dependencyRequest')}>
        <span class="section-title">Dependency Request</span>
        <span class="section-toggle">{sectionsOpen.dependencyRequest ? '−' : '+'}</span>
    </button>
    {#if sectionsOpen.dependencyRequest}
        <div class="section-content">
            {#if depConfig}
                <!-- Triggers -->
                <div class="field">
                    <label>Triggers</label>
                    <label class="checkbox-label">
                        <input type="checkbox"
                            checked={depConfig.triggers.includes('on_startup')}
                            onchange={...}
                        />
                        on_startup
                    </label>
                    <label class="checkbox-label">
                        <input type="checkbox"
                            checked={depConfig.triggers.includes('on_no_salvo_triggered')}
                            onchange={...}
                        />
                        on_no_salvo_triggered
                    </label>
                </div>

                <!-- Label -->
                <div class="field">
                    <label for="dep-label">Label</label>
                    <input id="dep-label" type="text"
                        value={depConfig.label}
                        oninput={updateLabel}
                        onblur={onFieldBlur}
                    />
                </div>

                <button class="danger-btn" onclick={removeDependencyRequest}>
                    Remove Config
                </button>
            {:else}
                <p>No dependency request configured.</p>
                {#if hasDependencyEdges}
                    <button class="add-btn" onclick={addDependencyRequest}>
                        Add Dependency Request
                    </button>
                {/if}
            {/if}
        </div>
    {/if}
</section>
```

Visibility: Only show this section if the node has at least one incoming dependency edge, OR if it already has a `dependency_request` config.

### Step 6: Dependency Labels Overview in Graph Sidebar

**New file: `src/lib/components/DependencyLabelsSection.svelte`**

Add to the graph-level sidebar (shown when no node is selected), as a new collapsible section:

```
sectionsOpen.dependencyLabels: false,
```

This section shows all unique dependency labels across the graph with the nodes that use each label:

```svelte
<section class="section">
    <button class="section-header" onclick={() => toggleSection('dependencyLabels')}>
        <span class="section-title">Dependency Labels</span>
        <span class="section-toggle">{sectionsOpen.dependencyLabels ? '−' : '+'}</span>
    </button>
    {#if sectionsOpen.dependencyLabels}
        <div class="section-content">
            {#if $dependencyLabels.size === 0}
                <p class="empty-message">No dependency labels in this graph.</p>
            {:else}
                {#each [...$dependencyLabels.entries()] as [label, nodeNames]}
                    <div class="label-group">
                        <div class="label-header">
                            <input type="text"
                                value={label}
                                onblur={(e) => renameLabel(label, e.target.value)}
                                class="label-input"
                            />
                            <span class="node-count">{nodeNames.length} node{nodeNames.length > 1 ? 's' : ''}</span>
                        </div>
                        <ul class="label-nodes">
                            {#each nodeNames as name}
                                <li>
                                    <button class="node-link" onclick={() => selectAndFocusNode(name)}>
                                        {name}
                                    </button>
                                </li>
                            {/each}
                        </ul>
                    </div>
                {/each}
            {/if}
        </div>
    {/if}
</section>
```

#### `renameLabel` function

When user edits a label, update ALL nodes that had the old label:

```typescript
function renameLabel(oldLabel: string, newLabel: string) {
    if (!newLabel || newLabel === oldLabel) return;
    const tab = get(activeTab);
    if (!tab) return;
    pushHistory();

    for (const node of tab.nodes) {
        const config = node.data._config as any;
        if (config?.dependency_request?.label === oldLabel) {
            updateNodeDataLive(node.id, {
                _config: {
                    ...config,
                    dependency_request: {
                        ...config.dependency_request,
                        label: newLabel,
                    },
                },
            });
        }
    }
    pushHistory();
}
```

#### `selectAndFocusNode` function

Click a node name → select it, scroll it into view:

```typescript
function selectAndFocusNode(name: string) {
    selectNodeByName(name);
    // Use svelteFlowRef to fitView on the selected node
}
```

### Step 7: Dependency Edge Click — Highlight Upstream Source Nodes

This is the headline interactive feature. When a user clicks on a dependency edge, the UI:
1. Highlights all upstream source nodes (the nodes that would be activated by a request cascade)
2. Shows a warning badge on source nodes that have no input salvo condition satisfiable with zero packets

#### Startability Check

A source node is "startable" if it has at least one input salvo condition whose term can evaluate to true with all ports empty. In practice:
- **No input ports** → the function factory generates an always-true salvo (`term: {type: "true"}`) → always startable, no warning
- **Has input ports** → check `_config.in_salvo_conditions`: if every condition's term requires non-empty ports (e.g., `port_state` with `non_empty`), the node is **not startable** → show warning
- **Has a `{type: "true"}` term** in any salvo condition → startable, no warning
- **Factory node with no resolved salvos** → can't determine, skip warning (optimistic)

This mirrors the netrun-sim validation in the design doc (section 7.1): "If the cascade reaches a source node that has no input salvo condition satisfiable with zero packets, a NetError is raised."

#### Approach: Client-side BFS

The cascade BFS is simple enough to implement in the frontend without calling the backend. The algorithm:

1. Starting from the selected dependency edge's target port, walk backward through all edges
2. At each node, follow ALL input ports backward (not just dependency edges — the cascade traverses all edges after initiation)
3. Nodes with no incoming edges are source nodes
4. For each source node, check if any input salvo condition has a term that is satisfiable with zero packets — if none do, mark as unstartable

**New file: `src/lib/utils/dependencyAnalysis.ts`**

```typescript
import type { NetrunEdge, FlowNode } from '$lib/stores/flowStore';

export interface CascadeResult {
    sourceNodes: string[];          // Node IDs that are source nodes
    visitedNodes: string[];         // All node IDs in the cascade path
    visitedEdges: string[];         // All edge IDs traversed
    unstartableNodes: string[];     // Source nodes with no salvo satisfiable at zero packets
}

/**
 * Check if a source node has at least one input salvo condition that is
 * satisfiable with zero packets (i.e., the term doesn't require any port
 * to be non-empty).
 *
 * A term of type "true" is always satisfiable.
 * A term that references port states (non_empty, equals, etc.) is not
 * satisfiable when all ports are empty.
 * An "and" term is satisfiable only if ALL sub-terms are satisfiable.
 *
 * For factory nodes without resolved salvo conditions, we return true
 * (optimistic — can't determine without resolution).
 */
function isStartableWithZeroPackets(node: FlowNode): boolean {
    const config = node.data._config as Record<string, unknown> | undefined;
    const salvos = config?.in_salvo_conditions as Record<string, unknown> | undefined;

    // No salvo conditions in config — factory nodes get them at resolve time.
    // Nodes with no input ports get an always-true salvo from the factory.
    if (!salvos) {
        // If the node has no input ports, it's always startable
        return node.data.inPorts.length === 0;
    }

    // Check each salvo condition
    for (const salvo of Object.values(salvos)) {
        const s = salvo as Record<string, unknown>;
        if (isTermSatisfiableEmpty(s.term as Record<string, unknown>)) {
            return true;
        }
    }
    return false;
}

/**
 * Recursively check if a salvo condition term is satisfiable when all
 * ports are empty (zero packets).
 */
function isTermSatisfiableEmpty(term: Record<string, unknown>): boolean {
    if (!term) return false;
    const type = term.type as string;

    if (type === 'true') return true;

    // Port state checks (non_empty, equals, greater_than, etc.)
    // are never satisfied when the port has 0 packets
    if (type === 'port') return false;

    // AND: all sub-terms must be satisfiable
    if (type === 'and') {
        const terms = term.terms as Record<string, unknown>[];
        return terms.every(t => isTermSatisfiableEmpty(t));
    }

    // Unknown term type — be optimistic
    return true;
}

/**
 * Perform backward BFS from a dependency edge to find source nodes.
 * This mirrors the netrun-sim cascade_backward algorithm.
 */
export function analyzeDependencyCascade(
    edge: NetrunEdge,
    allNodes: FlowNode[],
    allEdges: NetrunEdge[],
): CascadeResult {
    const nodeMap = new Map(allNodes.map(n => [n.id, n]));

    // BFS queue: input port refs (nodeId, portName)
    const queue: Array<{nodeId: string, portName: string}> = [];
    const processedNodes = new Set<string>();
    const sourceNodes: string[] = [];
    const visitedNodes: string[] = [];
    const visitedEdges: string[] = [];
    const unstartableNodes: string[] = [];

    // Start: the target port of the clicked dependency edge
    queue.push({ nodeId: edge.target, portName: edge.targetHandle! });

    while (queue.length > 0) {
        const { nodeId, portName } = queue.shift()!;

        // Find edges incoming to this port
        const incomingEdges = allEdges.filter(
            e => e.target === nodeId && e.targetHandle === portName
        );

        if (incomingEdges.length === 0) {
            // Unconnected port — skip (or could warn)
            continue;
        }

        for (const inEdge of incomingEdges) {
            visitedEdges.push(inEdge.id);
            const upstreamId = inEdge.source;

            if (processedNodes.has(upstreamId)) continue;
            processedNodes.add(upstreamId);
            visitedNodes.push(upstreamId);

            const upstreamNode = nodeMap.get(upstreamId);
            if (!upstreamNode) continue;

            const inPorts = upstreamNode.data.inPorts || [];

            // Check if any input port has an incoming edge
            const hasAnyIncoming = inPorts.some(p =>
                allEdges.some(e => e.target === upstreamId && e.targetHandle === p.name)
            );

            if (inPorts.length === 0 || !hasAnyIncoming) {
                // Source node — cascade terminates here
                sourceNodes.push(upstreamId);

                // Check startability: does this source node have a salvo
                // condition that can fire with zero input packets?
                if (!isStartableWithZeroPackets(upstreamNode)) {
                    unstartableNodes.push(upstreamId);
                }
                continue;
            }

            // Add all input ports to BFS queue
            for (const port of inPorts) {
                queue.push({ nodeId: upstreamId, portName: port.name });
            }
        }
    }

    return { sourceNodes, visitedNodes, visitedEdges, unstartableNodes };
}
```

#### Highlighting state

**File: `src/lib/stores/flowStore.ts`**

```typescript
// Cascade highlight state (set when user clicks a dependency edge)
export const cascadeHighlight = writable<CascadeResult | null>(null);
```

#### Triggering on edge click

**File: `src/lib/components/FlowEditor.svelte`**

Add an `onedgeclick` handler to SvelteFlow:

```typescript
function onEdgeClick(event: { edge: Edge }) {
    const edge = event.edge as NetrunEdge;
    if (edge.data?.dependency) {
        const result = analyzeDependencyCascade(
            edge,
            get(allVisibleNodes),
            get(allVisibleEdges),
        );
        cascadeHighlight.set(result);
    } else {
        cascadeHighlight.set(null);
    }
}
```

Clear highlight when clicking the pane or selecting a node:

```typescript
// In onPaneClick or onSelectionChange:
cascadeHighlight.set(null);
```

#### Visual highlight on nodes and edges

**File: `src/lib/components/FlowEditor.svelte`**

Modify `edgesWithSelection` and `nodesWithSelection` to incorporate cascade highlighting:

For **edges**: Cascade-path edges get a bright glow style.

For **nodes**: Source nodes in the cascade get a special highlight. Unstartable source nodes get a warning indicator.

**File: `src/lib/components/NetrunNode.svelte`**

Add props/state for cascade highlighting:

```svelte
<div
    class="netrun-node shape-{shape}"
    class:selected
    class:cascade-source={isCascadeSource}
    class:cascade-warning={isCascadeWarning}
    class:cascade-visited={isCascadeVisited}
>
```

CSS:
```css
.netrun-node.cascade-source {
    border-color: #a78bfa;  /* violet */
    box-shadow: 0 0 8px rgba(167, 139, 250, 0.5);
}

.netrun-node.cascade-warning {
    border-color: #fbbf24;  /* amber */
    box-shadow: 0 0 8px rgba(251, 191, 36, 0.5);
}
```

For the **warning badge on unstartable source nodes**, add a small overlay:

```svelte
{#if isCascadeWarning}
    <div class="cascade-warning-badge">
        No salvo satisfiable without input
    </div>
{/if}
```

CSS:
```css
.cascade-warning-badge {
    position: absolute;
    bottom: -24px;
    left: 0;
    right: 0;
    text-align: center;
    font-size: 10px;
    color: #fbbf24;
    background: rgba(0, 0, 0, 0.7);
    padding: 2px 6px;
    border-radius: 4px;
    pointer-events: none;
}
```

### Step 8: Context Menu for Edges

**File: `src/lib/components/FlowEditor.svelte`**

Add `onedgecontextmenu` handler to SvelteFlow:

```typescript
function onEdgeContextMenu(event: { edge: Edge; event: MouseEvent }) {
    event.event.preventDefault();
    showContextMenu({
        x: event.event.clientX,
        y: event.event.clientY,
        items: [
            {
                label: edge.data?.dependency ? 'Remove Dependency' : 'Make Dependency',
                action: () => toggleEdgeDependency(edge.id),
            },
            { type: 'separator' },
            {
                label: 'Delete Edge',
                action: () => deleteEdges([edge.id]),
                danger: true,
            },
        ],
    });
}
```

### Step 9: MiniMap Color for Source Nodes in Cascade

When a cascade highlight is active, tint source nodes in the minimap to make them visible even when zoomed out:

**File: `src/lib/components/FlowEditor.svelte`**

```typescript
nodeColor={(node) => {
    if ($cascadeHighlight?.unstartableNodes.includes(node.id)) return '#fbbf24';  // amber
    if ($cascadeHighlight?.sourceNodes.includes(node.id)) return '#a78bfa';       // violet
    if (node.data?.nodeType === 'decoration') return '#6b7280';
    if (node.data?.nodeType === 'subgraph') return '#22c55e';
    if (node.data?.nodeType === 'factory') return '#7c3aed';
    return '#3b82f6';
}}
```

---

## Summary of New Files

| File | Purpose |
|------|---------|
| `src/lib/components/EdgeProperties.svelte` | Edge properties panel (dependency toggle) |
| `src/lib/components/DependencyRequestSection.svelte` | Node-level dependency request config |
| `src/lib/components/DependencyLabelsSection.svelte` | Graph-level label overview + bulk rename |
| `src/lib/utils/dependencyAnalysis.ts` | Client-side cascade BFS for highlighting |

## Summary of Modified Files

| File | Changes |
|------|---------|
| `netrun_ui_backend/converter.py` | Preserve `dependency` on edge round-trip |
| `src/lib/stores/flowStore.ts` | `NetrunEdgeData` type, edge update helpers, `cascadeHighlight` store, `dependencyLabels` derived store |
| `src/lib/components/FlowEditor.svelte` | Dependency edge styling, `onedgeclick` cascade trigger, `onedgecontextmenu`, Alt-drag for dep edges |
| `src/lib/components/Sidebar.svelte` | Edge properties branch, dependency request section, dependency labels section |
| `src/lib/components/NetrunNode.svelte` | Cascade highlight classes + warning badge |

## Implementation Order

1. **Backend converter** (Step 1) — critical bug fix, no UI dependency
2. **Frontend store types + helpers** (Step 2) — foundation for everything else
3. **Edge visual styling** (Step 3) — immediate visual payoff
4. **Edge properties panel** (Step 4) — toggle dependency on/off
5. **Node dependency request config** (Step 5) — edit triggers + label
6. **Dependency labels overview** (Step 6) — graph-level label management
7. **Cascade highlight on edge click** (Step 7) — the headline interactive feature
8. **Context menu** (Step 8) — convenience
9. **Alt-drag edge creation** (Step 9) — power user feature
10. **MiniMap polish** (Step 10) — cosmetic

Steps 1-6 are essential. Steps 7-9 add significant interactive value.

## Design Decisions

1. **Purple for dependency edges** — Distinct from blue (selection), green (input ports), amber (output ports), red (errors). Purple/violet is unused in the current palette and visually reads as "special" without being alarming.

2. **Dashed stroke** — Universal visual language for "different kind of connection". Combined with purple, makes dependency edges immediately recognizable at any zoom level.

3. **Client-side BFS for cascade analysis** — The algorithm is simple (< 50 lines) and the graph is always small enough to traverse instantly. Avoids a backend round-trip just to highlight nodes. The BFS logic mirrors `Graph.cascade_backward` from netrun-sim.

4. **Warning on unstartable source nodes** — Shown as a soft amber badge below the node, not a validation error. A source node is "unstartable" if none of its input salvo conditions can fire with zero packets (i.e., all terms require non-empty ports). This mirrors the netrun-sim runtime check — the cascade would raise `NetError` at runtime. We check the `in_salvo_conditions` terms: `{type: "true"}` is always satisfiable, `{type: "port"}` with state checks is not, and `{type: "and"}` requires all sub-terms to pass. For factory nodes without resolved salvos, we skip the warning (optimistic, since factory-generated salvos for no-input-port nodes are always-true).

5. **Label rename is bulk** — Changing a label in the graph-level section changes ALL nodes with that label. This is the intended UX: labels are a graph-wide concept, not per-node (even though they're stored per-node in the config). Individual label changes are still possible via the node-level section.

6. **Edge data bag** — Using `edge.data.dependency` follows SvelteFlow conventions for attaching custom data to edges. This is cleaner than trying to store it elsewhere and automatically participates in SvelteFlow's serialization.
