# 03 — Visualization Package Extraction Plan

## Overview

Extract the graph rendering/visualization layer from `netrun-ui` into a standalone Svelte component library (`netrun-ui-vis`). This enables:

1. **Static HTML export** — Render a `.netrun.json` as an interactive (pan/zoom/select) but non-editable graph in a single `.html` file, no backend needed.
2. **Dashboard/monitoring** — Embed the viewer in a dashboard, overlay live execution state (running nodes, packet counts, errors).
3. **Simpler VS Code extension** — Embed the vis package + thin editing layer instead of the full `netrun-ui`.
4. **Docs/notebooks** — Embed netrun graph visualizations in documentation.

## Design Principles

### Props + Events, Not Stores

The vis package components must be **props-driven with event callbacks**. This is the fundamental design decision — it means consumers don't need to adopt our store architecture, and components work in any Svelte app (or even non-SvelteKit contexts).

```svelte
<!-- Read-only viewer: just pass data -->
<NetrunFlowViewer {nodes} {edges} />

<!-- Interactive: respond to events -->
<NetrunFlowViewer
  {nodes} {edges}
  onNodeClick={(e) => inspectNode(e.node)}
  onSelectionChange={(e) => updatePanel(e.nodes, e.edges)}
/>
```

The existing `netrun-ui` will wrap these components and bridge them to its store-based architecture.

### Minimal Dependencies

The vis package depends only on:
- `@xyflow/svelte` (graph rendering engine)
- `elkjs` (auto-layout, optional — only imported if layout is used)
- `svelte` (peer dependency)

No backend. No API client. No file I/O.

### CSS Custom Properties for Theming

Ship a default dark theme. Consumers override via CSS custom properties. The current codebase already uses this pattern extensively, so this is mostly formalization.

## Current State Analysis

### What's Tightly Coupled (Hard Part)

The core difficulty is that **node components import store functions for mutations**:

| Component | Store Reads | Store Mutations |
|-----------|------------|-----------------|
| `NetrunNode` | `$cascadeHighlight` | `updateNodeDimensions`, `pushHistory`, `toggleNodeDescExpanded` |
| `SubgraphNode` | (none via stores) | `updateNodeDimensions`, `pushHistory`, `toggleNodeDescExpanded`, `openSubgraphTab`, `toggleSubgraphExpansion` |
| `DecorationNode` | `getCurrentConfig` | `updateNodeDimensions`, `pushHistory` |
| `PortList` | (via props) | `toggleNodePortGroup` |

Every node component calls `updateNodeDimensions` (on resize) and `pushHistory` (after resize). These are **editing concerns**, not visualization concerns.

### What's Already Clean (Easy Part)

These modules have zero backend dependencies and are purely computational:

| Module | LOC | Notes |
|--------|-----|-------|
| `portGroups.ts` | 355 | 18/18 exports are pure functions |
| `dependencyAnalysis.ts` | 118 | 2/2 exports are pure functions |
| `autoLayout.ts` | 201 | Pure computation (ELK.js) |
| `salvoParser.ts` | 616 | Pure parsing |
| `salvoSerializer.ts` | 485 | Pure serialization |
| `portGroupStore.ts` | 53 | 3/3 exports are pure functions (misnamed as "store") |
| `constants.ts` | 5 | Constants only |
| `types/salvoConditions.ts` | 291 | Types + helper functions |

### Data Model

The data types are split across two files today:

- **`api.ts`**: `UINode`, `UIEdge`, `PortInfo`, `UINodeData` — the API transport types
- **`flowStore.ts`**: `NetrunNodeData`, `SubgraphNodeData`, `DecorationNodeData`, `PortConfig`, `NetrunEdgeData`, `NodeShape`, `DecorationType` — the internal rich types

The vis package needs the **internal rich types** (they carry all visual information). The API transport types stay in `netrun-ui`.

## Package Structure

```
netrun-ui-vis/
├── package.json
├── svelte.config.js
├── tsconfig.json
├── vite.config.ts            # Library mode
├── src/
│   ├── index.ts              # Public API
│   │
│   ├── types/
│   │   ├── index.ts
│   │   ├── nodes.ts          # PortConfig, NetrunNodeData, SubgraphNodeData,
│   │   │                     # DecorationNodeData, AnyNodeData, NodeShape,
│   │   │                     # DecorationType, + constant arrays
│   │   ├── edges.ts          # NetrunEdgeData
│   │   ├── graph.ts          # NetrunGraph (the top-level data structure)
│   │   ├── events.ts         # Event callback types
│   │   └── salvoConditions.ts
│   │
│   ├── components/
│   │   ├── index.ts
│   │   ├── NetrunFlowViewer.svelte    # Top-level component
│   │   ├── NetrunNode.svelte          # Regular/factory node (props-only)
│   │   ├── SubgraphNode.svelte        # Subgraph node (props-only)
│   │   ├── DecorationNode.svelte      # Decoration node (props-only)
│   │   └── PortList.svelte            # Port rendering (props-only)
│   │
│   ├── utils/
│   │   ├── index.ts
│   │   ├── autoLayout.ts
│   │   ├── portGroups.ts
│   │   ├── portGroupCollapse.ts       # Extracted from portGroupStore (pure fns)
│   │   ├── dependencyAnalysis.ts
│   │   ├── salvoParser.ts
│   │   └── salvoSerializer.ts
│   │
│   ├── constants.ts
│   └── theme.css             # Default dark theme variables
```

### Top-Level Data Structure

The vis package defines a `NetrunGraph` type that carries everything needed to render:

```typescript
/** Everything needed to render a netrun graph. No backend, no stores. */
export interface NetrunGraph {
  nodes: NetrunFlowNode[];       // Positioned nodes with full data
  edges: NetrunFlowEdge[];       // Edges with dependency flags
  settings?: GraphSettings;      // Visual settings (edge style, zoom, fonts)
}

export interface GraphSettings {
  edgeStyle?: 'smoothstep' | 'straight' | 'simplebezier' | 'bezier';
  edgeMarkers?: 'arrow-end' | 'arrow-start' | 'arrow-both' | 'none';
  minZoom?: number;
  maxZoom?: number;
  nodeTitleFontSize?: number;
  nodeDescFontSize?: number;
  nodePortFontSize?: number;
}
```

This is what consumers construct and pass to `<NetrunFlowViewer>`.

### Event Callbacks

```typescript
export interface FlowViewerEvents {
  /** Node was clicked */
  onNodeClick?: (event: { node: NetrunFlowNode; nativeEvent: MouseEvent }) => void;
  /** Node was double-clicked */
  onNodeDoubleClick?: (event: { node: NetrunFlowNode; nativeEvent: MouseEvent }) => void;
  /** Edge was clicked */
  onEdgeClick?: (event: { edge: NetrunFlowEdge; nativeEvent: MouseEvent }) => void;
  /** Selection changed */
  onSelectionChange?: (event: { nodes: NetrunFlowNode[]; edges: NetrunFlowEdge[] }) => void;
  /** Node was resized (drag ended) */
  onNodeResize?: (event: { nodeId: string; width: number; height: number }) => void;
  /** Nodes were dragged to new positions */
  onNodeDragStop?: (event: { nodes: Array<{ id: string; position: { x: number; y: number } }> }) => void;
  /** Connection was created between ports */
  onConnect?: (event: { source: string; target: string; sourceHandle: string; targetHandle: string }) => void;
  /** Nodes/edges were deleted */
  onDelete?: (event: { nodeIds: string[]; edgeIds: string[] }) => void;
  /** Context menu opened on node */
  onNodeContextMenu?: (event: { node: NetrunFlowNode; nativeEvent: MouseEvent }) => void;
  /** Context menu opened on edge */
  onEdgeContextMenu?: (event: { edge: NetrunFlowEdge; nativeEvent: MouseEvent }) => void;
  /** Port group was toggled */
  onPortGroupToggle?: (event: { nodeId: string; side: 'in' | 'out'; groupPath: string }) => void;
  /** Description expand/collapse was toggled */
  onDescriptionToggle?: (event: { nodeId: string }) => void;
}
```

### NetrunFlowViewer Props

```typescript
interface NetrunFlowViewerProps extends FlowViewerEvents {
  /** The graph data to render */
  graph: NetrunGraph;

  /** Which nodes are currently selected (controlled mode) */
  selectedNodeIds?: Set<string>;
  /** Which edges are currently selected (controlled mode) */
  selectedEdgeIds?: Set<string>;

  /** Cascade highlighting state (which nodes/edges to highlight) */
  cascadeHighlight?: CascadeHighlightState | null;

  /** Signal port configuration (prefix, suffix, types) — for port decoration */
  signalConfig?: { prefix: string; suffix: string; types: string[] };
  /** Control port configuration — for port decoration */
  controlConfig?: { prefix: string; suffix: string; types: string[] };

  /** Whether editing is enabled (connections, deletions, drag, resize) */
  editable?: boolean;  // default: false

  /** Whether to show the minimap */
  showMinimap?: boolean;  // default: true
  /** Whether to show controls (zoom buttons) */
  showControls?: boolean;  // default: true
  /** Whether to show the background grid */
  showBackground?: boolean;  // default: true

  /** Custom context menu items for nodes */
  nodeContextMenuItems?: ContextMenuItem[];
  /** Custom context menu items for edges */
  edgeContextMenuItems?: ContextMenuItem[];
}
```

When `editable` is `false` (the default), the viewer is read-only: no connections, no deletions, no resize. Consumers can still respond to clicks and selection. When `editable` is `true`, the viewer allows connections, deletions, drag, and resize — and emits events for each.

## Implementation Phases

### Phase 0: Scaffold the Package [DONE]

Create the `netrun-ui-vis/` directory as a sibling to `netrun-ui/` (not inside it). Set up:

- `package.json` with `svelte`, `@xyflow/svelte`, `elkjs` dependencies
- `vite.config.ts` in library mode (`build.lib`)
- `svelte.config.js` for component preprocessing
- `tsconfig.json` extending SvelteKit conventions

The package should be consumable via:
- Direct file reference from `netrun-ui` (workspace/path dependency during development)
- npm publish for external consumers

**Files created:**
- `netrun-ui-vis/package.json`
- `netrun-ui-vis/vite.config.ts`
- `netrun-ui-vis/svelte.config.js`
- `netrun-ui-vis/tsconfig.json`
- `netrun-ui-vis/src/index.ts`

### Phase 1: Extract Types and Constants [DONE]

Move type definitions and constants into the vis package. These have zero behavioral coupling.

**Move from `flowStore.ts`:**
- `NodeShape`, `NODE_SHAPES`
- `PortConfig`
- `BaseNodeData`, `FactoryDefaults`, `NetrunNodeData`, `SubgraphNodeData`
- `DecorationType`, `DECORATION_TYPES`, `DecorationNodeData`
- `AnyNodeData`
- `NetrunEdgeData`
- Type aliases: `FlowNode`, `NetrunNode`, `SubgraphNode`, `DecorationNode`, `NetrunEdge`

**Move from `types/salvoConditions.ts`:**
- All salvo condition types and helper functions (already self-contained)

**Move from `constants.ts`:**
- `PORT_ROW_HEIGHT`, `NODE_BORDER_WIDTH`

**New types:**
- `NetrunGraph`, `GraphSettings`
- `FlowViewerEvents`, `CascadeHighlightState`
- `NetrunFlowNode`, `NetrunFlowEdge` (type aliases wrapping @xyflow/svelte Node/Edge with our data)

**In `netrun-ui`:** Replace all imports with re-exports from the vis package. Zero visible change to behavior.

**Verification:** `npm run check` in `netrun-ui` passes. All existing tests pass.

### Phase 2: Extract Pure Utilities [DONE]

Move the purely computational utility modules.

**Move as-is:**
- `utils/portGroups.ts` (355 LOC) — update imports to use vis package types
- `utils/dependencyAnalysis.ts` (118 LOC) — only depends on `@xyflow/svelte` Edge type
- `utils/autoLayout.ts` (201 LOC) — only depends on `@xyflow/svelte` and `elkjs`
- `utils/salvoParser.ts` (616 LOC) — depends on salvo condition types
- `utils/salvoSerializer.ts` (485 LOC) — depends on salvo condition types

**Extract from `portGroupStore.ts`:**
- `AUTO_COLLAPSE_THRESHOLD`, `getDefaultCollapsed()`, `isPortGroupCollapsed()`, `getPortGroupStates()` — these are pure functions, not store logic. Move to `utils/portGroupCollapse.ts`.

**In `netrun-ui`:** Replace imports to point at vis package. The portGroupStore.ts in netrun-ui becomes a thin re-export.

**Verification:** `npm run check` passes. Existing tests pass.

### Phase 3: Refactor Node Components to Props-Only [DONE]

This is the hardest phase. Each node component currently imports store functions for mutations. We need to replace those with event callbacks passed as props.

#### 3a: NetrunNode

**Current store dependencies:**
- `cascadeHighlight` (read) → becomes a prop: `cascadeHighlight?: CascadeHighlightState | null`
- `updateNodeDimensions` (write) → becomes event: `onResize`
- `pushHistory` (write) → removed (consumer handles history)
- `toggleNodeDescExpanded` (write) → becomes event: `onDescriptionToggle`

**New props interface:**
```typescript
interface Props {
  id: string;
  data: NetrunNodeData;
  selected?: boolean;
  cascadeHighlight?: CascadeHighlightState | null;
  editable?: boolean;
  onResize?: (id: string, width: number, height: number) => void;
  onDescriptionToggle?: (id: string) => void;
  onDoubleClick?: (id: string, data: NetrunNodeData, metaKey: boolean) => void;
}
```

The component becomes a pure renderer: it reads data from props, emits events for user interactions, and doesn't import any stores.

#### 3b: SubgraphNode

**Current store dependencies:**
- `updateNodeDimensions` → `onResize` event
- `pushHistory` → removed
- `toggleNodeDescExpanded` → `onDescriptionToggle` event
- `openSubgraphTab` → `onSubgraphOpen` event
- `toggleSubgraphExpansion` → `onSubgraphToggleExpand` event

**New props interface extends NetrunNode pattern:**
```typescript
interface Props {
  id: string;
  data: SubgraphNodeData;
  selected?: boolean;
  cascadeHighlight?: CascadeHighlightState | null;
  editable?: boolean;
  onResize?: (id: string, width: number, height: number) => void;
  onDescriptionToggle?: (id: string) => void;
  onSubgraphOpen?: (id: string, data: SubgraphNodeData) => void;
  onSubgraphToggleExpand?: (id: string) => void;
}
```

#### 3c: DecorationNode

**Current store dependencies:**
- `updateNodeDimensions` → `onResize` event
- `pushHistory` → removed
- `getCurrentConfig` (for image URL) → `imageBaseUrl` prop

**New props:**
```typescript
interface Props {
  id: string;
  data: DecorationNodeData;
  selected?: boolean;
  editable?: boolean;
  imageBaseUrl?: string;  // Base URL for resolving relative image paths
  onResize?: (id: string, width: number, height: number) => void;
}
```

#### 3d: PortList

**Current store dependencies:**
- `toggleNodePortGroup` → `onPortGroupToggle` event
- `signalTypeFromPort` → `signalConfig` prop (prefix + suffix + types, compute locally)
- `controlTypeFromPort` → `controlConfig` prop (same pattern)

**New props interface:**
```typescript
interface Props {
  nodeId: string;
  ports: PortConfig[];
  side: 'in' | 'out';
  portGroupStates?: Record<string, boolean>;
  hidePortNames?: boolean;
  exposedPortNames?: string[];
  signalConfig?: { prefix: string; suffix: string; types: string[] };
  controlConfig?: { prefix: string; suffix: string; types: string[] };
  onPortGroupToggle?: (nodeId: string, side: 'in' | 'out', groupPath: string) => void;
}
```

#### 3e: Adapter Layer in netrun-ui

Create wrapper components in `netrun-ui` that bridge vis components to the store architecture:

```svelte
<!-- netrun-ui/src/lib/components/NetrunNodeWrapper.svelte -->
<script lang="ts">
  import { NetrunNode } from 'netrun-ui-vis';
  import { cascadeHighlight, updateNodeDimensions, pushHistory, toggleNodeDescExpanded } from '$lib/stores/flowStore';

  let { id, data, selected }: Props = $props();
</script>

<NetrunNode
  {id} {data} {selected}
  cascadeHighlight={$cascadeHighlight}
  editable={true}
  onResize={(id, w, h) => { updateNodeDimensions([{ id, width: w, height: h }]); pushHistory(); }}
  onDescriptionToggle={(id) => { toggleNodeDescExpanded(id); pushHistory(); }}
/>
```

These wrappers are thin (~10-20 lines each) and registered as node types in SvelteFlow.

**Verification:** Full app works identically. Visual diff: zero.

### Phase 4: Extract FlowViewer Component [DONE]

Create `NetrunFlowViewer.svelte` in the vis package. This wraps SvelteFlow with all netrun-specific configuration (dark theme, edge styling, markers, context menus, connection validation).

**What moves:**
- SvelteFlow setup (node types, edge options, snap grid, color mode)
- Background, Controls, MiniMap configuration
- Edge styling derivations (edge type, markers, dependency styling)
- Node/edge selection tracking
- Connection validation (`isValidConnection`)
- Context menu rendering
- Cascade highlight application to edges/minimap
- CSS: all global SvelteFlow style overrides

**What stays in netrun-ui:**
- Editor-specific behaviors wired via event callbacks
- Subgraph expansion view integration (netrun-ui creates an adapter that combines `expandedView` store output with vis package rendering)
- Tab switching / fit-view logic

**The viewer accepts the full `NetrunGraph` + events + config as props.**

**Verification:** Full app works identically.

### Phase 5: Theme CSS [DONE]

Extract the default dark theme as a standalone CSS file that consumers can import:

```css
/* netrun-ui-vis/src/theme.css */
:root {
  --netrun-bg-primary: #1a1a1a;
  --netrun-bg-secondary: #242424;
  --netrun-bg-tertiary: #2d2d2d;
  --netrun-border-color: #404040;
  --netrun-text-primary: #fff;
  --netrun-text-secondary: #a0a0a0;
  --netrun-accent-color: #3b82f6;
  --netrun-error-color: #ef4444;
  --netrun-node-bg: #2d2d2d;
  --netrun-node-border: #404040;
  --netrun-node-selected: #3b82f6;
  --netrun-port-input: #22c55e;
  --netrun-port-output: #f59e0b;
  --netrun-subgraph-border: #22c55e;
  /* ... */
}
```

The vis package components reference these `--netrun-*` prefixed properties internally, with fallback values. The current netrun-ui maps its existing `--bg-primary` etc. to these.

### Phase 6: Static HTML Export Tool

Build a tool that takes a `.netrun.json` file and produces a standalone `.html`:

1. Parse the config file (can be done in Python or JS)
2. Convert the graph config to `NetrunGraph` format (extract the converter logic from the Python backend into a JS utility, or provide a Python-side converter that outputs JSON in the vis format)
3. Bundle the vis package + data into a single HTML file using Vite's library build

This could be:
- A CLI command: `netrun vis export my_net.netrun.json -o my_net.html`
- A Python function in `netrun`: `netrun.vis.export_html(config, output_path)`

The HTML file embeds:
- Bundled vis package JS/CSS
- The graph data as an inline JSON blob
- A minimal mount script that renders `<NetrunFlowViewer>`

**This phase is the primary deliverable** — it's the use case that motivated this extraction.

## File Change Summary

### New files (netrun-ui-vis/)
| File | Source | Notes |
|------|--------|-------|
| `src/types/nodes.ts` | `flowStore.ts` types | Types + constant arrays |
| `src/types/edges.ts` | `flowStore.ts` types | NetrunEdgeData |
| `src/types/graph.ts` | New | NetrunGraph, GraphSettings |
| `src/types/events.ts` | New | FlowViewerEvents |
| `src/types/salvoConditions.ts` | `types/salvoConditions.ts` | Move as-is |
| `src/components/NetrunFlowViewer.svelte` | `FlowEditor.svelte` | Refactored to props-only |
| `src/components/NetrunNode.svelte` | `NetrunNode.svelte` | Refactored to props-only |
| `src/components/SubgraphNode.svelte` | `SubgraphNode.svelte` | Refactored to props-only |
| `src/components/DecorationNode.svelte` | `DecorationNode.svelte` | Refactored to props-only |
| `src/components/PortList.svelte` | `PortList.svelte` | Refactored to props-only |
| `src/utils/autoLayout.ts` | `utils/autoLayout.ts` | Move as-is |
| `src/utils/portGroups.ts` | `utils/portGroups.ts` | Move, update type imports |
| `src/utils/portGroupCollapse.ts` | `stores/portGroupStore.ts` | Extract pure functions |
| `src/utils/dependencyAnalysis.ts` | `utils/dependencyAnalysis.ts` | Move as-is |
| `src/utils/salvoParser.ts` | `utils/salvoParser.ts` | Move as-is |
| `src/utils/salvoSerializer.ts` | `utils/salvoSerializer.ts` | Move as-is |
| `src/constants.ts` | `constants.ts` | Move as-is |
| `src/theme.css` | New | Extracted CSS custom properties |

### Modified files (netrun-ui/)
| File | Change |
|------|--------|
| `package.json` | Add `netrun-ui-vis` dependency |
| `stores/flowStore.ts` | Remove moved types, import from vis package |
| `stores/portGroupStore.ts` | Thin re-export from vis package utils |
| `components/FlowEditor.svelte` | Wrap `NetrunFlowViewer`, wire store events |
| `components/NetrunNode.svelte` | Becomes wrapper importing vis component |
| `components/SubgraphNode.svelte` | Becomes wrapper importing vis component |
| `components/DecorationNode.svelte` | Becomes wrapper importing vis component |
| `components/PortList.svelte` | Becomes wrapper importing vis component |
| `utils/autoLayout.ts` | Re-export from vis package |
| `utils/portGroups.ts` | Re-export from vis package |
| `utils/dependencyAnalysis.ts` | Re-export from vis package |
| `utils/salvoParser.ts` | Re-export from vis package |
| `utils/salvoSerializer.ts` | Re-export from vis package |
| `types/salvoConditions.ts` | Re-export from vis package |
| `constants.ts` | Re-export from vis package |

### Deleted files (netrun-ui/)
None — all original files become re-exports to avoid breaking any existing import paths.

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| SvelteFlow version coupling | Pin same version in both packages; vis package declares it as peer dependency |
| CSS specificity conflicts | Prefix all vis package CSS custom properties with `--netrun-` |
| Bundle size increase (double-bundling) | Use workspace dependency so Vite deduplicates |
| Breaking change surface | Re-export everything from netrun-ui so no import paths change |
| Component render performance | Props-only components may re-render more than store-connected ones; mitigate with `$derived` memoization in wrappers |
| SubgraphExpandStore complexity | Keep expansion orchestration in netrun-ui; vis package only renders what it's given |

## Open Questions

1. **Package location**: Sibling directory (`netrun-ui-vis/`) or monorepo workspace? Sibling is simpler to start; can restructure later.
2. **Package name**: `netrun-ui-vis`, `@netrun/vis`, or `netrun-flow-viewer`?
3. **Static HTML converter**: Python-side (in `netrun` CLI) or JS-side (separate tool)? Python-side is simpler since the converter already exists in the backend.
4. **Signal/control port config**: Currently loaded from backend. For the vis package, should we hardcode the known types as defaults, or require consumers to always provide them? Providing defaults from the netrun package seems cleanest.
