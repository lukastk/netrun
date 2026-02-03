# SvelteFlow Documentation Reference

This document summarizes the key SvelteFlow concepts and APIs relevant to building netrun-ui.

## Overview

SvelteFlow is a library for building node-based UIs with Svelte. It provides:
- An infinite canvas with pan/zoom
- Nodes and edges with customizable rendering
- Connection handling via "handles"
- Built-in components for minimap, controls, backgrounds
- Sub-flows (nested graphs) support

**Key Links:**
- API Reference: https://svelteflow.dev/api-reference
- Examples: https://svelteflow.dev/examples
- Learn/Guides: https://svelteflow.dev/learn

---

## Core Architecture

### Main Components

```svelte
<script>
  import {
    SvelteFlow,
    SvelteFlowProvider,
    Background,
    Controls,
    MiniMap,
    Panel
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
</script>

<SvelteFlow
  bind:nodes
  bind:edges
  {nodeTypes}
  {edgeTypes}
  fitView
>
  <Background />
  <Controls />
  <MiniMap />
  <Panel position="top-left">Custom content</Panel>
</SvelteFlow>
```

### State Management

SvelteFlow uses Svelte's `$state.raw()` for reactive node/edge arrays:

```javascript
let nodes = $state.raw([
  { id: '1', type: 'custom', position: { x: 0, y: 0 }, data: { label: 'Node 1' } },
  { id: '2', type: 'custom', position: { x: 200, y: 100 }, data: { label: 'Node 2' } },
]);

let edges = $state.raw([
  { id: 'e1-2', source: '1', target: '2' },
]);
```

---

## Node Type Definition

```typescript
interface Node<NodeData = any, NodeType = string> {
  // Required
  id: string;
  position: { x: number; y: number };

  // Common
  type?: string;              // Matches key in nodeTypes
  data?: NodeData;            // Passed to custom node component

  // Connection
  sourcePosition?: Position;  // Default handle positions
  targetPosition?: Position;
  handles?: NodeHandle[];     // Programmatic handle definitions

  // Visibility & Interaction
  hidden?: boolean;
  selected?: boolean;
  dragging?: boolean;
  draggable?: boolean;
  selectable?: boolean;
  connectable?: boolean;
  deletable?: boolean;

  // Sizing
  width?: number;
  height?: number;
  initialWidth?: number;
  initialHeight?: number;
  measured?: { width?: number; height?: number }; // Read-only

  // Sub-flows
  parentId?: string;          // For nested nodes
  extent?: CoordinateExtent | 'parent' | null;
  expandParent?: boolean;

  // Styling
  zIndex?: number;
  class?: string;
  style?: string;

  // Accessibility
  ariaLabel?: string;
  focusable?: boolean;
}
```

---

## Edge Type Definition

```typescript
interface Edge<EdgeData = any, EdgeType = string> {
  // Required
  id: string;
  source: string;             // Source node ID
  target: string;             // Target node ID

  // Handle targeting (for nodes with multiple handles)
  sourceHandle?: string | null;
  targetHandle?: string | null;

  // Appearance
  type?: EdgeType;            // 'default', 'straight', 'step', 'smoothstep', or custom
  label?: string;
  labelStyle?: string;
  style?: string;
  class?: string;
  animated?: boolean;

  // Markers (arrows)
  markerStart?: EdgeMarkerType;
  markerEnd?: EdgeMarkerType;

  // Interaction
  hidden?: boolean;
  selected?: boolean;
  selectable?: boolean;
  deletable?: boolean;
  interactionWidth?: number;  // Click target width

  // Data
  data?: EdgeData;

  // Styling
  zIndex?: number;
}
```

---

## Custom Nodes

Custom nodes are Svelte components that receive props automatically:

```svelte
<!-- CustomNode.svelte -->
<script lang="ts">
  import { Handle, Position, useSvelteFlow, type NodeProps } from '@xyflow/svelte';

  // Props injected by SvelteFlow
  let { id, data, selected }: NodeProps<{ label: string }> = $props();

  const { updateNodeData } = useSvelteFlow();
</script>

<div class="custom-node" class:selected>
  <!-- Target handle (input) on left -->
  <Handle type="target" position={Position.Left} />

  <div class="content">
    <h3>{data.label}</h3>
    <input
      value={data.value}
      oninput={(e) => updateNodeData(id, { value: e.target.value })}
      class="nodrag"
    />
  </div>

  <!-- Source handle (output) on right -->
  <Handle type="source" position={Position.Right} />
</div>

<style>
  .custom-node {
    padding: 10px;
    border: 1px solid #ccc;
    border-radius: 4px;
    background: white;
  }
  .custom-node.selected {
    border-color: #3b82f6;
  }
</style>
```

Register custom node types:

```svelte
<script>
  import CustomNode from './CustomNode.svelte';

  const nodeTypes = {
    custom: CustomNode,
    // Add more types...
  };
</script>

<SvelteFlow bind:nodes bind:edges {nodeTypes} />
```

---

## Handle Component

Handles are connection points on nodes:

```svelte
<script>
  import { Handle, Position } from '@xyflow/svelte';
</script>

<!-- Basic handles -->
<Handle type="target" position={Position.Left} />
<Handle type="source" position={Position.Right} />

<!-- Multiple handles with IDs -->
<Handle type="target" position={Position.Left} id="input-a" />
<Handle type="target" position={Position.Left} id="input-b" style="top: 60%" />
<Handle type="source" position={Position.Right} id="output" />

<!-- With validation -->
<Handle
  type="target"
  position={Position.Left}
  isValidConnection={(connection) => {
    // Return true to allow connection
    return connection.source !== connection.target;
  }}
/>

<!-- Connection events -->
<Handle
  type="source"
  position={Position.Right}
  onconnect={(connections) => console.log('Connected:', connections)}
  ondisconnect={(connections) => console.log('Disconnected:', connections)}
/>
```

### Handle Props

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `type` | `'source' \| 'target'` | `'source'` | Connection direction |
| `position` | `Position` | `Position.Top` | Location on node |
| `id` | `string` | - | Unique ID (required for multiple handles) |
| `isConnectable` | `boolean` | `true` | Allow connections |
| `isConnectableStart` | `boolean` | `true` | Can start connections |
| `isConnectableEnd` | `boolean` | `true` | Can receive connections |
| `isValidConnection` | `(conn) => boolean` | - | Custom validation |
| `onconnect` | `(connections) => void` | - | Connection event |
| `ondisconnect` | `(connections) => void` | - | Disconnection event |

---

## Sub-Flows (Nested Graphs)

Create parent-child relationships using `parentId`:

```javascript
let nodes = $state.raw([
  // Parent node (must come first!)
  {
    id: 'group-1',
    type: 'group',  // Built-in group type
    position: { x: 0, y: 0 },
    data: { label: 'My Group' },
    style: 'width: 400px; height: 300px;',
  },
  // Child nodes
  {
    id: 'child-1',
    position: { x: 20, y: 40 },  // Relative to parent
    parentId: 'group-1',
    extent: 'parent',  // Constrain to parent bounds
    data: { label: 'Child 1' },
  },
  {
    id: 'child-2',
    position: { x: 200, y: 40 },
    parentId: 'group-1',
    extent: 'parent',
    data: { label: 'Child 2' },
  },
]);
```

**Important:** Parents must appear before children in the nodes array!

---

## useSvelteFlow Hook

Access flow utilities from any component inside `<SvelteFlow>`:

```svelte
<script>
  import { useSvelteFlow } from '@xyflow/svelte';

  const {
    // Viewport
    zoomIn, zoomOut, setZoom, getZoom,
    setCenter, setViewport, getViewport,
    fitView, fitBounds,

    // Node/Edge access
    getNode, getNodes, getInternalNode,
    getEdge, getEdges,

    // Modification
    updateNode, updateNodeData,
    updateEdge, deleteElements,

    // Spatial
    getIntersectingNodes, isNodeIntersecting,
    getNodesBounds,

    // Coordinates
    screenToFlowPosition, flowToScreenPosition,

    // Utility
    getHandleConnections, toObject,
  } = useSvelteFlow();
</script>
```

### Common Operations

```javascript
// Add a new node
function addNode() {
  const position = screenToFlowPosition({ x: 100, y: 100 });
  nodes = [...nodes, { id: crypto.randomUUID(), position, data: {} }];
}

// Update node data
updateNodeData('node-1', { label: 'New Label' });

// Delete selected elements
deleteElements({ nodes: selectedNodes, edges: selectedEdges });

// Fit view to specific nodes
fitView({ nodes: ['node-1', 'node-2'], padding: 0.2 });

// Export flow state
const flowState = toObject();
// Returns: { nodes: [...], edges: [...], viewport: {...} }
```

---

## MiniMap Component

```svelte
<SvelteFlow bind:nodes bind:edges>
  <MiniMap
    nodeColor={(node) => {
      // Color nodes based on type
      switch (node.type) {
        case 'input': return '#6ede87';
        case 'output': return '#e86262';
        default: return '#eee';
      }
    }}
    nodeStrokeWidth={3}
    pannable
    zoomable
    position="bottom-right"
  />
</SvelteFlow>
```

### MiniMap Props

| Prop | Type | Description |
|------|------|-------------|
| `bgColor` | `string` | Background color |
| `nodeColor` | `string \| (node) => string` | Node fill color |
| `nodeStrokeColor` | `string \| (node) => string` | Node stroke color |
| `nodeStrokeWidth` | `number` | Stroke width |
| `nodeBorderRadius` | `number` | Node corner radius |
| `maskColor` | `string` | Viewport indicator color |
| `pannable` | `boolean` | Enable pan via minimap |
| `zoomable` | `boolean` | Enable zoom via minimap |
| `position` | `PanelPosition` | Placement on canvas |
| `width` / `height` | `number` | Minimap dimensions |

---

## Event Handlers

### Node Events

```svelte
<SvelteFlow
  bind:nodes
  bind:edges
  onnodeclick={(event) => {
    console.log('Clicked node:', event.node.id);
  }}
  onnodedragstart={(event) => {
    console.log('Started dragging:', event.node.id);
  }}
  onnodedragstop={(event) => {
    console.log('Stopped dragging:', event.node.id);
  }}
  onnodecontextmenu={(event) => {
    event.event.preventDefault();
    showContextMenu(event.node, event.event);
  }}
/>
```

### Edge Events

```svelte
<SvelteFlow
  onedgeclick={(event) => console.log('Clicked edge:', event.edge.id)}
  onedgecontextmenu={(event) => showEdgeMenu(event.edge)}
/>
```

### Connection Events

```svelte
<SvelteFlow
  onconnect={(connection) => {
    // Called when a connection is completed
    console.log('Connected:', connection);
  }}
  onconnectstart={(event) => {
    console.log('Started connecting from:', event.nodeId, event.handleId);
  }}
  onconnectend={(event) => {
    console.log('Connection ended');
  }}
  onbeforeconnect={(connection) => {
    // Modify the edge before it's added
    return { ...connection, animated: true };
  }}
/>
```

### Selection Events

```svelte
<SvelteFlow
  onselectionchanged={(params) => {
    console.log('Selected nodes:', params.nodes);
    console.log('Selected edges:', params.edges);
  }}
/>
```

### Pane Events

```svelte
<SvelteFlow
  onpaneclick={(event) => {
    // Clicked on empty canvas
    deselectAll();
  }}
  onpanecontextmenu={(event) => {
    event.event.preventDefault();
    showPaneContextMenu(event.event);
  }}
/>
```

---

## Keyboard Shortcuts

SvelteFlow has built-in keyboard shortcuts:

| Key | Default Action |
|-----|----------------|
| `Backspace` / `Delete` | Delete selected elements |
| `Shift` + drag | Selection box |
| `Meta` / `Ctrl` + click | Multi-select |
| `Meta` / `Ctrl` + scroll | Zoom |
| `Space` + drag | Pan |

Customize via props:

```svelte
<SvelteFlow
  deleteKey="Delete"
  selectionKey="Shift"
  multiSelectionKey={['Meta', 'Control']}
  panActivationKey="Space"
  zoomActivationKey={['Meta', 'Control']}
/>
```

---

## Built-in Edge Types

| Type | Description |
|------|-------------|
| `default` | Bezier curve (smooth) |
| `straight` | Direct line |
| `step` | Right-angle steps |
| `smoothstep` | Rounded right-angle steps |

```javascript
edges = [
  { id: 'e1', source: '1', target: '2', type: 'smoothstep' },
  { id: 'e2', source: '2', target: '3', type: 'straight' },
];
```

---

## Relevant Examples for netrun-ui

Based on the SvelteFlow examples, these are most relevant:

1. **Custom Nodes** - For our port-based node rendering
2. **Drag and Drop** - Adding nodes from sidebar
3. **Context Menu** - Right-click menus on nodes/edges
4. **Validation** - Port type checking on connections
5. **Subflows** - For nested subgraph visualization
6. **Node Resizer** - If we want resizable nodes

---

## Recommended State Management

For netrun-ui, I recommend:

1. **Svelte 5 Runes** (`$state`, `$derived`, `$effect`) for component-local state
2. **Svelte Stores** for shared app state (current file, selection, undo history)
3. **Context API** for passing state down the component tree

Example structure:

```javascript
// stores/flowStore.js
import { writable, derived } from 'svelte/store';

export const nodes = writable([]);
export const edges = writable([]);
export const selectedNodeIds = writable(new Set());

export const selectedNodes = derived(
  [nodes, selectedNodeIds],
  ([$nodes, $selectedIds]) => $nodes.filter(n => $selectedIds.has(n.id))
);

// Undo/redo history
export const history = writable({ past: [], future: [] });

export function pushHistory(state) {
  history.update(h => ({
    past: [...h.past, state],
    future: []
  }));
}

export function undo() {
  // ... undo logic
}

export function redo() {
  // ... redo logic
}
```

---

## Next Steps

1. Set up SvelteKit project with SvelteFlow
2. Create custom node component for netrun nodes (with port handles)
3. Implement basic file loading/saving via FastAPI backend
4. Add properties sidebar with two-way binding to node data
