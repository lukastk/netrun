# Plan: Subgraph Support for netrun and netrun-ui

## Overview

Implement subgraphs - groups of nodes that behave as a single "meta-node" that can be nested, referenced by file path, and navigated in the UI. Subgraphs are a native netrun concept that resolve to flat graphs at runtime with prefixed node names.

## Key Concepts

- **Inline subgraphs**: Nodes defined directly within the subgraph config
- **File-referenced subgraphs**: Reference external `.netrun.json` file (only graph portion used)
- **Exposed ports**: Input/output ports that connect the subgraph to the parent graph
- **Resolution**: At runtime, subgraphs flatten to nodes with prefixed names (e.g., `subgraph.node`)

---

## Part 1: netrun Python Config Changes

### Files to Modify

- `netrun/pts/netrun/05_net/00_config.pct.py` (source - edit this)
- `netrun/pts/tests/05_net/test_config.pct.py` (add tests)

### 1.1 New Models

```python
class ExposedPortConfig(BaseModel):
    """Maps an exposed port to an internal node's port."""
    internal_node: str
    internal_port: str
    rename: str | None = None  # Exposed name (defaults to internal_port)

class SubgraphConfig(BaseModel):
    """A group of nodes that acts as a single node."""
    type: Literal["subgraph"] = "subgraph"  # Discriminator
    name: str

    # Either inline OR file reference (not both)
    nodes: list["NodeConfig | SubgraphConfig"] | None = None
    edges: list[EdgeConfig] = Field(default_factory=list)
    path: str | None = None  # Path to .netrun.json file

    # Exposed ports
    exposed_in_ports: dict[str, ExposedPortConfig] = Field(default_factory=dict)
    exposed_out_ports: dict[str, ExposedPortConfig] = Field(default_factory=dict)

    meta: dict[str, Any] = Field(default_factory=dict)

    def resolve(self, base_path: Path | None = None) -> tuple[list[NodeConfig], list[EdgeConfig], dict]:
        """Resolve to flat nodes/edges with prefixed names."""
        pass
```

### 1.2 Update GraphConfig

```python
class NodeConfig(BaseModel):
    type: Literal["node"] = "node"  # Add discriminator
    # ... existing fields unchanged

class GraphConfig(BaseModel):
    nodes: list[NodeConfig | SubgraphConfig]  # Changed from list[NodeConfig]

    def resolve(self, base_path: Path | None = None) -> "GraphConfig":
        """Resolve all subgraphs to flat graph with prefixed names."""
        pass
```

### 1.3 Resolution Logic

The `resolve()` method must:
1. Load external subgraphs from file if `path` is set
2. Recursively resolve nested subgraphs
3. Prefix all internal node names with `subgraph_name.`
4. Rewrite internal edges with prefixed names
5. Map exposed ports to internal node ports
6. Validate no name collisions after flattening

### 1.4 Validation

Add to `GraphConfig.resolve()`:
- Node names must be unique after resolution
- Prevent conflicts: can't have `foo.bar` at top level if subgraph `foo` contains `bar`

---

## Part 2: netrun-ui Backend Changes

### Files to Modify

- `netrun-ui/backend/app/converter.py` - Format conversion
- `netrun-ui/backend/app/routes/files.py` - New endpoints

### 2.1 Converter Updates

**`graph_config_to_ui()`** - Handle subgraph nodes:
- Detect subgraphs by `type: "subgraph"` field
- Convert to UI node with `type: "subgraphNode"`
- Build exposed ports as node ports
- Store full subgraph config in `_subgraphConfig` for round-trip

**`ui_to_graph_config()`** - Restore subgraph config:
- Detect by `nodeType: "subgraph"`
- Restore from `_subgraphConfig` storage

### 2.2 New API Endpoints

**POST `/api/files/subgraph/load`**
- Input: `path` (file) or `inline_config` (inline)
- Output: `{ nodes, edges, exposed_in_ports, exposed_out_ports, source }`
- Purpose: Load subgraph content for editing

**POST `/api/files/subgraph/create`**
- Input: `selected_node_ids`, `all_nodes`, `all_edges`, `subgraph_name`
- Output: `{ subgraph_node, remaining_nodes, remaining_edges, updated_edges }`
- Purpose: Create subgraph from selection, auto-detect boundary ports

---

## Part 3: netrun-ui Frontend Changes

### Files to Create

- `netrun-ui/src/lib/components/SubgraphNode.svelte` - Subgraph node rendering
- `netrun-ui/src/lib/components/Breadcrumb.svelte` - Hierarchy navigation

### Files to Modify

- `netrun-ui/src/lib/components/FlowEditor.svelte` - Register node type, double-click handler
- `netrun-ui/src/lib/stores/flowStore.ts` - Subgraph tab opening, creation
- `netrun-ui/src/lib/stores/tabsStore.ts` - Subgraph context tracking
- `netrun-ui/src/lib/components/Toolbar.svelte` - Create subgraph button
- `netrun-ui/src/lib/components/Sidebar.svelte` - Subgraph properties
- `netrun-ui/src/lib/api.ts` - New API methods
- `netrun-ui/src/routes/+page.svelte` - Breadcrumb integration

### 3.1 SubgraphNode Component

Visual differences from regular nodes:
- Green border/header (vs gray for regular, purple for factory)
- "SG" badge in header
- Shows internal node count
- Shows source (file path or "Inline")
- "Double-click to edit" hint

### 3.2 Tab State Extension

```typescript
interface SubgraphContext {
    parentTabId: string;
    nodeId: string;
    path: string[];  // Breadcrumb: ["Root", "Subgraph1", "Nested"]
}

interface TabState {
    // ... existing fields
    subgraphContext: SubgraphContext | null;
}
```

### 3.3 Navigation Flow

1. **Double-click subgraph node** -> Call `openSubgraphTab()`
2. **Load subgraph content** -> API call to `/subgraph/load`
3. **Create new tab** -> With `subgraphContext` set
4. **Show breadcrumb** -> Clickable path to navigate back

### 3.4 Create Subgraph Flow

1. **Select 2+ nodes** -> "Create Subgraph" button enabled
2. **Click button** -> Prompt for name
3. **API call** -> `/subgraph/create` with selection
4. **Update graph** -> Replace selected nodes with subgraph node
5. **Auto-expose ports** -> Edges crossing boundary become exposed ports

### 3.5 Keyboard Shortcuts

- `Cmd+G` - Create subgraph from selection

---

## Implementation Phases

### Phase 1: netrun Config (2 days)
1. Add `ExposedPortConfig` model
2. Add `SubgraphConfig` model with validation
3. Add `type` discriminator to `NodeConfig`
4. Update `GraphConfig.nodes` type
5. Implement `SubgraphConfig.resolve()`
6. Implement `GraphConfig.resolve()`
7. Add name uniqueness validation
8. Write unit tests

### Phase 2: Backend Converter (1 day)
1. Update `graph_config_to_ui()` for subgraphs
2. Update `ui_to_graph_config()` for subgraphs
3. Add `/subgraph/load` endpoint
4. Add `/subgraph/create` endpoint
5. Test round-trip serialization

### Phase 3: Frontend Core (2 days)
1. Create `SubgraphNode.svelte`
2. Register node type in FlowEditor
3. Add double-click navigation handler
4. Extend TabState for subgraph context
5. Implement `openSubgraphTab()`
6. Update API client

### Phase 4: Frontend Polish (2 days)
1. Create `Breadcrumb.svelte`
2. Add breadcrumb to layout
3. Implement `createSubgraphFromSelection()`
4. Add toolbar button
5. Update Sidebar with subgraph section
6. Add Cmd+G shortcut

### Phase 5: Testing (1 day)
1. Test nested subgraphs (3+ levels)
2. Test file-referenced subgraphs
3. Test copy/paste subgraphs
4. Test undo/redo
5. Test edge reconnection
6. Error handling

---

## UI Design

```
┌─────────────────────────────────────────────────────────────────┐
│ [File ops] [Undo] [Redo] │ filename │ [Validate] [+ Node] [SG]  │ <- Toolbar
├─────────────────────────────────────────────────────────────────┤
│ [example.netrun.json] [MySubgraph] [+]                          │ <- TabBar
├─────────────────────────────────────────────────────────────────┤
│ Root > MySubgraph                                               │ <- Breadcrumb
├──────────┬────────────────────────────────────────┬─────────────┤
│ Explorer │   ┌───────────────┐                    │  Sidebar    │
│          │   │ SG SubgraphA  │ <- Green border    │             │
│          │   │ ─────────────│                    │  [Subgraph] │
│          │   │ ○ in    out ○│ <- Exposed ports   │  Source:    │
│          │   │   (3 nodes)  │                    │  Inline     │
│          │   │ dbl-click... │                    │  [Edit]     │
│          │   └───────────────┘                    │             │
└──────────┴────────────────────────────────────────┴─────────────┘
```

---

## Verification

1. **Config round-trip**: Create subgraph in Python, serialize to JSON, load in UI, save, load in Python
2. **Create subgraph**: Select nodes, create subgraph, verify edges reconnected
3. **Navigate into**: Double-click subgraph, verify breadcrumb shows path
4. **Navigate back**: Click breadcrumb, verify returns to parent
5. **Nested subgraphs**: Create subgraph inside subgraph, verify 3-level navigation
6. **File reference**: Create subgraph referencing external file, verify loads correctly
7. **Resolution**: Verify `GraphConfig.resolve()` produces flat graph with prefixed names

---

## Edge Cases

- **Circular references**: Detect and prevent during load/save
- **Missing files**: Show error when file-based subgraph file not found
- **Name collisions**: Validate and show error during resolution
- **Empty subgraphs**: Allow subgraphs with only pass-through ports
- **Copy/paste**: Subgraphs copy as single unit with all internal state
