# Plan: Phase 2 Remaining Features

## Overview

Complete the remaining Phase 2 features for netrun-ui:
1. Copy/paste nodes (works across tabs)
2. NetConfig/pool editing in sidebar
3. Validation (backend + UI feedback)
4. Recent files

## Feature 1: Copy/Paste Nodes

### Requirements
- Cmd+C to copy selected nodes
- Cmd+V to paste at cursor position (or center of viewport)
- Copy works across tabs (use a shared clipboard store)
- Paste creates new nodes with new IDs
- Edges between copied nodes are NOT copied (per spec)

### Implementation

**New file: `netrun-ui/src/lib/stores/clipboardStore.ts`**
```typescript
interface ClipboardState {
  nodes: NetrunNode[];
  sourceTabId: string | null;
}

export const clipboard = writable<ClipboardState>({ nodes: [], sourceTabId: null });

export function copyNodes(nodes: NetrunNode[]): void;
export function pasteNodes(position?: { x: number; y: number }): NetrunNode[];
```

**Modify: `netrun-ui/src/lib/components/Toolbar.svelte`**
- Add Cmd+C handler to copy selected nodes
- Add Cmd+V handler to paste nodes
- Add Cmd+X for cut (copy + delete)

**Modify: `netrun-ui/src/lib/stores/flowStore.ts`**
- Add `copySelectedNodes()` function
- Add `pasteNodes(position)` function

### Keyboard Shortcuts
- `Cmd+C` - Copy selected nodes
- `Cmd+V` - Paste nodes
- `Cmd+X` - Cut (copy + delete)

---

## Feature 2: NetConfig/Pool Editing in Sidebar

### Requirements
- When no node is selected, sidebar shows net-level settings
- Edit GraphConfig settings (name, description, etc.)
- Edit pools configuration
- Edit UI-specific settings (edge style, etc.)

### Implementation

**Modify: `netrun-ui/src/lib/components/Sidebar.svelte`**
- When `$selectedNode` is null, show NetConfig editor instead of "Select a node"
- Sections:
  - **Graph Settings**: name, description
  - **Pools**: List of pool configurations (add/edit/remove)
  - **UI Settings**: Default edge style, grid settings, etc.

**Modify: `netrun-ui/src/lib/stores/flowStore.ts`**
- Add functions to update `extraData` (pools, etc.)
- Add functions to update `graphMeta`

**Modify: `netrun-ui/src/lib/stores/tabsStore.ts`**
- Ensure `extraData` and `graphMeta` are properly typed and accessible

### UI Design
```
┌─────────────────────────┐
│ Properties              │
├─────────────────────────┤
│ [v] Graph Settings      │
│   Name: [___________]   │
│   Description: [____]   │
├─────────────────────────┤
│ [v] Pools               │
│   + Add Pool            │
│   ┌─────────────────┐   │
│   │ thread_pool     │   │
│   │ Type: ThreadPool│   │
│   │ Workers: 4      │   │
│   └─────────────────┘   │
├─────────────────────────┤
│ [v] UI Settings         │
│   Edge Style: [smooth]  │
│   Snap to Grid: [x]     │
└─────────────────────────┘
```

---

## Feature 3: Validation (Backend + UI Feedback)

### Requirements
- Backend validates node configurations
- UI shows validation errors on nodes (red border, error icon)
- Sidebar shows validation details for selected node
- Validation runs on:
  - File load
  - Node edit
  - Factory preview

### Current State
- Factory nodes already show validation errors from preview
- Need to extend to regular nodes and connection validation

### Implementation

**Backend: `netrun-ui/backend/main.py`**
- Add `POST /api/validate` endpoint
- Validate entire graph or individual nodes
- Return list of validation errors with node IDs

**Modify: `netrun-ui/src/lib/components/NetrunNode.svelte`**
- Show red border when `data.isValid === false`
- Show error icon with tooltip

**Modify: `netrun-ui/src/lib/stores/flowStore.ts`**
- Add `validateGraph()` function that calls backend
- Update node validation state based on response

**Modify: `netrun-ui/src/lib/components/Sidebar.svelte`**
- Show validation errors section when node has errors

---

## Feature 4: Recent Files

### Requirements
- Remember recently opened files
- Show in welcome screen / file menu
- Persist across sessions (localStorage)
- Limit to last 10 files

### Implementation

**New file: `netrun-ui/src/lib/stores/recentFilesStore.ts`**
```typescript
interface RecentFile {
  path: string;
  name: string;
  lastOpened: number; // timestamp
}

export const recentFiles = writable<RecentFile[]>([]);

export function addRecentFile(path: string): void;
export function removeRecentFile(path: string): void;
export function loadRecentFiles(): void; // from localStorage
export function saveRecentFiles(): void; // to localStorage
```

**Modify: `netrun-ui/src/lib/stores/flowStore.ts`**
- Call `addRecentFile()` in `loadFromFile()`

**Modify: `netrun-ui/src/routes/+page.svelte`**
- Show recent files in empty state / welcome screen

**Modify: `netrun-ui/src/lib/components/FileExplorer.svelte`**
- Add "Recent Files" section at top

---

## Implementation Order

1. **Copy/Paste Nodes** - Self-contained, useful immediately
2. **Recent Files** - Quick win, improves UX
3. **NetConfig/Pool Editing** - Extends sidebar functionality
4. **Validation** - Requires backend changes, more complex

---

## Files to Create/Modify

### New Files
- `netrun-ui/src/lib/stores/clipboardStore.ts`
- `netrun-ui/src/lib/stores/recentFilesStore.ts`

### Modified Files
- `netrun-ui/src/lib/stores/flowStore.ts`
- `netrun-ui/src/lib/components/Toolbar.svelte`
- `netrun-ui/src/lib/components/Sidebar.svelte`
- `netrun-ui/src/lib/components/NetrunNode.svelte`
- `netrun-ui/src/lib/components/FileExplorer.svelte`
- `netrun-ui/src/routes/+page.svelte`
- `netrun-ui/backend/main.py` (for validation endpoint)

---

## Verification

### Copy/Paste
1. Select nodes, Cmd+C, Cmd+V - nodes should paste with new IDs
2. Copy in one tab, paste in another - should work
3. Cmd+X should cut (copy + delete original)

### Recent Files
1. Open a file - should appear in recent files
2. Close and reopen app - recent files should persist
3. Click recent file - should open it

### NetConfig Editing
1. Click empty canvas - sidebar should show net settings
2. Edit pool config - should update extraData
3. Save file - pool config should be preserved

### Validation
1. Create invalid node config - should show red border
2. Fix the config - red border should disappear
3. Check sidebar for error details
