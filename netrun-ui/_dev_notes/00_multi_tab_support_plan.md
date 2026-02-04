# Plan: Multi-Tab Support for netrun-ui

## Overview

Implement tabs for multiple open files in netrun-ui, allowing users to work with several `.netrun.json`/`.netrun.toml` files simultaneously.

## Approach

**Strategy: Indexed Tabs with Per-Tab State**

Create a `tabsStore.ts` that manages an array of tab states, with the existing flow store functions reading/writing to the active tab's state. This minimizes refactoring while providing complete state isolation per tab.

## Implementation Steps

### 1. Create Tab Store (`netrun-ui/src/lib/stores/tabsStore.ts`)

```typescript
interface TabState {
  id: string;
  filePath: string | null;
  fileName: string;
  isDirty: boolean;
  nodes: NetrunNode[];
  edges: NetrunEdge[];
  history: History;
  extraData: Record<string, unknown> | null;
  graphMeta: Record<string, unknown> | null;
  fileFormat: 'json' | 'toml';
}

// Stores
export const tabs = writable<TabState[]>([]);
export const activeTabId = writable<string | null>(null);
export const activeTab = derived([tabs, activeTabId], ...);

// Functions
export function createTab(filePath?: string): string;
export function closeTab(tabId: string): void;
export function switchTab(tabId: string): void;
export function getTabByFilePath(filePath: string): TabState | undefined;
```

### 2. Refactor Flow Store (`netrun-ui/src/lib/stores/flowStore.ts`)

Modify existing stores to read/write from the active tab:

- Change `nodes`, `edges`, `isDirty`, etc. to be derived from `activeTab`
- Update `loadFromFile()` to create a new tab or switch to existing
- Update `saveToFile()` to save the active tab
- Update `clearFlow()` to reset active tab or create new

### 3. Create Tab Bar Component (`netrun-ui/src/lib/components/TabBar.svelte`)

- Horizontal tab strip below the Toolbar
- Each tab shows: filename (or "Untitled"), dirty indicator (*), close button
- Active tab highlighted
- Click to switch tabs
- Middle-click or close button to close tab
- "New Tab" button at the end

### 4. Update Main Page Layout (`netrun-ui/src/routes/+page.svelte`)

```svelte
<Toolbar />
<TabBar />  <!-- NEW -->
<div class="main-content">
  ...
</div>
```

### 5. Update File Explorer Integration

When clicking a file in FileExplorer:
- If file is already open → switch to that tab
- Otherwise → open in new tab

### 6. Keyboard Shortcuts

- `Cmd+T` - New tab
- `Cmd+W` - Close current tab
- `Cmd+1-9` - Switch to tab by index
- `Ctrl+Tab` / `Ctrl+Shift+Tab` - Next/previous tab

## Files to Modify

1. **New**: `netrun-ui/src/lib/stores/tabsStore.ts`
2. **New**: `netrun-ui/src/lib/components/TabBar.svelte`
3. **Modify**: `netrun-ui/src/lib/stores/flowStore.ts` - Integrate with tabs
4. **Modify**: `netrun-ui/src/routes/+page.svelte` - Add TabBar, update layout
5. **Modify**: `netrun-ui/src/lib/components/FileExplorer.svelte` - Open in tab
6. **Modify**: `netrun-ui/src/lib/components/Toolbar.svelte` - Update file name display

## UI Design

```
┌─────────────────────────────────────────────────────────────────┐
│ [Open] [Save] [Undo] [Redo]    │    [+ Node] [+ Factory]       │ <- Toolbar
├─────────────────────────────────────────────────────────────────┤
│ [example.netrun.json *] [simple.netrun.toml] [Untitled] [+]    │ <- TabBar
├──────────┬────────────────────────────────────────┬─────────────┤
│ Explorer │          Canvas (SvelteFlow)           │   Sidebar   │
│          │                                        │             │
│          │                                        │             │
└──────────┴────────────────────────────────────────┴─────────────┘
```

## Tab Behavior

1. **Opening files**: Creates new tab or switches to existing
2. **New file**: Creates tab with "Untitled" name
3. **Close tab with unsaved changes**: Show confirmation dialog
4. **Close last tab**: Keep one empty "Untitled" tab
5. **Dirty indicator**: Show `*` after filename when unsaved

## Verification

1. Start the UI with `./start.sh`
2. Open a file from explorer → should create new tab
3. Open another file → should create second tab
4. Click between tabs → canvas should switch
5. Edit nodes → dirty indicator should appear
6. Save → dirty indicator should clear
7. Close tab with unsaved changes → should show confirmation
8. Cmd+T → should create new empty tab
9. Cmd+W → should close current tab
