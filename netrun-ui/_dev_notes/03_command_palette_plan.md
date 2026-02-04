# Command Palette Implementation Plan

## Overview

Implement a VS Code-style command palette that provides quick access to all actions via a searchable interface invoked with a keyboard shortcut (Cmd+Shift+P / Ctrl+Shift+P).

## Features

1. **Fuzzy search** - Search commands by name or keywords
2. **Keyboard navigation** - Arrow keys to select, Enter to execute, Escape to close
3. **Keyboard shortcut display** - Show shortcuts next to commands
4. **Categories** - Group commands (File, Edit, View, Node, etc.)
5. **Recent commands** - Show recently used commands at top
6. **Extensible** - Easy to add new commands in the future (recipes, etc.)

## Implementation Plan

### Phase 1: Core Command Palette Component

#### 1.1 Create Command Registry

**File: `src/lib/stores/commandStore.ts`**

```typescript
interface Command {
    id: string;
    label: string;
    category: 'file' | 'edit' | 'view' | 'node' | 'subgraph';
    keywords?: string[];
    shortcut?: string;
    action: () => void | Promise<void>;
    enabled?: () => boolean; // Dynamic enable/disable
}

// Registry to hold all commands
export const commands = writable<Command[]>([]);
export const commandPaletteOpen = writable(false);
export const recentCommands = writable<string[]>([]); // Command IDs

// Register a command
export function registerCommand(command: Command): void;

// Unregister a command
export function unregisterCommand(id: string): void;

// Execute a command by ID
export function executeCommand(id: string): Promise<void>;

// Search commands by query
export function searchCommands(query: string): Command[];
```

#### 1.2 Create Command Palette Component

**File: `src/lib/components/CommandPalette.svelte`**

UI Structure:
```
┌──────────────────────────────────────────────────┐
│ 🔍 [Search commands...                         ] │
├──────────────────────────────────────────────────┤
│ Recent                                           │
│   📄 Save                                  ⌘S    │
│   ↩️ Undo                                  ⌘Z    │
├──────────────────────────────────────────────────┤
│ File                                             │
│   📄 New File                              ⌘N    │
│   📂 Open File                             ⌘O    │
│   💾 Save                                  ⌘S    │
│   💾 Save As...                        ⌘⇧S       │
├──────────────────────────────────────────────────┤
│ Edit                                             │
│   ↩️ Undo                                  ⌘Z    │
│   ↪️ Redo                              ⌘⇧Z       │
│   📋 Copy                                  ⌘C    │
│   📋 Paste                                 ⌘V    │
│   ✂️ Cut                                   ⌘X    │
├──────────────────────────────────────────────────┤
│ Node                                             │
│   ➕ Add Node                                    │
│   ⚙️ Add Factory Node                            │
│   ✓ Validate All                                 │
├──────────────────────────────────────────────────┤
│ Subgraph                                         │
│   📦 Create Subgraph                       ⌘G    │
└──────────────────────────────────────────────────┘
```

Features:
- Modal overlay with blur background
- Search input auto-focused when opened
- Keyboard navigation (↑↓ to select, Enter to execute, Esc to close)
- Click outside to close
- Fuzzy matching on label and keywords
- Highlight matching text in results
- Category headers (collapsible optional)
- Shortcut displayed on right

#### 1.3 Integrate with Main Layout

**File: `src/routes/+page.svelte`**

- Add CommandPalette component to layout
- Add global keyboard listener for Cmd+Shift+P

### Phase 2: Register All Existing Commands

#### 2.1 File Commands
- New File (Cmd+N)
- New Tab (Cmd+T)
- Open File (Cmd+O)
- Save (Cmd+S)
- Save As (Cmd+Shift+S)
- Close Tab (Cmd+W)

#### 2.2 Edit Commands
- Undo (Cmd+Z)
- Redo (Cmd+Shift+Z)
- Copy (Cmd+C)
- Paste (Cmd+V)
- Cut (Cmd+X)
- Select All (Cmd+A)

#### 2.3 View Commands
- Toggle Sidebar (future)
- Toggle Minimap (future)
- Zoom In
- Zoom Out
- Fit to View

#### 2.4 Node Commands
- Add Node
- Add Factory Node
- Delete Selected Nodes (Delete/Backspace)
- Validate All Nodes

#### 2.5 Subgraph Commands
- Create Subgraph from Selection (Cmd+G)

#### 2.6 Tab Commands
- Next Tab (Ctrl+Tab)
- Previous Tab (Ctrl+Shift+Tab)
- Go to Tab 1-9 (Cmd+1-9)

### Phase 3: Refactor Keyboard Shortcuts

Move all keyboard shortcut handling from Toolbar.svelte to a centralized system:

**File: `src/lib/stores/keyboardStore.ts`**

```typescript
// Map shortcut string to command ID
interface ShortcutBinding {
    key: string;           // e.g., "s", "z", "g"
    metaKey?: boolean;     // Cmd on Mac, Ctrl on Windows
    ctrlKey?: boolean;
    shiftKey?: boolean;
    altKey?: boolean;
    commandId: string;
}

export const shortcuts = writable<ShortcutBinding[]>([]);

// Global keyboard event handler
export function handleGlobalKeydown(event: KeyboardEvent): void;

// Register a shortcut
export function registerShortcut(binding: ShortcutBinding): void;

// Format shortcut for display (e.g., "⌘S", "⌘⇧Z")
export function formatShortcut(binding: ShortcutBinding): string;
```

### Phase 4: Polish

1. **Persist recent commands** - Store in localStorage
2. **Accessibility** - ARIA labels, focus management
3. **Animation** - Smooth open/close transitions
4. **Empty state** - "No commands found" message
5. **Category filtering** - Optional category prefix (e.g., ">file:" to filter)

## Files to Create

1. `src/lib/stores/commandStore.ts` - Command registry
2. `src/lib/stores/keyboardStore.ts` - Keyboard shortcut handling
3. `src/lib/components/CommandPalette.svelte` - Main component

## Files to Modify

1. `src/routes/+page.svelte` - Add CommandPalette to layout
2. `src/lib/components/Toolbar.svelte` - Remove inline keyboard handlers, use command system
3. `src/lib/components/FlowEditor.svelte` - Remove inline keyboard handlers if any

## Verification

1. Open command palette with Cmd+Shift+P
2. Search for "save" - shows Save and Save As commands
3. Navigate with arrow keys, execute with Enter
4. Verify all existing shortcuts still work
5. Verify recent commands appear at top after use
6. Verify disabled commands show as disabled (e.g., "Create Subgraph" when < 2 nodes selected)

## Dependencies

- None (pure frontend feature)

## Estimated Effort

- Phase 1: Command registry + basic UI
- Phase 2: Register all commands
- Phase 3: Refactor keyboard handling
- Phase 4: Polish
