# Custom Modal Dialogs

## Overview

Replace ugly JavaScript `prompt()`, `alert()`, and `confirm()` dialogs with custom styled modals that match the app's dark theme.

## Current Usage

The app currently uses native browser dialogs in these places:

1. **prompt()** - Used for:
   - New file creation (filename input)
   - Open file (path input)
   - Save As (path input)
   - Factory node creation (import path input)
   - Create subgraph (name input)
   - Navigate to path in file explorer

2. **alert()** - Used for:
   - Error messages (save failed, open failed, etc.)
   - Success messages (subgraph saved to parent)
   - Validation results

3. **confirm()** - Used for:
   - Unsaved changes warning
   - Close tab with unsaved changes

## Implementation Plan

### Phase 1: Create Modal Infrastructure

#### 1.1 Modal Store

**File: `src/lib/stores/modalStore.ts`**

```typescript
interface ModalState {
    isOpen: boolean;
    type: 'prompt' | 'alert' | 'confirm';
    title: string;
    message?: string;
    placeholder?: string;
    defaultValue?: string;
    inputType?: 'text' | 'path';  // 'path' shows file path styling
    confirmText?: string;
    cancelText?: string;
    onConfirm?: (value?: string) => void;
    onCancel?: () => void;
}

// Functions to show modals that return Promises
function showPrompt(options): Promise<string | null>;
function showAlert(options): Promise<void>;
function showConfirm(options): Promise<boolean>;
```

#### 1.2 Modal Component

**File: `src/lib/components/Modal.svelte`**

Features:
- Dark themed overlay with blur
- Centered modal card
- Title and optional message
- Input field for prompts (with placeholder)
- Confirm/Cancel buttons
- Keyboard support (Enter to confirm, Escape to cancel)
- Focus trap
- Click outside to cancel (for alerts)

UI Design:
```
┌─────────────────────────────────────────┐
│ ✕                                       │
│                                         │
│   Create New File                       │
│                                         │
│   Enter filename (relative or absolute) │
│   ┌─────────────────────────────────┐   │
│   │ my_flow.netrun.json             │   │
│   └─────────────────────────────────┘   │
│                                         │
│            [Cancel]  [Create]           │
└─────────────────────────────────────────┘
```

### Phase 2: Replace All Usages

#### 2.1 File Operations
- `commands.ts` - New File, Open File
- `Toolbar.svelte` - New, Open, Save, Save As
- `+page.svelte` - Empty state buttons

#### 2.2 Node Operations
- `Toolbar.svelte` - Add Factory Node
- `commands.ts` - Create Subgraph

#### 2.3 Confirmations
- `commands.ts` - Unsaved changes warnings
- `Toolbar.svelte` - Unsaved changes warnings
- `tabsStore.ts` - Close tab confirmation

#### 2.4 Alerts
- Error messages throughout
- Success messages

### Phase 3: Polish

1. **Animations** - Fade in/out, scale
2. **Accessibility** - ARIA labels, focus management
3. **Input validation** - Show error state for invalid input
4. **Path autocomplete** (future) - Could add file path suggestions

## Files to Create

1. `src/lib/stores/modalStore.ts` - Modal state and helper functions
2. `src/lib/components/Modal.svelte` - The modal component

## Files to Modify

1. `src/routes/+page.svelte` - Add Modal component, replace prompts
2. `src/lib/commands.ts` - Replace all prompt/alert/confirm calls
3. `src/lib/components/Toolbar.svelte` - Replace all prompt/alert/confirm calls
4. `src/lib/stores/tabsStore.ts` - Replace confirm for close tab
5. `src/lib/components/FileExplorer.svelte` - Replace prompt for navigate

## API Design

```typescript
// Simple prompt
const filename = await showPrompt({
    title: 'Create New File',
    message: 'Enter filename (relative or absolute)',
    placeholder: 'my_flow.netrun.json',
    defaultValue: 'my_flow.netrun.json',
});
if (filename) {
    // User entered a value
}

// Alert
await showAlert({
    title: 'Error',
    message: 'Failed to save file: Permission denied',
});

// Confirm
const confirmed = await showConfirm({
    title: 'Unsaved Changes',
    message: 'You have unsaved changes. Discard them?',
    confirmText: 'Discard',
    cancelText: 'Cancel',
});
if (confirmed) {
    // User confirmed
}
```

## Verification

1. Create a new file - modal appears with styled input
2. Open a file - modal appears
3. Try to create new file with unsaved changes - confirm modal appears
4. Close tab with unsaved changes - confirm modal appears
5. Error scenarios show alert modal
6. Keyboard navigation works (Tab, Enter, Escape)
7. Click outside closes modal (where appropriate)
