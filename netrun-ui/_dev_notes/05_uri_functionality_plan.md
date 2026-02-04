# Plan: URI Functionality for netrun-ui

## Overview

Implement URL-based navigation and deep linking for netrun-ui. This allows:
- Opening files directly via URL query parameters
- Sharing links to specific files
- Browser history support (back/forward navigation)
- Bookmarkable workspace states

## Features

### 1. File Opening via URL Parameter

**URL Format:**
```
http://localhost:5173/?file=/path/to/file.netrun.json
http://localhost:5173/?file=~/projects/my_flow.netrun.toml
```

**Behavior:**
- On page load, check for `?file=` query parameter
- If present, automatically open the file in a new tab
- Show error modal if file doesn't exist or fails to load
- Update URL when opening files via other methods (file explorer, recent files, etc.)

### 2. Multi-File Support

**URL Format:**
```
http://localhost:5173/?file=/path/to/file1.netrun.json&file=/path/to/file2.netrun.json
```

**Behavior:**
- Support multiple `file` parameters
- Open each file in its own tab
- First file becomes the active tab

### 3. Node Selection via URL (Optional Enhancement)

**URL Format:**
```
http://localhost:5173/?file=/path/to/file.netrun.json&node=MyNodeName
```

**Behavior:**
- After loading file, select the specified node
- Center viewport on the selected node

### 4. Browser History Integration

**Behavior:**
- Update URL when switching tabs (active file path)
- Browser back/forward buttons navigate between recently viewed files
- URL reflects current state without full page reload

---

## Implementation

### Phase 1: Basic File Parameter (Core Feature)

#### 1.1 Create Page Load Handler

**File: `src/routes/+page.ts`** (new file)

```typescript
import type { PageLoad } from './$types';

export const load: PageLoad = ({ url }) => {
    const fileParams = url.searchParams.getAll('file');
    const nodeParam = url.searchParams.get('node');

    return {
        initialFiles: fileParams,
        initialNode: nodeParam,
    };
};
```

#### 1.2 Update Page Component

**File: `src/routes/+page.svelte`**

- Accept `data` prop from load function
- On mount, process `data.initialFiles`
- Open each file using `loadFromFile()`
- Handle errors with modal alerts

#### 1.3 URL Update on File Open

**File: `src/lib/stores/flowStore.ts`**

- After successful file load, update browser URL
- Use `history.replaceState()` or SvelteKit's `goto()`
- Preserve other query parameters

### Phase 2: History Integration

#### 2.1 Create URL Store

**File: `src/lib/stores/urlStore.ts`** (new file)

```typescript
import { browser } from '$app/environment';
import { goto } from '$app/navigation';

export function updateUrlWithFile(filePath: string | null): void {
    if (!browser) return;

    const url = new URL(window.location.href);
    if (filePath) {
        url.searchParams.set('file', filePath);
    } else {
        url.searchParams.delete('file');
    }

    goto(url.toString(), { replaceState: true, noScroll: true });
}

export function updateUrlWithFiles(filePaths: string[]): void {
    if (!browser) return;

    const url = new URL(window.location.href);
    url.searchParams.delete('file');
    filePaths.forEach(path => url.searchParams.append('file', path));

    goto(url.toString(), { replaceState: true, noScroll: true });
}
```

#### 2.2 Integrate with Tab Switching

- When active tab changes, update URL to reflect current file
- When all tabs closed, clear file parameter from URL

### Phase 3: Node Selection (Enhancement)

#### 3.1 Add Node Parameter Handling

- Parse `node` query parameter
- After file loads, find node by name and select it
- Use `fitView` to center on selected node

---

## Files to Create

| File | Purpose |
|------|---------|
| `src/routes/+page.ts` | Page load function for query params |
| `src/lib/stores/urlStore.ts` | URL manipulation utilities |

## Files to Modify

| File | Changes |
|------|---------|
| `src/routes/+page.svelte` | Handle initial files from load, update URL on changes |
| `src/lib/stores/flowStore.ts` | Call URL update after file operations |
| `src/lib/stores/tabsStore.ts` | Call URL update on tab switch |

---

## Edge Cases

1. **Invalid file path**: Show error modal, don't crash
2. **File not found**: Show error modal with path
3. **Permission denied**: Show appropriate error
4. **URL encoding**: Handle special characters in paths (spaces, unicode)
5. **Relative paths**: Resolve relative to some base (home directory?)
6. **Empty file parameter**: Ignore `?file=` with empty value
7. **Duplicate files**: Don't open same file twice (existing behavior)

---

## Testing

1. Load app with `?file=/valid/path.netrun.json` - should auto-open
2. Load app with `?file=/invalid/path.json` - should show error
3. Load app with multiple `?file=` params - should open all
4. Open file via UI - URL should update
5. Switch tabs - URL should update to show active file
6. Browser back button - should navigate to previous file
7. Copy URL, open in new tab - should restore state

---

## Security Considerations

- File paths are handled by backend, which validates access
- No arbitrary code execution from URL parameters
- Backend already restricts file access to allowed paths

---

## Implementation Order

1. Create `+page.ts` with query param extraction
2. Update `+page.svelte` to handle initial files on mount
3. Create `urlStore.ts` with URL update utilities
4. Integrate URL updates into flowStore (file open/close)
5. Integrate URL updates into tabsStore (tab switch)
6. Add error handling for invalid files
7. Test all edge cases
