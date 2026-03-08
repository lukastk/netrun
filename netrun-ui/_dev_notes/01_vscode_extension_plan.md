# VS Code Extension for netrun-ui

## Overview

Create a VS Code custom editor extension that opens `.netrun.json` and `.netrun.toml` files in a visual flow editor — the same editor UI as the standalone netrun-ui, but integrated natively into VS Code. Each file opens in its own VS Code tab (no internal tab system). The extension lives in `netrun-ui/vscode/` and imports shared components from `../src/lib/`.

## Architecture

```
User opens .netrun.json in VS Code
  → VS Code activates extension
  → Extension starts FastAPI backend (if not running)
  → Extension creates a webview panel with the Svelte editor app
  → Webview loads file via backend REST API
  → User edits flow visually
  → Save triggers backend write via REST API
  → Extension tracks dirty state for VS Code's save indicators
```

### Key decisions

- **Option B**: Separate Svelte entry point in `vscode/` that imports from `../src/lib/`
- **Backend**: Same FastAPI backend, started as a child process by the extension
- **Communication**: Webview talks directly to backend via REST (same as standalone), plus `postMessage` for VS Code integration (dirty state, file path, save triggers)
- **No tabs**: VS Code handles tabs natively — each webview = one file
- **No file explorer**: VS Code has its own
- **No command palette**: VS Code has its own (but we keep webview-internal shortcuts for node operations)

### Directory structure

```
netrun-ui/
├── src/lib/                  # Shared (components, stores, API, utils)
│   ├── stores/
│   │   ├── editorStore.ts    # NEW — single-file editor state (extracted from flowStore)
│   │   ├── flowStore.ts      # Refactored — wraps editorStore with tab logic
│   │   ├── tabsStore.ts      # Unchanged — web-only tab management
│   │   └── ...               # Other stores unchanged
│   ├── components/
│   │   ├── EditorShell.svelte # NEW — shared editor chrome (toolbar, breadcrumb, canvas, sidebar)
│   │   └── ...               # Other components unchanged
│   └── api.ts                # Refactored — injectable base URL
├── src/routes/               # Web-only (SvelteKit pages)
│   └── +page.svelte          # Refactored — uses EditorShell, adds TabBar/FileExplorer/CommandPalette
├── vscode/
│   ├── package.json          # Extension manifest + npm deps
│   ├── tsconfig.json         # TypeScript config for extension host
│   ├── tsconfig.webview.json # TypeScript config for webview
│   ├── vite.config.ts        # Builds webview Svelte app
│   ├── esbuild.config.mjs    # Builds extension host code
│   ├── src/
│   │   ├── extension.ts      # Extension entry: activation, backend lifecycle
│   │   ├── editorProvider.ts # CustomTextEditorProvider implementation
│   │   └── webview/
│   │       ├── main.ts       # Webview entry point
│   │       └── App.svelte    # VS Code-specific root component
│   └── dist/                 # Built output (gitignored)
│       ├── extension.js      # Compiled extension host
│       └── webview/          # Compiled webview assets
```

---

## Implementation Plan

### Phase 1: Refactor shared code (prerequisite)

These changes are made in the existing `netrun-ui/src/lib/` code. The standalone web app must continue to work identically after each step.

#### Step 1.1: Make API base URL injectable

**File:** `src/lib/api.ts`

Currently:
```ts
export const API_BASE = import.meta.env.DEV ? 'http://127.0.0.1:8000/api' : '/api';
```

Change to:
```ts
let _apiBase = import.meta.env.DEV ? 'http://127.0.0.1:8000/api' : '/api';

export function setApiBase(url: string) {
  _apiBase = url;
}

export function getApiBase(): string {
  return _apiBase;
}
```

Update the `request()` method in the `ApiClient` class to use `getApiBase()` instead of the constant.

**Verification:** Web app works identically (no behavior change, just indirection).

#### Step 1.2: Extract `EditorStore` from `flowStore`

**Goal:** Create a self-contained store that manages one file's editing state (nodes, edges, history, extraData, graphExtra, dirty flag, validation, etc.) without any knowledge of tabs.

**New file:** `src/lib/stores/editorStore.ts`

This store encapsulates the "single file editor" concept:

```ts
export interface EditorState {
  filePath: string | null;
  fileName: string;
  isDirty: boolean;
  nodes: FlowNode[];
  edges: NetrunEdge[];
  history: { past: HistoryState[]; future: HistoryState[] };
  extraData: Record<string, unknown> | null;
  graphExtra: Record<string, unknown> | null;
  fileFormat: 'json' | 'toml';
  subgraphContext: SubgraphContext | null;
  isNewFile: boolean;
}

export function createEditorStore(initial?: Partial<EditorState>) {
  // Returns an object with:
  // - Readable stores: nodes, edges, isDirty, currentFilePath, extraData, etc.
  // - Mutation functions: addNode, updateNode, deleteNodes, pushHistory, undo, redo, etc.
  // - File ops: loadFile(path), saveFile(path?), reloadFile(), clearFlow()
  // - All the same functions currently in flowStore, but operating on internal state
  //   instead of activeTab
}
```

**Key design:**
- `createEditorStore()` is a factory function — each call creates an independent editor instance
- Internal state is a `writable<EditorState>` (not derived from `activeTab`)
- All mutation functions (addNode, deleteNodes, pushHistory, undo, redo, etc.) operate on this internal state
- Selection state (`selectedNodeIds`, `selectedEdgeIds`) lives inside the store instance
- The store exposes the same derived stores as flowStore currently does (`nodes`, `edges`, `isDirty`, etc.)

**What moves from flowStore to editorStore:**
- All node/edge CRUD functions
- History (pushHistory, undo, redo)
- Validation functions
- Extra data / graph extra management
- Factory preview functions
- Signal/control port injection
- Copy/paste/cut
- Interaction mode
- Subgraph creation
- Node selection (selectedNodeIds, selectedEdgeIds, selectedNode, etc.)
- Dependency/cascade highlight state

**What stays in flowStore (web-only):**
- Tab re-exports (tabs, activeTab, createTab, switchTab, etc.)
- `loadFromFile()` with tab-aware logic (check if already open, create/reuse tab)
- `saveInlineSubgraphToParent()` (operates across tabs)
- `allVisibleNodes`/`allVisibleEdges` (set by subgraphExpandStore, which is tab-aware)
- Recent files management
- Before-tab-switch handler registration
- Tab reload handler

**How flowStore uses editorStore:**
- flowStore maintains a map of `tabId → EditorStore` instances
- The "active editor" is derived from the active tab
- flowStore re-exports the active editor's stores so existing component imports don't break:
  ```ts
  // flowStore.ts (refactored)
  const editors = new Map<string, ReturnType<typeof createEditorStore>>();

  export const nodes = derived(activeTab, ($tab) => {
    const editor = editors.get($tab?.id);
    return editor ? get(editor.nodes) : [];
  });
  // ... same for edges, isDirty, etc.

  export function addNode(...args) {
    const editor = editors.get(get(activeTabId));
    editor?.addNode(...args);
  }
  ```

**Verification:** All existing imports from flowStore continue to work. Web app behavior unchanged.

#### Step 1.3: Extract `EditorShell` component

**New file:** `src/lib/components/EditorShell.svelte`

This component contains the shared editor UI:
- Toolbar (with file-operation buttons made optional via props)
- Breadcrumb (for subgraph navigation)
- Canvas container with `SvelteFlowProvider` + `FlowEditor`
- Sidebar
- FactorySelectorModal
- RecipeModal
- Modal (generic dialogs)

**Props:**
```ts
interface EditorShellProps {
  // The editor store instance to use
  editor: ReturnType<typeof createEditorStore>;
  // Optional: hide file-related toolbar buttons
  showFileButtons?: boolean;  // default true
  // Optional: hide tab-related toolbar buttons
  showTabButtons?: boolean;   // default true
  // Optional: callback when save is requested
  onSaveRequest?: () => void;
}
```

**Refactor `+page.svelte`** to use EditorShell:
```svelte
<div class="app">
  <TabBar />
  <div class="main-content">
    {#if showFileExplorer}
      <FileExplorer ... />
    {/if}
    <EditorShell editor={activeEditor} showFileButtons={true} showTabButtons={true} />
  </div>
  <CommandPalette />
</div>
```

**Challenge:** Many child components (Sidebar, Toolbar, FlowEditor, etc.) currently import directly from `flowStore`. After the refactor, they need to get their editor state from somewhere. Two approaches:

**Approach A — Svelte context:** EditorShell sets the editor store in Svelte context, and child components read from context instead of importing flowStore directly. This is the cleanest but requires touching every component that imports from flowStore.

**Approach B — Keep flowStore as the "active editor" facade:** flowStore continues to re-export the active editor's stores. Components keep importing from flowStore. The VS Code entry point sets up a single-editor flowStore without tabs. This is less work but means components are still coupled to flowStore as a module.

**Recommendation: Approach B.** It's significantly less disruptive. The key insight is that flowStore already acts as a facade — it exposes derived stores from the active tab. We just need to make it so that flowStore can also work with a single editor (no tabs). This can be done with a simple initialization flag:

```ts
// flowStore.ts
let singleEditorMode = false;
let singleEditor: ReturnType<typeof createEditorStore> | null = null;

export function initSingleEditorMode(editor: ReturnType<typeof createEditorStore>) {
  singleEditorMode = true;
  singleEditor = editor;
}
```

Then all the derived stores check `singleEditorMode` and return from `singleEditor` instead of `activeTab`. All the mutation functions similarly delegate to `singleEditor`.

**Alternatively**, and perhaps even simpler: the VS Code entry point just creates one tab in tabsStore and never creates more. The tab system still works, there's just always exactly one tab. The TabBar component is simply not rendered. This avoids any changes to flowStore at all.

**Revised recommendation: Use the "single tab" approach.** Create one tab, load the file into it, never show TabBar. This is by far the least invasive refactor:
- No changes to flowStore
- No changes to any component imports
- No EditorStore extraction needed (can be done later as a clean-up)
- The only change is what the root component renders

This means Phase 1 reduces to just:
1. Step 1.1 (injectable API base URL)
2. Step 1.3 (EditorShell component extraction) — but even simpler since we don't need the editor store prop

#### Step 1.3 (revised): Extract `EditorShell` component

**New file:** `src/lib/components/EditorShell.svelte`

Move the core editor layout from `+page.svelte` into this component:

```svelte
<!-- EditorShell.svelte -->
<script lang="ts">
  import { SvelteFlowProvider } from '@xyflow/svelte';
  import Toolbar from './Toolbar.svelte';
  import Breadcrumb from './Breadcrumb.svelte';
  import Sidebar from './Sidebar.svelte';
  import FlowEditor from './FlowEditor.svelte';
  import Modal from './Modal.svelte';
  import FactorySelectorModal from './FactorySelectorModal.svelte';
  import RecipeModal from './RecipeModal.svelte';
  // ... imports for stores used by the editor

  // Props to control what chrome is shown
  let {
    showFileButtons = true,
    onInit = undefined,
  }: {
    showFileButtons?: boolean;
    onInit?: () => void;
  } = $props();
</script>

<Toolbar {showFileButtons} />
<Breadcrumb />
<div class="editor-content">
  <div class="canvas-container">
    <SvelteFlowProvider>
      <FlowEditor />
    </SvelteFlowProvider>
  </div>
  <Sidebar />
</div>
<Modal />
{#if $factorySelectorState.isOpen}
  <FactorySelectorModal ... />
{/if}
{#if $recipeModalState.show}
  <RecipeModal ... />
{/if}
```

**Refactored `+page.svelte`:**
```svelte
<div class="app">
  <TabBar />
  <div class="main-content">
    {#if showFileExplorer}
      <FileExplorer ... />
    {/if}
    <div class="editor-container">
      <EditorShell />
      <!-- Empty state overlay -->
      {#if $nodes.length === 0 && !$currentFilePath && !$isNewFile}
        <div class="empty-state">...</div>
      {/if}
    </div>
  </div>
  <CommandPalette />
</div>
```

**Verification:** Web app looks and works identically.

#### Step 1.4: Add CORS support for VS Code webview origin

**File:** `netrun_ui_backend/main.py`

Add a `NETRUN_UI_CORS_ORIGINS` environment variable that, if set, adds extra allowed origins:

```python
extra_origins = os.environ.get("NETRUN_UI_CORS_ORIGINS", "").split(",")
origins = [
    "http://localhost:5173",
    "http://localhost:4173",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:4173",
] + [o.strip() for o in extra_origins if o.strip()]
```

For VS Code webviews, the simplest approach is to just allow all origins when running as an extension backend. The extension will set `NETRUN_UI_CORS_ORIGINS=*`.

Actually, VS Code webview origins are opaque (`vscode-webview://<id>`), so the cleanest solution is a flag:

```python
allow_all = os.environ.get("NETRUN_UI_ALLOW_ALL_ORIGINS", "").lower() in ("1", "true", "yes")
```

And if set, use `allow_origins=["*"]`.

**Verification:** Standalone app unchanged (env var not set). Extension can set it.

---

### Phase 2: VS Code extension scaffolding

#### Step 2.1: Extension manifest and project setup

**File:** `vscode/package.json`

```json
{
  "name": "netrun-ui",
  "displayName": "netrun-ui",
  "description": "Visual editor for netrun flow configurations",
  "version": "0.1.0",
  "engines": { "vscode": "^1.85.0" },
  "categories": ["Custom Editors"],
  "activationEvents": [],
  "main": "./dist/extension.js",
  "contributes": {
    "customEditors": [
      {
        "viewType": "netrun-ui.flowEditor",
        "displayName": "Netrun Flow Editor",
        "selector": [
          { "filenamePattern": "*.netrun.json" },
          { "filenamePattern": "*.netrun.toml" }
        ],
        "priority": "default"
      }
    ],
    "configuration": {
      "title": "netrun-ui",
      "properties": {
        "netrun-ui.pythonPath": {
          "type": "string",
          "default": "python",
          "description": "Path to Python interpreter with netrun installed"
        },
        "netrun-ui.backendPort": {
          "type": "number",
          "default": 0,
          "description": "Backend port (0 = auto-detect)"
        }
      }
    }
  },
  "scripts": {
    "build": "npm run build:extension && npm run build:webview",
    "build:extension": "node esbuild.config.mjs",
    "build:webview": "vite build",
    "dev": "npm run build:extension -- --watch & npm run build:webview -- --watch",
    "package": "vsce package"
  },
  "devDependencies": {
    "@types/vscode": "^1.85.0",
    "esbuild": "^0.20.0",
    "vite": "^7.3.1",
    "svelte": "^5.48.2",
    "@sveltejs/vite-plugin-svelte": "^6.2.4",
    "@xyflow/svelte": "^1.5.0",
    "elkjs": "^0.11.0",
    "typescript": "^5.9.3"
  }
}
```

**File:** `vscode/vite.config.ts`

```ts
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vite';
import path from 'path';

export default defineConfig({
  plugins: [svelte()],
  resolve: {
    alias: {
      '$lib': path.resolve(__dirname, '../src/lib'),
    },
  },
  build: {
    outDir: 'dist/webview',
    rollupOptions: {
      input: 'src/webview/main.ts',
      output: {
        entryFileNames: 'main.js',
        assetFileNames: '[name][extname]',
      },
    },
  },
  define: {
    __APP_VERSION__: JSON.stringify('vscode'),
  },
});
```

**File:** `vscode/esbuild.config.mjs`

```js
import * as esbuild from 'esbuild';

const watch = process.argv.includes('--watch');

const ctx = await esbuild.context({
  entryPoints: ['src/extension.ts'],
  bundle: true,
  outfile: 'dist/extension.js',
  external: ['vscode'],
  format: 'cjs',
  platform: 'node',
  sourcemap: true,
});

if (watch) {
  await ctx.watch();
} else {
  await ctx.rebuild();
  await ctx.dispose();
}
```

#### Step 2.2: Extension host — backend lifecycle management

**File:** `vscode/src/extension.ts`

```ts
import * as vscode from 'vscode';
import { NetrunEditorProvider } from './editorProvider';

let backendProcess: ChildProcess | null = null;
let backendPort: number = 0;

export async function activate(context: vscode.ExtensionContext) {
  // Start backend
  backendPort = await startBackend(context);

  // Register custom editor
  const provider = new NetrunEditorProvider(context, backendPort);
  context.subscriptions.push(
    vscode.window.registerCustomEditorProvider(
      'netrun-ui.flowEditor',
      provider,
      { supportsMultipleEditorsPerDocument: false }
    )
  );
}

export function deactivate() {
  if (backendProcess) {
    backendProcess.kill();
    backendProcess = null;
  }
}

async function startBackend(context: vscode.ExtensionContext): Promise<number> {
  const config = vscode.workspace.getConfiguration('netrun-ui');
  const pythonPath = config.get<string>('pythonPath', 'python');
  let port = config.get<number>('backendPort', 0);

  // Find free port if not specified
  if (port === 0) {
    port = await findFreePort();
  }

  const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath || os.homedir();

  backendProcess = spawn(pythonPath, [
    '-m', 'netrun_ui_backend.cli',
    '--server',
    '--port', String(port),
    '-C', workspaceFolder,
  ], {
    env: {
      ...process.env,
      NETRUN_UI_ALLOW_ALL_ORIGINS: '1',
    },
  });

  // Wait for health check
  await waitForServer(`http://127.0.0.1:${port}/health`);

  return port;
}
```

#### Step 2.3: Custom editor provider

**File:** `vscode/src/editorProvider.ts`

```ts
import * as vscode from 'vscode';

export class NetrunEditorProvider implements vscode.CustomTextEditorProvider {
  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly backendPort: number,
  ) {}

  async resolveCustomTextEditor(
    document: vscode.TextDocument,
    webviewPanel: vscode.WebviewPanel,
    _token: vscode.CancellationToken,
  ): Promise<void> {
    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(this.context.extensionUri, 'dist', 'webview'),
      ],
    };

    // Set webview HTML
    webviewPanel.webview.html = this.getHtmlForWebview(webviewPanel.webview);

    // Send initial config to webview
    webviewPanel.webview.postMessage({
      type: 'init',
      filePath: document.uri.fsPath,
      apiBase: `http://127.0.0.1:${this.backendPort}/api`,
    });

    // Handle messages from webview
    webviewPanel.webview.onDidReceiveMessage(async (message) => {
      switch (message.type) {
        case 'dirty':
          // Could track dirty state if using CustomDocument instead
          break;
        case 'requestSave':
          // Webview requests a save — the backend handles actual file write
          // We just need VS Code to know the document changed
          break;
      }
    });

    // Handle document changes from outside (e.g., git checkout)
    const changeSubscription = vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.uri.toString() === document.uri.toString() && e.contentChanges.length > 0) {
        webviewPanel.webview.postMessage({
          type: 'externalChange',
          filePath: document.uri.fsPath,
        });
      }
    });

    webviewPanel.onDidDispose(() => {
      changeSubscription.dispose();
    });
  }

  private getHtmlForWebview(webview: vscode.Webview): string {
    const scriptUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'dist', 'webview', 'main.js')
    );
    const styleUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'dist', 'webview', 'main.css')
    );
    const nonce = getNonce();

    return `<!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta http-equiv="Content-Security-Policy"
        content="default-src 'none';
          style-src ${webview.cspSource} 'unsafe-inline';
          script-src 'nonce-${nonce}';
          connect-src http://127.0.0.1:*;
          img-src ${webview.cspSource} http://127.0.0.1:* data:;">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <link rel="stylesheet" href="${styleUri}">
      <title>netrun-ui</title>
    </head>
    <body>
      <div id="app"></div>
      <script nonce="${nonce}" src="${scriptUri}"></script>
    </body>
    </html>`;
  }
}
```

#### Step 2.4: Webview entry point

**File:** `vscode/src/webview/main.ts`

```ts
import App from './App.svelte';
import { setApiBase } from '$lib/api';
import { mount } from 'svelte';

// VS Code webview API
const vscode = acquireVsCodeApi();

// Listen for init message from extension host
window.addEventListener('message', (event) => {
  const message = event.data;
  if (message.type === 'init') {
    setApiBase(message.apiBase);

    mount(App, {
      target: document.getElementById('app')!,
      props: {
        filePath: message.filePath,
        vscode,
      },
    });
  }
});
```

**File:** `vscode/src/webview/App.svelte`

```svelte
<script lang="ts">
  import { onMount } from 'svelte';
  import EditorShell from '$lib/components/EditorShell.svelte';
  import { loadFromFile, nodes, currentFilePath, isDirty } from '$lib/stores/flowStore';
  import { loadConfigSchema } from '$lib/stores/schemaStore';
  import { loadSignalTypes } from '$lib/stores/signalStore';
  import { loadControlTypes } from '$lib/stores/controlStore';
  import '$lib/configFieldRegistrations';

  let { filePath, vscode }: { filePath: string; vscode: any } = $props();

  // Track dirty state and notify extension host
  $effect(() => {
    vscode.postMessage({ type: 'dirty', isDirty: $isDirty });
  });

  onMount(async () => {
    // Initialize schema and config
    await Promise.all([
      loadConfigSchema(),
      loadSignalTypes(),
      loadControlTypes(),
    ]);

    // Load the file
    await loadFromFile(filePath);
  });
</script>

<div class="app">
  <EditorShell showFileButtons={false} />
</div>

<style>
  .app {
    height: 100vh;
    width: 100vw;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
</style>
```

---

### Phase 3: Polish and edge cases

#### Step 3.1: Handle save integration

The backend already writes files directly when `saveToFile()` calls `api.saveFile()`. For VS Code, this mostly works as-is since the backend has filesystem access. However, VS Code won't automatically detect the file changed (since the write bypasses VS Code's document model).

Options:
1. **Simple approach**: After `saveToFile()` succeeds, post a message to the extension host, which calls `vscode.commands.executeCommand('workbench.action.files.revert')` to refresh VS Code's view of the file. Since we're using `CustomTextEditorProvider`, VS Code knows about the document.
2. **Better approach**: Use `CustomReadonlyEditorProvider` or a custom document model where we control serialization. But this is more complex.

**Recommendation**: Start with option 1 (simple). The file is managed by the backend's REST API, and we just notify VS Code after writes.

#### Step 3.2: Handle external file changes

When the file changes on disk (git operations, external editor), the extension host receives `onDidChangeTextDocument`. It forwards this to the webview, which calls `reloadFile()`.

#### Step 3.3: Keyboard shortcut handling

VS Code webviews receive keyboard events when focused. Most editor-internal shortcuts (Cmd+Z undo, Cmd+C copy, etc.) work automatically. The key conflicts:

- **Cmd+S**: Override in extension's `keybindings` to trigger webview save when a netrun editor is active
- **Cmd+Shift+P**: Let VS Code handle this (opens VS Code command palette)
- **Cmd+W**: Let VS Code handle this (closes editor tab)

The webview's keyboard handler already checks for input focus before handling shortcuts, so most things work.

#### Step 3.4: Theme integration

VS Code webviews can access VS Code's CSS variables for theming. The netrun-ui already uses CSS variables (`--bg-primary`, `--text-primary`, etc.). We can map VS Code's theme variables to netrun-ui's variables in the webview HTML:

```css
:root {
  --bg-primary: var(--vscode-editor-background);
  --text-primary: var(--vscode-editor-foreground);
  --border-color: var(--vscode-panel-border);
  /* etc. */
}
```

This is a nice-to-have, not blocking for initial release.

---

## Implementation order

1. **Step 1.1** — Injectable API base URL (~15 min)
2. **Step 1.3** — Extract EditorShell component (~1-2 hours)
3. **Step 1.4** — CORS env var support (~15 min)
4. **Step 2.1** — Extension scaffolding (package.json, build config) (~1 hour)
5. **Step 2.2** — Extension host with backend lifecycle (~1-2 hours)
6. **Step 2.3** — Custom editor provider (~1 hour)
7. **Step 2.4** — Webview entry point + App.svelte (~1-2 hours)
8. **Step 3.1-3.4** — Polish (save, external changes, keys, theme) (~2-3 hours)

## What we're NOT doing (simplifications)

- **No EditorStore extraction** (Phase 1.2 was dropped) — We use the "single tab" approach instead. The webview creates one tab in tabsStore and never shows the TabBar. This avoids a large refactor of flowStore and all components that import from it. The EditorStore extraction can happen later as a clean-up if needed.
- **No VS Code command palette integration** — Keeping the webview's own command palette (Cmd+Shift+P conflict means users use Cmd+K or similar in webview). Can add VS Code commands later.
- **No theme integration** in v1 — The dark theme works fine in most VS Code themes.
- **No custom document model** — Using `CustomTextEditorProvider` (simpler) rather than `CustomEditorProvider` with custom document.

## Risks

1. **Python/netrun not installed**: Extension needs Python with netrun-ui-backend installed. Mitigation: clear error message on activation, configuration option for Python path.
2. **Port conflicts**: Multiple VS Code windows. Mitigation: reuse find_free_port() logic, or share one backend per workspace.
3. **CSP in webviews**: SvelteFlow may need specific CSP rules. Mitigation: test early, adjust CSP as needed.
4. **Svelte module resolution**: The `$lib` alias needs to resolve to `../src/lib/` in the Vite config. This should work but needs testing with all deep imports.
5. **Global store state**: If two webviews are open simultaneously, they share the same JS module state (same stores). This is a problem since flowStore uses module-level singletons. Mitigation: VS Code creates separate webview contexts per panel, so each gets its own JS environment. Verify this is the case.
