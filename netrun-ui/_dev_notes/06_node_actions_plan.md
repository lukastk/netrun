# Plan: Node Actions (Custom Commands/URIs)

## Overview

Allow users to define custom commands/scripts that can be executed from the UI, with template variables for dynamic paths. Actions can be defined at the project level (apply to all nodes) or at the node level.

## Use Cases

- Open node's source code in VS Code: `code $PROJECT_ROOT/src/$NODE_NAME.py`
- Run tests for a node: `pytest $PROJECT_ROOT/tests/test_$NODE_NAME.py -v`
- Open documentation: `open https://docs.example.com/$NODE_NAME`
- Custom build/deploy scripts

---

## Data Model

### Graph-level Configuration (stored in `meta.ui`)

```json
{
  "meta": {
    "ui": {
      "projectRoot": "./",
      "defaultCmd": "code",
      "env": {
        "CUSTOM_VAR": "some_value",
        "SRC_DIR": "src/nodes"
      },
      "actions": [
        {
          "id": "open-source",
          "label": "Open Source",
          "command": "$DEFAULT_CMD $PROJECT_ROOT/$SRC_DIR/$NODE_NAME.py",
          "icon": "code"
        },
        {
          "id": "run-tests",
          "label": "Run Tests",
          "command": "pytest $PROJECT_ROOT/tests/test_$NODE_NAME.py -v",
          "icon": "test"
        }
      ]
    }
  }
}
```

### Node-level Configuration (stored in node's `meta.ui`)

```json
{
  "meta": {
    "ui": {
      "actions": [
        {
          "id": "custom-action",
          "label": "Deploy Node",
          "command": "./scripts/deploy.sh $NODE_NAME"
        }
      ],
      "disabledActions": ["run-tests"]
    }
  }
}
```

### Built-in Variables

| Variable | Description |
|----------|-------------|
| `$NODE_NAME` | The node's label/name |
| `$NODE_ID` | The node's internal ID |
| `$NET_FILE_PATH` | Full path to the .netrun.json file |
| `$NET_FILE_DIR` | Directory containing the net file |
| `$PROJECT_ROOT` | Configured project root (resolved to absolute path) |
| `$DEFAULT_CMD` | Configured default command |

Plus any custom variables defined in `env`.

---

## Backend

### New Endpoint: `POST /api/actions/execute`

**Request:**
```json
{
  "command": "code /path/to/file.py",
  "workingDirectory": "/path/to/project",
  "env": {
    "CUSTOM_VAR": "value"
  }
}
```

**Response:**
```json
{
  "success": true,
  "exitCode": 0,
  "stdout": "...",
  "stderr": "..."
}
```

**Security considerations:**
- Commands execute in the context of the backend server
- Working directory is constrained to project root
- Consider adding a confirmation dialog for first-time command execution
- Could add command whitelist/blacklist in future

### Variable Resolution

Backend helper to resolve variables:
```python
def resolve_command(
    command: str,
    node_name: str,
    node_id: str,
    net_file_path: str,
    project_root: str,
    default_cmd: str,
    custom_env: dict[str, str]
) -> tuple[str, dict[str, str]]:
    """Resolve template variables in command and return (command, env_dict)"""
```

---

## Frontend

### 1. Project Settings Panel

New component or section in sidebar for configuring project-level settings:

```
┌─────────────────────────────────┐
│ Project Settings           [×] │
├─────────────────────────────────┤
│ Project Root                    │
│ ┌─────────────────────────────┐ │
│ │ ./                          │ │
│ └─────────────────────────────┘ │
│ (relative to net file)          │
│                                 │
│ Default Command                 │
│ ┌─────────────────────────────┐ │
│ │ code                        │ │
│ └─────────────────────────────┘ │
│                                 │
│ Environment Variables           │
│ ┌─────────────────────────────┐ │
│ │ SRC_DIR = src/nodes         │ │
│ │ TEST_DIR = tests            │ │
│ │ [+ Add Variable]            │ │
│ └─────────────────────────────┘ │
│                                 │
│ Default Actions                 │
│ ┌─────────────────────────────┐ │
│ │ 📝 Open Source              │ │
│ │    $DEFAULT_CMD $PROJECT... │ │
│ │ 🧪 Run Tests                │ │
│ │    pytest $PROJECT_ROOT/... │ │
│ │ [+ Add Action]              │ │
│ └─────────────────────────────┘ │
└─────────────────────────────────┘
```

### 2. Sidebar Actions Section

When a node is selected, show available actions:

```
┌─────────────────────────────────┐
│ Actions                         │
├─────────────────────────────────┤
│ [▶ Open Source]                 │
│ [▶ Run Tests]                   │
│ [▶ Deploy Node]  (node-specific)│
│                                 │
│ [+ Add Action]  [⚙ Settings]    │
└─────────────────────────────────┘
```

### 3. Action Editor Modal

For adding/editing actions:

```
┌─────────────────────────────────────┐
│ Edit Action                    [×]  │
├─────────────────────────────────────┤
│ Label                               │
│ ┌─────────────────────────────────┐ │
│ │ Open Source                     │ │
│ └─────────────────────────────────┘ │
│                                     │
│ Command                             │
│ ┌─────────────────────────────────┐ │
│ │ $DEFAULT_CMD \                  │ │
│ │   $PROJECT_ROOT/src/$NODE_NAME  │ │
│ │   .py                           │ │
│ └─────────────────────────────────┘ │
│ (textarea, resizable)               │
│                                     │
│ Available variables:                │
│ $NODE_NAME, $PROJECT_ROOT, ...      │
│                                     │
│ [Cancel]              [Save Action] │
└─────────────────────────────────────┘
```

### 4. Command Execution Feedback

When running a command:
- Show loading spinner on button
- On success: Brief toast notification
- On error: Show error modal with stderr output

---

## Implementation Phases

### Phase 1: Backend (execution endpoint)
1. Add `POST /api/actions/execute` endpoint
2. Implement variable resolution
3. Add working directory support
4. Return stdout/stderr

### Phase 2: Data Model & Storage
1. Update GraphMeta type to include action settings
2. Update node data type to include node-specific actions
3. Ensure round-trip serialization works

### Phase 3: Sidebar Actions UI
1. Create Actions section in Sidebar
2. Display project-level actions for selected node
3. Wire up execution to backend
4. Add loading/error states

### Phase 4: Settings Panel
1. Create ProjectSettings component
2. Add env variable editor
3. Add default actions editor
4. Add project root / default cmd inputs

### Phase 5: Node-specific Actions
1. Add action editor to node sidebar section
2. Allow adding node-specific actions
3. Allow disabling project-level actions per node

---

## Files to Create

| File | Purpose |
|------|---------|
| `backend/app/routes/actions.py` | Execute endpoint |
| `src/lib/components/ActionsPanel.svelte` | Actions section in sidebar |
| `src/lib/components/ActionEditor.svelte` | Modal for editing actions |
| `src/lib/components/ProjectSettings.svelte` | Project settings panel |
| `src/lib/stores/actionsStore.ts` | Action execution and state |

## Files to Modify

| File | Changes |
|------|---------|
| `backend/app/main.py` | Register actions router |
| `src/lib/api.ts` | Add executeAction API method |
| `src/lib/components/Sidebar.svelte` | Add ActionsPanel |
| `src/lib/stores/flowStore.ts` | Add graphMeta actions helpers |

---

## Example Workflow

1. User opens a netrun file
2. Goes to Project Settings, sets:
   - `PROJECT_ROOT`: `./`
   - `DEFAULT_CMD`: `code`
   - Adds action: "Open Source" → `$DEFAULT_CMD $PROJECT_ROOT/src/$NODE_NAME.py`
3. Saves the file (settings persist in meta.ui)
4. Selects a node named "DataProcessor"
5. Clicks "Open Source" button in sidebar
6. Backend executes: `code /full/path/to/project/src/DataProcessor.py`
7. VS Code opens the file

---

## Security Notes

- Commands run with same permissions as backend process
- Working directory constrained to project
- Consider adding:
  - Command execution logging
  - Optional confirmation dialogs
  - Command history in UI
