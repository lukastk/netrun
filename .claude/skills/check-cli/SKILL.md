---
name: check-cli
description: Checklist to verify the netrun CLI is up-to-date after adding or changing a netrun feature. Run this after any feature work.
disable-model-invocation: true
---

# netrun CLI Update Checklist

Run this checklist after adding or modifying any netrun feature to ensure the CLI, tests, docs, and UI backend are all in sync.

The feature being checked: **$ARGUMENTS** (if blank, check everything that's changed since the last commit on main).

---

## Phase 1: Identify What Changed

Determine what was added or modified. Classify the change:

- [ ] **New config field** (e.g., new field on `NodeConfig`, `NetConfig`, `NodeExecutionConfig`, `GraphConfig`)
- [ ] **New factory** or factory parameter
- [ ] **New CLI command**
- [ ] **Modified CLI command** (new flag, changed behavior)
- [ ] **New node feature** (e.g., new execution context method, new lifecycle hook)
- [ ] **New salvo condition type** or port feature
- [ ] **New pool type** or pool option
- [ ] **Other** (describe)

---

## Phase 2: CLI Commands

Check each relevant CLI command to see if it needs updating. The commands live in `netrun/pts/netrun/10_cli/` (never edit `src/netrun/cli/` directly).

### Inspection commands (`02_commands.pct.py`)

| Command | What it exposes | Check |
|---------|----------------|-------|
| `validate` | All config fields (pydantic + graph validation + Rust sim) | Does validation catch invalid values for the new feature? Are error messages clear? |
| `info` | Summary stats: node count, edges, pools, factories, actions, recipes | Does `info` surface the new feature if it's a top-level concept? |
| `structure` | Graph topology: nodes with ports, edges, salvos, factory info | If ports, salvos, or factories changed, does `structure` show them? |
| `nodes` | Node list with port names and factory | If ports changed, does `nodes` reflect them? |
| `node NAME` | Full node detail: ports, salvos, factory, factory_args, execution_config, extra | Does `node` show the new field? Is it in `execution_config` output? |
| `factory-info` | Factory parameters, types, defaults | If a factory changed, does `factory-info` reflect new params? |
| `convert` | JSON <-> TOML round-trip | Does the new feature survive a convert round-trip without data loss? |

### Graph mutation commands (`06_graph.pct.py`)

| Command | What it does | Check |
|---------|-------------|-------|
| `add-node` | Creates a node dict from flags or stdin JSON | Can the new feature be set via `--json` stdin? Should a new flag be added? |
| `remove-node` | Removes node + connected edges + output_queue refs | If the feature adds new cross-references (like output_queues reference nodes), does removal clean them up? |
| `edit-node` | Rename, port add/remove, `--merge` | Can the new feature be set via `--merge`? If it's a commonly-used field, does it warrant a dedicated flag? |
| `add-edge` | Adds edge, validates nodes, warns fan-out | If edge semantics changed (new edge types, new validation rules), is `add-edge` updated? |
| `remove-edge` | Removes edge by source/target | Same as above for removal. |

### Subcommands

| Command | Check |
|---------|-------|
| `actions list/run` (`03_actions.pct.py`) | If action context or template vars changed, are they reflected? |
| `recipes list/run` (`04_recipes.pct.py`) | If recipe config changed, is the runner updated? |

### Helpers (`01_helpers.pct.py`)

- [ ] If a new config field affects validation: does `validate_after_write()` catch it?
- [ ] If the change affects config file format: does `load_raw_data()` / `write_config_data()` handle it?
- [ ] If new node lookup logic is needed: is `get_node_by_name()` still sufficient?

### Registration (`00_app.pct.py`)

- [ ] If a new command was added: is it registered in `00_app.pct.py`?

---

## Phase 3: Tests

Test files live in `netrun/pts/tests/10_cli/`. Check:

- [ ] **Existing tests still pass**: `cd netrun && uv run pytest src/tests/cli/ -v`
- [ ] **New feature has test coverage**:
  - If a new command was added: tests in `test_cli_graph.pct.py` or `test_cli.pct.py`
  - If an existing command changed: updated assertions in relevant test file
  - If validation changed: test that invalid configs produce the right errors
- [ ] **Test against sample projects**: Do any sample projects exercise the new feature? If so, run the CLI commands against them:
  ```bash
  netrun validate -c sample_projects/00_basic_net_project/main.netrun.json
  netrun info -c sample_projects/00_basic_net_project/main.netrun.json
  ```

---

## Phase 4: nblite Export

After editing any `.pct.py` file:

```bash
cd /Users/lukas/dev/20260113_w3pmcj__netrun2/netrun
nbl export --reverse && nbl export
```

Then re-run tests to confirm generated code is correct:

```bash
uv run pytest src/tests/cli/ -v
```

---

## Phase 5: Documentation

### Agent docs (used by AI agents and as reference)

- [ ] **`agents/NETRUN_INSTRUCTIONS.md`** (Section 17 — CLI Reference):
  - New commands added to the command table?
  - New options documented?
  - New config fields mentioned in relevant sections (e.g., Section 9 for execution config, Section 11 for node variables)?

- [ ] **`agents/NETRUN_INSTRUCTIONS_CONCISE.md`** (CLI section, ~line 211):
  - Command table updated?
  - Quick reference for new features added to relevant section?

- [ ] **`agents/SKILL.md`** (CLI skill for graph management):
  - New commands or flags documented?
  - Common workflows section updated if the feature changes how users build pipelines?

### Project CLAUDE.md

- [ ] **`CLAUDE.md`** (root): If the feature is architecturally significant (new module, new config model, new protocol), is it documented in the relevant section?

---

## Phase 6: UI Backend

The netrun-ui backend (`netrun-ui/netrun_ui_backend/`) mirrors some CLI functionality via HTTP endpoints. Check if the backend needs updating:

| Backend route | CLI equivalent | Check |
|---------------|---------------|-------|
| `routes/schema.py` — `/api/config/schema` | (config introspection) | New config fields exposed in schema? New types added? |
| `routes/factories.py` — `/api/factories/info` | `factory-info` | Factory param changes reflected? |
| `routes/files.py` — `/api/files/validate` | `validate` | Validation logic in sync? |
| `routes/actions.py` | `actions list/run` | Action context changes reflected? |
| `routes/recipes.py` | `recipes list/run` | Recipe config changes reflected? |

Not every CLI change requires a backend update — only when the feature affects config structure, validation, or introspection that the UI relies on.

---

## Phase 7: Sample Projects

If the feature is significant enough to demonstrate:

- [ ] Does an existing sample project already cover it? If so, does it still work correctly?
- [ ] Should a new sample project be created? (Only for major features — see `sample_projects/` numbering convention)
- [ ] Run the full test suite against sample projects if any were modified.

---

## Summary Checklist (Quick Reference)

For a quick pass, confirm these are all done:

1. [ ] CLI source updated (`pts/netrun/10_cli/*.pct.py`)
2. [ ] New command registered in `00_app.pct.py` (if applicable)
3. [ ] Tests added/updated (`pts/tests/10_cli/*.pct.py`)
4. [ ] `nbl export --reverse && nbl export` run successfully
5. [ ] All CLI tests pass: `uv run pytest src/tests/cli/ -v`
6. [ ] `agents/NETRUN_INSTRUCTIONS.md` Section 17 updated
7. [ ] `agents/NETRUN_INSTRUCTIONS_CONCISE.md` CLI section updated
8. [ ] `agents/SKILL.md` updated (if graph management commands changed)
9. [ ] UI backend checked for sync (if config structure changed)
10. [ ] Sample projects verified (if applicable)
