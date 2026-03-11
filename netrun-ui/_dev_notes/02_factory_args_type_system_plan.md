# Factory Arguments Type System Plan

## Problem

The factory arguments UI currently uses string-matching heuristics to detect parameter types (`isBoolParam`, `isIntParam`, etc.). Any type not matching a primitive is treated as an "import path" — which is wrong for complex types like `list[str] | dict[str, int]`.

**Issues to fix:**

1. **Fallback shows "(import path)"** for unrecognized types — should show an error instead
2. **No union type support** — `bool | int`, `str | SomeEnum`, etc. have no way to switch between types
3. **No list support** — `list[str]`, `list[int]`, `list[bool]`, etc.
4. **No dict support** — `dict[str, X]` where X is any supported type

## Design

### Structured Type Representation

Replace the raw `type: str` with a structured `TypeInfo` that the frontend can use to select the correct editor widget.

**TypeInfo schema (recursive):**

```
TypeInfo =
  | { kind: "str" }
  | { kind: "int" }
  | { kind: "float" }
  | { kind: "bool" }
  | { kind: "enum", options: string[] }
  | { kind: "list", item: TypeInfo }
  | { kind: "dict", value: TypeInfo }        # key is always str
  | { kind: "union", variants: TypeInfo[] }  # 2+ known variants, None excluded
  | { kind: "unknown", raw: string | null }  # unsupported type → error in UI
```

**Parameter-level optionality:** `T | None` is represented as `optional: true` on the parameter, with `TypeInfo` containing only the non-None type. For example:
- `int | None` → `optional: true`, `type_info: { kind: "int" }`
- `str | int | None` → `optional: true`, `type_info: { kind: "union", variants: [{kind:"str"}, {kind:"int"}] }`
- `list[str] | dict[str, int]` → `optional: false`, `type_info: { kind: "union", variants: [...] }`

**Simplification rules for unions:**
- Strip `None` from unions → set `optional` flag
- Filter out `unknown` variants (e.g. `Callable` in `Callable | str`)
- If one known variant remains after filtering → unwrap to that variant (not a union)
- If zero known variants remain → `kind: "unknown"`
- If 2+ known variants remain → `kind: "union"`

This handles `func: Callable | str` correctly: Callable is unknown, gets filtered out, leaving just `str` → renders as text input.

### No-annotation parameters

Parameters without type annotations (like some older factory args) get `type_info: { kind: "str" }` as a reasonable default, since most untyped factory args are strings/import paths.

### Updated FactoryParameter model

```python
class FactoryParameter(BaseModel):
    name: str
    type: str | None = None            # raw type string (for display)
    type_info: TypeInfo | None = None   # structured type (for widget selection)
    optional: bool = False              # whether None is allowed
    default: Any | None = None
    has_default: bool = False
    enum_options: list[str] | None = None  # kept for backward compat
```

---

## Implementation Steps

### Step 1: Backend — TypeInfo model and parser

**File:** `netrun-ui/netrun_ui_backend/routes/factories.py`

1. Define `TypeInfo` as a Pydantic model with a `kind` discriminator:

```python
class TypeInfo(BaseModel):
    kind: str  # "str", "int", "float", "bool", "enum", "list", "dict", "union", "unknown"
    # For enum:
    options: list[str] | None = None
    # For list:
    item: "TypeInfo | None" = None
    # For dict:
    value: "TypeInfo | None" = None
    # For union:
    variants: list["TypeInfo"] | None = None
    # For unknown:
    raw: str | None = None
```

2. Write `parse_type_annotation(annotation) -> tuple[TypeInfo, bool]`:
   - Uses `typing.get_origin()` and `typing.get_args()` to inspect generics
   - Recognizes `list[T]` via `get_origin(ann) is list`
   - Recognizes `dict[K, V]` via `get_origin(ann) is dict`
   - Recognizes `Union` / `|` via `get_origin(ann) is types.UnionType or typing.Union`
   - Recognizes `Enum` subclasses → `kind: "enum"` with options
   - Recognizes primitives `str`, `int`, `float`, `bool` → `kind: "<name>"`
   - Separates `None` from unions → returns `(type_info, is_optional)`
   - Filters unknown variants from unions, simplifies single-variant unions
   - Falls back to `kind: "unknown"` with `raw: str(annotation)`

3. Handle `tuple` as special case of list (or `unknown` for now — not requested).

4. Update the `/signature` endpoint to call `parse_type_annotation` and populate `type_info` and `optional` on each `FactoryParameter`.

5. Keep populating `type` (raw string) and `enum_options` for backward compatibility.

### Step 2: Frontend — TypeInfo type and FactoryParameter update

**File:** `netrun-ui/src/lib/api.ts`

1. Add TypeScript `TypeInfo` type:

```typescript
export type TypeInfo =
    | { kind: 'str' }
    | { kind: 'int' }
    | { kind: 'float' }
    | { kind: 'bool' }
    | { kind: 'enum'; options: string[] }
    | { kind: 'list'; item: TypeInfo }
    | { kind: 'dict'; value: TypeInfo }
    | { kind: 'union'; variants: TypeInfo[] }
    | { kind: 'unknown'; raw: string | null };
```

2. Update `FactoryParameter`:

```typescript
export interface FactoryParameter {
    name: string;
    type: string | null;
    type_info: TypeInfo | null;
    optional: boolean;
    default: unknown;
    has_default: boolean;
    enum_options?: string[] | null;
}
```

### Step 3: Frontend — New editor components

**New file:** `netrun-ui/src/lib/components/FactoryArgEditor.svelte`

A recursive Svelte component that renders the correct editor based on `TypeInfo.kind`:

- **`str`** → `<input type="text">`
- **`int`** → `<input type="number" step="1">`
- **`float`** → `<input type="number" step="any">`
- **`bool`** → `<input type="checkbox">`
- **`enum`** → `<select>` with options
- **`list`** → List editor (see below)
- **`dict`** → Dict editor (see below)
- **`union`** → Union switcher (see below)
- **`unknown`** → Error message: "Unsupported type: {raw}"

**Props:**
```typescript
type_info: TypeInfo
value: unknown
optional: boolean        // shows "None" option if true
onchange: (value: unknown) => void
```

This component calls itself recursively for nested types (e.g. list items, dict values, union variants).

### Step 4: List editor

Renders inside `FactoryArgEditor` when `kind === "list"`.

**UI:**
```
[item editor for type T] [x]
[item editor for type T] [x]
[+ Add]
```

**Behavior:**
- Value is a JSON array: `["a", "b", "c"]`
- Each item is rendered with the editor for `item` TypeInfo
- "x" button removes item
- "+ Add" button appends a default value (empty string for str, 0 for int, false for bool, etc.)
- Changes emit the full array via `onchange`

**Default values by kind:**
- `str` → `""`
- `int` → `0`
- `float` → `0.0`
- `bool` → `false`
- `enum` → first option value

### Step 5: Dict editor

Renders inside `FactoryArgEditor` when `kind === "dict"`.

**UI:**
```
[key: text input] : [value editor for type V] [x]
[key: text input] : [value editor for type V] [x]
[+ Add]
```

**Behavior:**
- Value is a JSON object: `{"a": 1, "b": 3}`
- Keys are always strings (text input)
- Values rendered with editor for `value` TypeInfo
- "+ Add" adds empty key with default value
- Duplicate key detection (warn or prevent)
- Changes emit the full object via `onchange`

### Step 6: Union switcher

Renders inside `FactoryArgEditor` when `kind === "union"`.

**UI:**
```
[dropdown: variant label ▼]
[editor for selected variant]
```

**Behavior:**
- Dropdown shows human-readable labels for each variant (e.g. "list[str]", "dict[str, int]", "int", "bool")
- Selecting a variant switches the editor
- When switching variants, the value resets to the default for the new type
- The selected variant is inferred from the current value's type, or stored alongside

**Variant label generation:** A `typeInfoLabel(info: TypeInfo): string` function:
- `str` → `"str"`, `int` → `"int"`, etc.
- `list` → `"list[" + typeInfoLabel(item) + "]"`
- `dict` → `"dict[str, " + typeInfoLabel(value) + "]"`
- `enum` → `"enum"`
- `union` → variants joined with `" | "`

**Variant detection from value:** Infer which variant matches the current value:
- `typeof value === 'string'` → `str` or `enum`
- `typeof value === 'number' && Number.isInteger(value)` → `int` (prefer) or `float`
- `typeof value === 'number'` → `float`
- `typeof value === 'boolean'` → `bool`
- `Array.isArray(value)` → `list`
- `typeof value === 'object' && !Array.isArray(value)` → `dict`
- No match → first variant as default

### Step 7: Update Sidebar.svelte

Replace the current type-detection functions and inline rendering with a single `FactoryArgEditor` component call:

```svelte
{#each factoryParams as param}
    <div class="factory-arg">
        <div class="arg-header">
            <span class="arg-key">{param.name}</span>
            <span class="arg-type">({param.type ?? 'untyped'})</span>
            {#if !param.has_default}
                <span class="arg-required">*</span>
            {/if}
        </div>
        {#if param.type_info}
            <FactoryArgEditor
                type_info={param.type_info}
                value={$selectedNode.data.factoryArgs?.[param.name] ?? param.default}
                optional={param.optional}
                onchange={(v) => updateFactoryArg(param.name, v)}
                onblur={() => { pushHistory(); refreshFactoryPreview(); }}
            />
        {:else}
            <!-- No type info (should not happen with new backend) -->
            <input type="text" ... />
        {/if}
    </div>
{/each}
```

### Step 8: Update convertFactoryArgValue / updateFactoryArg

The `FactoryArgEditor` component emits already-typed values (arrays, objects, numbers, booleans, strings), so `convertFactoryArgValue` becomes much simpler — just pass through the value. The conversion responsibility moves into each editor widget.

`updateFactoryArg` changes signature from `(key: string, value: string | boolean)` to `(key: string, value: unknown)` since complex editors emit structured values directly.

### Step 9: Remove old type detection functions

Delete `isImportPathParam`, `isBoolParam`, `isIntParam`, `isFloatParam`, `isEnumParam`, `getParamPlaceholder`, `getParamTypeHint`, and `convertFactoryArgValue` from Sidebar.svelte. Their logic is now inside `FactoryArgEditor`.

---

## Files to modify

| File | Changes |
|------|---------|
| `netrun_ui_backend/routes/factories.py` | Add TypeInfo model, parse_type_annotation(), update FactoryParameter, update /signature endpoint |
| `src/lib/api.ts` | Add TypeInfo type, update FactoryParameter interface |
| `src/lib/components/FactoryArgEditor.svelte` | **New file** — recursive editor component |
| `src/lib/components/Sidebar.svelte` | Replace inline arg rendering with FactoryArgEditor, remove old type functions |

## Edge cases

- **Deeply nested types** (`list[list[str]]`, `dict[str, list[int]]`): Handled by recursion. UI may get deep but functional.
- **Large lists/dicts**: No virtualization needed for now; factory args are typically small.
- **Default values for complex types**: Backend serializes defaults as JSON. Frontend uses them as initial values.
- **`Callable | str`** (from_function's `func`): Callable is unknown, filtered from union, leaves `str`. Renders as text input. Correct behavior.
- **Tuple types**: Treated as `unknown` for now (not requested).
- **`typing.Annotated`**: Ignored for now (strip wrapper, use inner type).

## Testing

- Backend: Unit tests for `parse_type_annotation` covering all type combinations
- Frontend: Manual testing with the join factory (`list[str] | dict[str, int]`), broadcast factory (enum + int), and from_function factory (`Callable | str`, bool)
