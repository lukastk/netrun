<script lang="ts">
	import type { TypeInfo } from '$lib/api';
	import FactoryArgEditor from './FactoryArgEditor.svelte';

	interface Props {
		type_info: TypeInfo;
		value: unknown;
		optional?: boolean;
		onchange: (value: unknown) => void;
		onblur?: () => void;
		depth?: number;
	}

	let { type_info, value, optional = false, onchange, onblur, depth = 0 }: Props = $props();

	const MAX_DEPTH = 5;

	// --- Helpers ---

	function typeInfoLabel(info: TypeInfo): string {
		switch (info.kind) {
			case 'str': return 'str';
			case 'int': return 'int';
			case 'float': return 'float';
			case 'bool': return 'bool';
			case 'enum': return 'enum';
			case 'list': return `list[${typeInfoLabel(info.item)}]`;
			case 'dict': return `dict[str, ${typeInfoLabel(info.value)}]`;
			case 'union': return info.variants.map(typeInfoLabel).join(' | ');
			case 'unknown': return info.raw ?? 'unknown';
		}
	}

	function defaultValueForType(info: TypeInfo): unknown {
		switch (info.kind) {
			case 'str': return '';
			case 'int': return 0;
			case 'float': return 0.0;
			case 'bool': return false;
			case 'enum': return info.options[0] ?? '';
			case 'list': return [];
			case 'dict': return {};
			case 'union': return defaultValueForType(info.variants[0]);
			case 'unknown': return '';
		}
	}

	function detectVariantIndex(val: unknown, variants: TypeInfo[]): number {
		if (val === undefined || val === null) return 0;
		for (let i = 0; i < variants.length; i++) {
			const v = variants[i];
			if (v.kind === 'bool' && typeof val === 'boolean') return i;
			if (v.kind === 'int' && typeof val === 'number' && Number.isInteger(val)) return i;
			if (v.kind === 'float' && typeof val === 'number') return i;
			if (v.kind === 'str' && typeof val === 'string') return i;
			if (v.kind === 'enum' && typeof val === 'string') return i;
			if (v.kind === 'list' && Array.isArray(val)) return i;
			if (v.kind === 'dict' && typeof val === 'object' && !Array.isArray(val)) return i;
		}
		return 0;
	}

	// --- List helpers ---

	function getListItems(): unknown[] {
		return Array.isArray(value) ? value : [];
	}

	function updateListItem(index: number, newVal: unknown) {
		const items = [...getListItems()];
		items[index] = newVal;
		onchange(items);
	}

	function removeListItem(index: number) {
		const items = [...getListItems()];
		items.splice(index, 1);
		onchange(items);
	}

	function addListItem() {
		if (type_info.kind !== 'list') return;
		const items = [...getListItems()];
		items.push(defaultValueForType(type_info.item));
		onchange(items);
	}

	// --- Dict helpers ---

	function getDictEntries(): [string, unknown][] {
		if (value && typeof value === 'object' && !Array.isArray(value)) {
			return Object.entries(value as Record<string, unknown>);
		}
		return [];
	}

	function updateDictKey(oldKey: string, newKey: string) {
		const entries = getDictEntries();
		const obj: Record<string, unknown> = {};
		for (const [k, v] of entries) {
			obj[k === oldKey ? newKey : k] = v;
		}
		onchange(obj);
	}

	function updateDictValue(key: string, newVal: unknown) {
		const obj = { ...(value as Record<string, unknown> || {}) };
		obj[key] = newVal;
		onchange(obj);
	}

	function removeDictEntry(key: string) {
		const obj = { ...(value as Record<string, unknown> || {}) };
		delete obj[key];
		onchange(obj);
	}

	function addDictEntry() {
		if (type_info.kind !== 'dict') return;
		const obj = { ...(value as Record<string, unknown> || {}) };
		// Find a unique key
		let key = '';
		let i = 0;
		while (key in obj) {
			key = `key${i++}`;
		}
		obj[key] = defaultValueForType(type_info.value);
		onchange(obj);
	}

	// --- Union helpers ---

	let unionVariantIndex = $derived(
		type_info.kind === 'union' ? detectVariantIndex(value, type_info.variants) : 0
	);

	function switchVariant(newIndex: number) {
		if (type_info.kind !== 'union') return;
		onchange(defaultValueForType(type_info.variants[newIndex]));
	}

	// --- Primitive value handlers ---

	function handleStringInput(e: Event) {
		onchange((e.target as HTMLInputElement).value);
	}

	function handleIntInput(e: Event) {
		const str = (e.target as HTMLInputElement).value;
		if (str === '') { onchange(undefined); return; }
		const parsed = parseInt(str, 10);
		onchange(isNaN(parsed) ? str : parsed);
	}

	function handleFloatInput(e: Event) {
		const str = (e.target as HTMLInputElement).value;
		if (str === '') { onchange(undefined); return; }
		const parsed = parseFloat(str);
		onchange(isNaN(parsed) ? str : parsed);
	}

	function handleBoolChange(e: Event) {
		onchange((e.target as HTMLInputElement).checked);
	}

	function handleEnumChange(e: Event) {
		onchange((e.target as HTMLSelectElement).value);
	}
</script>

{#if optional && value === null}
	<!-- Optional param set to None -->
	<div class="optional-none">
		<span class="none-label">None</span>
		<button class="set-value-btn" onclick={() => onchange(defaultValueForType(type_info))}>Set value</button>
	</div>
{:else}
	{#if optional}
		<button class="clear-btn" onclick={() => { onchange(null); onblur?.(); }}>Clear (None)</button>
	{/if}

	{#if depth >= MAX_DEPTH}
		<span class="type-error">Nesting too deep</span>
	{:else if type_info.kind === 'str'}
		<input
			type="text"
			value={String(value ?? '')}
			oninput={handleStringInput}
			{onblur}
		/>
	{:else if type_info.kind === 'int'}
		<input
			type="number"
			step="1"
			value={value ?? ''}
			oninput={handleIntInput}
			{onblur}
		/>
	{:else if type_info.kind === 'float'}
		<input
			type="number"
			step="any"
			value={value ?? ''}
			oninput={handleFloatInput}
			{onblur}
		/>
	{:else if type_info.kind === 'bool'}
		<label class="checkbox-label">
			<input
				type="checkbox"
				checked={value === true}
				onchange={handleBoolChange}
			/>
			<span class="checkbox-text">{value === true ? 'True' : 'False'}</span>
		</label>
	{:else if type_info.kind === 'enum'}
		<select
			value={String(value ?? '')}
			onchange={handleEnumChange}
		>
			{#if value === undefined || value === null || value === ''}
				<option value="" disabled selected>Select...</option>
			{/if}
			{#each type_info.options as option (option)}
				<option value={option}>{option}</option>
			{/each}
		</select>
	{:else if type_info.kind === 'list'}
		<div class="list-editor">
			{#each getListItems() as item, i (i)}
				<div class="list-item">
					<div class="list-item-editor">
						<FactoryArgEditor
							type_info={type_info.item}
							value={item}
							onchange={(v: unknown) => updateListItem(i, v)}
							{onblur}
							depth={depth + 1}
						/>
					</div>
					<button class="remove-btn" onclick={() => { removeListItem(i); onblur?.(); }} title="Remove">&times;</button>
				</div>
			{/each}
			<button class="add-btn" onclick={() => { addListItem(); onblur?.(); }}>+ Add</button>
		</div>
	{:else if type_info.kind === 'dict'}
		<div class="dict-editor">
			{#each getDictEntries() as [key, val] (key)}
				<div class="dict-entry">
					<input
						type="text"
						class="dict-key"
						value={key}
						onchange={(e) => { updateDictKey(key, (e.target as HTMLInputElement).value); onblur?.(); }}
						placeholder="key"
					/>
					<span class="dict-separator">:</span>
					<div class="dict-value-editor">
						<FactoryArgEditor
							type_info={type_info.value}
							value={val}
							onchange={(v: unknown) => updateDictValue(key, v)}
							{onblur}
							depth={depth + 1}
						/>
					</div>
					<button class="remove-btn" onclick={() => { removeDictEntry(key); onblur?.(); }} title="Remove">&times;</button>
				</div>
			{/each}
			<button class="add-btn" onclick={() => { addDictEntry(); onblur?.(); }}>+ Add</button>
		</div>
	{:else if type_info.kind === 'union'}
		<div class="union-editor">
			<select
				class="variant-select"
				value={String(unionVariantIndex)}
				onchange={(e) => { switchVariant(parseInt((e.target as HTMLSelectElement).value, 10)); onblur?.(); }}
			>
				{#each type_info.variants as variant, i (i)}
					<option value={String(i)}>{typeInfoLabel(variant)}</option>
				{/each}
			</select>
			<div class="variant-editor">
				<FactoryArgEditor
					type_info={type_info.variants[unionVariantIndex]}
					{value}
					{onchange}
					{onblur}
					depth={depth + 1}
				/>
			</div>
		</div>
	{:else if type_info.kind === 'unknown'}
		<span class="type-error">Unsupported type: {type_info.raw ?? 'unknown'}</span>
	{/if}
{/if}

<style>
	input, select {
		width: 100%;
		padding: 6px 8px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	input:focus, select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	select {
		cursor: pointer;
	}

	.checkbox-label {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 6px 0;
		cursor: pointer;
	}

	.checkbox-label input[type="checkbox"] {
		width: 16px;
		height: 16px;
		margin: 0;
		cursor: pointer;
		accent-color: var(--accent-color, #3b82f6);
	}

	.checkbox-text {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
	}

	.type-error {
		font-size: 11px;
		color: var(--error-color, #ef4444);
		padding: 4px 0;
	}

	/* Optional controls */
	.optional-none {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.none-label {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		font-style: italic;
	}

	.set-value-btn, .clear-btn {
		font-size: 10px;
		padding: 2px 6px;
		background: var(--bg-secondary, #2a2a2a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
	}

	.set-value-btn:hover, .clear-btn:hover {
		background: var(--bg-primary, #1a1a1a);
		color: var(--text-primary, #fff);
	}

	.clear-btn {
		align-self: flex-start;
		margin-bottom: 4px;
	}

	/* List editor */
	.list-editor {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.list-item {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.list-item-editor {
		flex: 1;
		min-width: 0;
	}

	/* Dict editor */
	.dict-editor {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.dict-entry {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.dict-key {
		flex: 0 0 80px;
		min-width: 0;
	}

	.dict-separator {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		flex-shrink: 0;
	}

	.dict-value-editor {
		flex: 1;
		min-width: 0;
	}

	/* Union editor */
	.union-editor {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.variant-select {
		font-size: 11px;
		padding: 4px 6px;
	}

	/* Add/Remove buttons */
	.add-btn {
		align-self: flex-start;
		font-size: 11px;
		padding: 2px 8px;
		background: var(--bg-secondary, #2a2a2a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		margin-top: 2px;
	}

	.add-btn:hover {
		background: var(--bg-primary, #1a1a1a);
		color: var(--text-primary, #fff);
	}

	.remove-btn {
		flex-shrink: 0;
		width: 20px;
		height: 20px;
		display: flex;
		align-items: center;
		justify-content: center;
		background: none;
		border: 1px solid transparent;
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		font-size: 14px;
		padding: 0;
		line-height: 1;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
		border-color: var(--error-color, #ef4444);
		background: rgba(239, 68, 68, 0.1);
	}
</style>
