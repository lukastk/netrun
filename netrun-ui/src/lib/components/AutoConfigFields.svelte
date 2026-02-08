<script lang="ts">
	import type { FieldSchema, ModelSchema } from '$lib/api';
	import { getAutoFields } from '$lib/stores/schemaStore';
	import { pushHistory } from '$lib/stores/flowStore';
	import { tooltip } from '$lib/utils/tooltip';

	interface Props {
		modelName: string;
		schema: ModelSchema;
		values: Record<string, unknown>;
		onUpdate: (updates: Record<string, unknown>) => void;
		onUpdateLive?: (updates: Record<string, unknown>) => void;
	}

	let { modelName, schema, values, onUpdate, onUpdateLive }: Props = $props();

	let autoFields = $derived(getAutoFields(modelName, schema));

	function formatLabel(name: string): string {
		return name
			.split('_')
			.map(w => w.charAt(0).toUpperCase() + w.slice(1))
			.join(' ');
	}

	function getValue(field: FieldSchema): unknown {
		const v = values[field.name];
		return v !== undefined ? v : field.default;
	}

	function setValue(name: string, value: unknown) {
		if (value === '' || value === null || value === undefined) {
			const { [name]: _removed, ...rest } = values;
			onUpdate(rest);
		} else {
			onUpdate({ ...values, [name]: value });
		}
	}

	function setValueLive(name: string, value: unknown) {
		const fn = onUpdateLive ?? onUpdate;
		if (value === '' || value === null || value === undefined) {
			const { [name]: _removed, ...rest } = values;
			fn(rest);
		} else {
			fn({ ...values, [name]: value });
		}
	}

	// Tri-state cycling for bool_or_null
	function cycleTriState(val: unknown): boolean | null {
		if (val === null || val === undefined) return true;
		if (val === true) return false;
		return null;
	}

	function triStateLabel(val: unknown): string {
		if (val === null || val === undefined) return 'Inherit';
		return val ? 'Yes' : 'No';
	}

	function triStateClass(val: unknown): string {
		if (val === null || val === undefined) return 'inherit';
		return val ? 'yes' : 'no';
	}
</script>

{#each autoFields as field (field.name)}
	{@const val = getValue(field)}

	{#if field.category === 'bool'}
		<label class="checkbox-field">
			<input
				type="checkbox"
				checked={val === true}
				onchange={(e) => {
					setValue(field.name, (e.target as HTMLInputElement).checked);
					pushHistory();
				}}
			/>
			<span>{formatLabel(field.name)}</span>
			{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
		</label>

	{:else if field.category === 'bool_or_null'}
		<div class="tri-state-row">
			<button
				class="tri-state-btn"
				onclick={() => {
					setValue(field.name, cycleTriState(val));
					pushHistory();
				}}
			>
				<span class="tri-state-label">
					{formatLabel(field.name)}
					{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
				</span>
				<span class="tri-state-value tri-state-{triStateClass(val)}">{triStateLabel(val)}</span>
			</button>
		</div>

	{:else if field.category === 'str' || field.category === 'str_or_null'}
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
			</label>
			<input
				type="text"
				value={val ?? ''}
				placeholder={field.category === 'str_or_null' ? '(none)' : ''}
				oninput={(e) => {
					const v = (e.target as HTMLInputElement).value;
					setValueLive(field.name, v || (field.category === 'str_or_null' ? null : v));
				}}
				onblur={() => pushHistory()}
			/>
		</div>

	{:else if field.category === 'int' || field.category === 'int_or_null'}
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
			</label>
			<input
				type="number"
				step="1"
				value={val ?? ''}
				placeholder={field.category === 'int_or_null' ? 'none' : ''}
				oninput={(e) => {
					const v = (e.target as HTMLInputElement).value;
					setValueLive(field.name, v ? parseInt(v) : (field.category === 'int_or_null' ? null : 0));
				}}
				onblur={() => pushHistory()}
			/>
		</div>

	{:else if field.category === 'float' || field.category === 'float_or_null'}
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
			</label>
			<input
				type="number"
				step="any"
				value={val ?? ''}
				placeholder={field.category === 'float_or_null' ? 'none' : ''}
				oninput={(e) => {
					const v = (e.target as HTMLInputElement).value;
					setValueLive(field.name, v ? parseFloat(v) : (field.category === 'float_or_null' ? null : 0));
				}}
				onblur={() => pushHistory()}
			/>
		</div>

	{:else if field.category === 'enum'}
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
			</label>
			<select
				value={val ?? ''}
				onchange={(e) => {
					setValue(field.name, (e.target as HTMLSelectElement).value);
					pushHistory();
				}}
			>
				{#each field.enum_values ?? [] as opt}
					<option value={opt}>{opt}</option>
				{/each}
			</select>
		</div>

	{:else if field.category === 'enum_or_null'}
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
			</label>
			<select
				value={val ?? '__default__'}
				onchange={(e) => {
					const v = (e.target as HTMLSelectElement).value;
					setValue(field.name, v === '__default__' ? null : v);
					pushHistory();
				}}
			>
				<option value="__default__">Default</option>
				{#each field.enum_values ?? [] as opt}
					<option value={opt}>{opt}</option>
				{/each}
			</select>
		</div>
	{/if}
{/each}

<style>
	.field {
		margin-bottom: 10px;
	}

	.field:last-child {
		margin-bottom: 0;
	}

	.field label {
		display: block;
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
		margin-bottom: 4px;
	}

	.field input,
	.field select {
		width: 100%;
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.field input:focus,
	.field select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field select {
		cursor: pointer;
	}

	.has-tooltip-icon {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 13px;
		height: 13px;
		margin-left: 4px;
		border-radius: 50%;
		background: var(--border-color, #404040);
		color: var(--text-secondary, #a0a0a0);
		font-size: 9px;
		font-weight: 600;
		vertical-align: middle;
		cursor: help;
	}

	.checkbox-field {
		display: flex;
		align-items: center;
		gap: 8px;
		cursor: pointer;
		font-size: 12px;
		color: var(--text-primary, #fff);
		margin-bottom: 6px;
	}

	.checkbox-field:last-child {
		margin-bottom: 0;
	}

	.checkbox-field input[type='checkbox'] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}

	/* Tri-state buttons */
	.tri-state-row {
		margin-bottom: 4px;
	}

	.tri-state-row:last-child {
		margin-bottom: 0;
	}

	.tri-state-btn {
		display: flex;
		align-items: center;
		justify-content: space-between;
		width: 100%;
		padding: 4px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
	}

	.tri-state-btn:hover {
		border-color: var(--accent-color, #3b82f6);
	}

	.tri-state-label {
		font-size: 11px;
	}

	.tri-state-value {
		font-size: 10px;
		font-weight: 500;
		padding: 1px 6px;
		border-radius: 2px;
	}

	.tri-state-inherit {
		color: var(--text-secondary, #a0a0a0);
		background: transparent;
	}

	.tri-state-yes {
		color: #4ade80;
		background: rgba(74, 222, 128, 0.1);
	}

	.tri-state-no {
		color: #f87171;
		background: rgba(248, 113, 113, 0.1);
	}
</style>
