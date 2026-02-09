<script lang="ts">
	import type { FieldSchema, ModelSchema } from '$lib/api';
	import { getAutoFields } from '$lib/stores/schemaStore';
	import { pushHistory } from '$lib/stores/flowStore';
	import { tooltip } from '$lib/utils/tooltip';
	import { isEnvVar, makeEnvVar, getEnvVarName, getEnvVarDefault } from '$lib/utils/envvar';

	interface Props {
		modelName: string;
		schema: ModelSchema;
		values: Record<string, unknown>;
		onUpdate: (updates: Record<string, unknown>) => void;
		onUpdateLive?: (updates: Record<string, unknown>) => void;
	}

	let { modelName, schema, values, onUpdate, onUpdateLive }: Props = $props();

	let autoFields = $derived(getAutoFields(modelName, schema));

	// Track which fields are in env var mode
	let envVarMode: Record<string, boolean> = $state({});

	// Initialize env var mode from current values
	$effect(() => {
		const newMode: Record<string, boolean> = {};
		for (const field of autoFields) {
			if (field.env_var_supported && isEnvVar(values[field.name])) {
				newMode[field.name] = true;
			}
		}
		envVarMode = newMode;
	});

	function isFieldEnvVar(field: FieldSchema): boolean {
		return field.env_var_supported && (envVarMode[field.name] || false);
	}

	function toggleEnvVarMode(field: FieldSchema) {
		const wasEnvVar = envVarMode[field.name] || false;
		envVarMode[field.name] = !wasEnvVar;

		if (wasEnvVar) {
			// Switching from env var → value: use the env var's default if available
			const currentVal = values[field.name];
			const defaultFromEnv = isEnvVar(currentVal) ? getEnvVarDefault(currentVal) : null;
			const newVal = defaultFromEnv ?? field.default;
			setValue(field.name, newVal);
		} else {
			// Switching from value → env var: preserve current value as default
			const currentVal = values[field.name];
			const literalVal = currentVal !== undefined ? currentVal : field.default;
			onUpdate({ ...values, [field.name]: makeEnvVar('', literalVal) });
		}
		pushHistory();
	}

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

	// Env var input handlers
	function updateEnvVarName(field: FieldSchema, envName: string) {
		const currentVal = values[field.name];
		const currentDefault = isEnvVar(currentVal) ? getEnvVarDefault(currentVal) : null;
		const fn = onUpdateLive ?? onUpdate;
		fn({ ...values, [field.name]: makeEnvVar(envName, currentDefault) });
	}

	function updateEnvVarDefault(field: FieldSchema, defaultVal: unknown) {
		const currentVal = values[field.name];
		const currentName = isEnvVar(currentVal) ? getEnvVarName(currentVal) : '';
		const fn = onUpdateLive ?? onUpdate;
		fn({ ...values, [field.name]: makeEnvVar(currentName, defaultVal) });
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
	{@const inEnvMode = isFieldEnvVar(field)}

	{#if inEnvMode}
		<!-- Env var mode: show env var name + optional default -->
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
				<button
					class="envvar-toggle active"
					title="Switch to literal value"
					onclick={() => toggleEnvVarMode(field)}
				>$</button>
			</label>
			<div class="envvar-input-group">
				<div class="envvar-name-row">
					<span class="envvar-prefix">$</span>
					<input
						type="text"
						class="envvar-name-input"
						value={getEnvVarName(val)}
						placeholder="ENV_VAR_NAME"
						oninput={(e) => updateEnvVarName(field, (e.target as HTMLInputElement).value)}
						onblur={() => pushHistory()}
					/>
				</div>
				<div class="envvar-default-row">
					<span class="envvar-default-label">default:</span>
					{#if field.category === 'bool' || field.category === 'bool_or_null'}
						<select
							class="envvar-default-input"
							value={String(getEnvVarDefault(val) ?? '')}
							onchange={(e) => {
								const v = (e.target as HTMLSelectElement).value;
								const parsed = v === 'true' ? true : v === 'false' ? false : null;
								updateEnvVarDefault(field, parsed);
								pushHistory();
							}}
						>
							{#if field.category === 'bool_or_null'}<option value="">inherit</option>{/if}
							<option value="true">true</option>
							<option value="false">false</option>
						</select>
					{:else if field.category === 'int' || field.category === 'int_or_null'}
						<input
							type="number"
							step="1"
							class="envvar-default-input"
							value={getEnvVarDefault(val) ?? ''}
							placeholder={field.category === 'int_or_null' ? 'none' : '0'}
							oninput={(e) => {
								const v = (e.target as HTMLInputElement).value;
								updateEnvVarDefault(field, v ? parseInt(v) : null);
							}}
							onblur={() => pushHistory()}
						/>
					{:else if field.category === 'float' || field.category === 'float_or_null'}
						<input
							type="number"
							step="any"
							class="envvar-default-input"
							value={getEnvVarDefault(val) ?? ''}
							placeholder={field.category === 'float_or_null' ? 'none' : '0'}
							oninput={(e) => {
								const v = (e.target as HTMLInputElement).value;
								updateEnvVarDefault(field, v ? parseFloat(v) : null);
							}}
							onblur={() => pushHistory()}
						/>
					{:else if field.category === 'enum' || field.category === 'enum_or_null'}
						<select
							class="envvar-default-input"
							value={String(getEnvVarDefault(val) ?? '__default__')}
							onchange={(e) => {
								const v = (e.target as HTMLSelectElement).value;
								updateEnvVarDefault(field, v === '__default__' ? null : v);
								pushHistory();
							}}
						>
							{#if field.category === 'enum_or_null'}<option value="__default__">None</option>{/if}
							{#each field.enum_values ?? [] as opt}
								<option value={opt}>{opt}</option>
							{/each}
						</select>
					{:else}
						<input
							type="text"
							class="envvar-default-input"
							value={getEnvVarDefault(val) ?? ''}
							placeholder="(none)"
							oninput={(e) => {
								const v = (e.target as HTMLInputElement).value;
								updateEnvVarDefault(field, v || null);
							}}
							onblur={() => pushHistory()}
						/>
					{/if}
				</div>
			</div>
		</div>

	{:else if field.category === 'bool'}
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
			{#if field.env_var_supported}
				<button
					class="envvar-toggle"
					title="Switch to environment variable"
					onclick={(e) => { e.stopPropagation(); toggleEnvVarMode(field); }}
				>$</button>
			{/if}
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
					{#if field.env_var_supported}
						<button
							class="envvar-toggle"
							title="Switch to environment variable"
							onclick={(e) => { e.stopPropagation(); toggleEnvVarMode(field); }}
						>$</button>
					{/if}
				</span>
				<span class="tri-state-value tri-state-{triStateClass(val)}">{triStateLabel(val)}</span>
			</button>
		</div>

	{:else if field.category === 'str' || field.category === 'str_or_null'}
		<div class="field">
			<label>
				{formatLabel(field.name)}
				{#if field.description}<span class="has-tooltip-icon" use:tooltip={field.description}>?</span>{/if}
				{#if field.env_var_supported}
					<button
						class="envvar-toggle"
						title="Switch to environment variable"
						onclick={() => toggleEnvVarMode(field)}
					>$</button>
				{/if}
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
				{#if field.env_var_supported}
					<button
						class="envvar-toggle"
						title="Switch to environment variable"
						onclick={() => toggleEnvVarMode(field)}
					>$</button>
				{/if}
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
				{#if field.env_var_supported}
					<button
						class="envvar-toggle"
						title="Switch to environment variable"
						onclick={() => toggleEnvVarMode(field)}
					>$</button>
				{/if}
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
				{#if field.env_var_supported}
					<button
						class="envvar-toggle"
						title="Switch to environment variable"
						onclick={() => toggleEnvVarMode(field)}
					>$</button>
				{/if}
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
				{#if field.env_var_supported}
					<button
						class="envvar-toggle"
						title="Switch to environment variable"
						onclick={() => toggleEnvVarMode(field)}
					>$</button>
				{/if}
			</label>
			<select
				value={val ?? '__default__'}
				onchange={(e) => {
					const v = (e.target as HTMLSelectElement).value;
					setValue(field.name, v === '__default__' ? null : v);
					pushHistory();
				}}
			>
				<option value="__default__">None</option>
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
		display: flex;
		align-items: center;
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
		display: flex;
		align-items: center;
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

	/* Env var toggle button */
	.envvar-toggle {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 16px;
		height: 16px;
		margin-left: auto;
		padding: 0;
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		background: transparent;
		color: var(--text-secondary, #666);
		font-size: 10px;
		font-weight: 700;
		cursor: pointer;
		line-height: 1;
		flex-shrink: 0;
	}

	.envvar-toggle:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.envvar-toggle.active {
		background: var(--accent-color, #3b82f6);
		border-color: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.envvar-toggle.active:hover {
		background: var(--accent-hover, #2563eb);
		border-color: var(--accent-hover, #2563eb);
	}

	/* Env var input group */
	.envvar-input-group {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.envvar-name-row {
		display: flex;
		align-items: center;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid rgba(59, 130, 246, 0.4);
		border-radius: 3px;
		overflow: hidden;
	}

	.envvar-prefix {
		padding: 6px 4px 6px 8px;
		color: var(--accent-color, #3b82f6);
		font-size: 12px;
		font-weight: 700;
		user-select: none;
	}

	.envvar-name-input {
		flex: 1;
		padding: 6px 8px 6px 0;
		background: transparent;
		border: none;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.envvar-name-input:focus {
		outline: none;
	}

	.envvar-default-row {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.envvar-default-label {
		font-size: 10px;
		color: var(--text-secondary, #666);
		white-space: nowrap;
		flex-shrink: 0;
	}

	.envvar-default-input {
		flex: 1;
		padding: 3px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
		min-width: 0;
	}

	.envvar-default-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
		color: var(--text-primary, #fff);
	}

	select.envvar-default-input {
		cursor: pointer;
	}
</style>
