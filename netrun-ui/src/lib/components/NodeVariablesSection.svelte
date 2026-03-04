<script lang="ts">
	import { validateVarValue, type NodeVariable } from '$lib/stores/variablesStore';
	import { isEnvVar, makeEnvVar, getEnvVarName, getEnvVarDefault } from '$lib/utils/envvar';

	const VAR_TYPES = ['str', 'int', 'float', 'bool', 'json'] as const;

	interface Props {
		variables: Record<string, NodeVariable>;
		inheritedVariables?: Record<string, NodeVariable>;
		onUpdate: (vars: Record<string, NodeVariable>) => void;
		level: 'net' | 'node';
	}

	let { variables, inheritedVariables = {}, onUpdate, level }: Props = $props();

	// New variable form state
	let newName = $state('');
	let newValue = $state('');
	let newType = $state('str');
	let newEnvVarMode = $state(false);
	let newEnvVarName = $state('');
	let newEnvVarDefault = $state('');
	let newError = $derived(newEnvVarMode ? null : validateVarValue(newValue, newType));

	// Track which existing variables are in env var mode
	let envVarMode: Record<string, boolean> = $state({});

	// Initialize env var mode from current values
	$effect(() => {
		const newMode: Record<string, boolean> = {};
		for (const [name, variable] of Object.entries(variables)) {
			if (isEnvVar(variable.value)) {
				newMode[name] = true;
			}
		}
		envVarMode = newMode;
	});

	// Compute display list: own vars + inherited (not overridden)
	let displayVars = $derived.by(() => {
		const entries: Array<{
			name: string;
			variable: NodeVariable;
			source: 'own' | 'inherited';
		}> = [];

		// Add own variables first
		for (const [name, variable] of Object.entries(variables)) {
			const isOverride = level === 'node' && name in inheritedVariables;
			entries.push({ name, variable, source: isOverride ? 'own' : 'own' });
		}

		// Add inherited variables that aren't overridden (node level only)
		if (level === 'node') {
			for (const [name, variable] of Object.entries(inheritedVariables)) {
				if (!(name in variables)) {
					entries.push({ name, variable, source: 'inherited' });
				}
			}
		}

		return entries.sort((a, b) => a.name.localeCompare(b.name));
	});

	function toggleEnvVarMode(name: string) {
		const wasEnvVar = envVarMode[name] || false;
		envVarMode[name] = !wasEnvVar;

		const current = { ...variables };
		const existing = current[name];
		if (!existing) return;

		if (wasEnvVar) {
			// Switching from env var → literal: use the env var's default or ''
			const defaultVal = isEnvVar(existing.value) ? getEnvVarDefault(existing.value) : null;
			current[name] = { ...existing, value: defaultVal != null ? String(defaultVal) : '' };
		} else {
			// Switching from literal → env var: preserve current value as default
			const literalVal = typeof existing.value === 'string' ? existing.value : '';
			current[name] = { ...existing, value: makeEnvVar('', literalVal || undefined) };
		}
		onUpdate(current);
	}

	function updateVarValue(name: string, value: string) {
		const current = { ...variables };
		const existing = current[name] || { value: '', type: 'str' };
		current[name] = { ...existing, value };
		onUpdate(current);
	}

	function updateVarEnvName(name: string, envName: string) {
		const current = { ...variables };
		const existing = current[name];
		if (!existing) return;
		const currentDefault = isEnvVar(existing.value) ? getEnvVarDefault(existing.value) : null;
		current[name] = { ...existing, value: makeEnvVar(envName, currentDefault) };
		onUpdate(current);
	}

	function updateVarEnvDefault(name: string, defaultVal: string) {
		const current = { ...variables };
		const existing = current[name];
		if (!existing) return;
		const currentName = isEnvVar(existing.value) ? getEnvVarName(existing.value) : '';
		current[name] = { ...existing, value: makeEnvVar(currentName, defaultVal || undefined) };
		onUpdate(current);
	}

	function updateVarType(name: string, type: string) {
		const current = { ...variables };
		const existing = current[name] || { value: '' };
		current[name] = { ...existing, type };
		onUpdate(current);
	}

	function removeVar(name: string) {
		const { [name]: _, ...rest } = variables;
		delete envVarMode[name];
		onUpdate(rest);
	}

	function addVar() {
		const trimmedName = newName.trim();
		if (!trimmedName) return;
		const current = { ...variables };
		if (newEnvVarMode) {
			current[trimmedName] = {
				value: makeEnvVar(newEnvVarName, newEnvVarDefault || undefined),
				type: newType === 'str' ? undefined : newType,
			};
		} else {
			current[trimmedName] = { value: newValue, type: newType === 'str' ? undefined : newType };
		}
		onUpdate(current);
		newName = '';
		newValue = '';
		newType = 'str';
		newEnvVarMode = false;
		newEnvVarName = '';
		newEnvVarDefault = '';
	}

	function addOption(name: string, optionValue: string) {
		const trimmed = optionValue.trim();
		if (!trimmed) return;
		const current = { ...variables };
		const existing = current[name];
		if (!existing) return;
		const opts = existing.options ? [...existing.options] : [];
		if (!opts.map(String).includes(trimmed)) {
			opts.push(trimmed);
		}
		current[name] = { ...existing, options: opts };
		onUpdate(current);
	}

	function removeOption(name: string, index: number) {
		const current = { ...variables };
		const existing = current[name];
		if (!existing || !existing.options) return;
		const opts = existing.options.filter((_, i) => i !== index);
		current[name] = { ...existing, options: opts.length > 0 ? opts : undefined };
		onUpdate(current);
	}

	function overrideInherited(name: string) {
		const inherited = inheritedVariables[name];
		if (!inherited) return;
		const current = { ...variables };
		// Create an inherit var that references the global; user can set value later
		current[name] = { inherit: true };
		onUpdate(current);
	}

	function formatVarValuePreview(value: string | number | boolean | { $env: string; default?: unknown } | undefined): string {
		if (value === undefined || value === null) return '(unset)';
		if (isEnvVar(value)) {
			return `$${getEnvVarName(value) || '...'}`;
		}
		return typeof value === 'string' ? value : String(value);
	}
</script>

<div class="variables-section">
	{#if displayVars.length === 0}
		<p class="empty-hint">No variables defined</p>
	{:else}
		{#each displayVars as { name, variable, source } (name)}
			{@const inEnvMode = envVarMode[name] || false}
			{@const isInheritVar = source === 'own' && variable.inherit === true}
			{@const globalVar = inheritedVariables[name]}
			{@const effectiveType = isInheritVar && globalVar ? (globalVar.type || 'str') : (variable.type || 'str')}
			{@const effectiveOptions = isInheritVar && globalVar ? globalVar.options : variable.options}
			<div class="var-row" class:inherited={source === 'inherited'}>
				<div class="var-header">
					<span class="var-name">{name}</span>
					<span class="var-type-badge">{effectiveType}</span>
					{#if source === 'inherited'}
						<span class="var-badge default">default</span>
					{:else if isInheritVar}
						<span class="var-badge inherit">inherit</span>
					{:else if level === 'node' && name in inheritedVariables}
						<span class="var-badge override">override</span>
					{/if}
					{#if source !== 'inherited' && !isInheritVar}
						<button
							class="envvar-toggle"
							class:active={inEnvMode}
							title={inEnvMode ? 'Switch to literal value' : 'Switch to environment variable'}
							onclick={() => toggleEnvVarMode(name)}
						>$</button>
					{/if}
				</div>
				{#if source === 'inherited'}
					<div class="var-inherited-row">
						<span class="var-value-preview">{formatVarValuePreview(variable.value)}</span>
						<button
							class="override-btn"
							onclick={() => overrideInherited(name)}
							title="Override this variable at node level"
						>
							Override
						</button>
					</div>
				{:else if isInheritVar}
					{@const globalValue = globalVar ? formatVarValuePreview(globalVar.value) : '(unset)'}
					{@const error = validateVarValue(variable.value, effectiveType, effectiveOptions)}
					<div class="var-edit-row">
						{#if effectiveOptions?.length}
							<select
								class="options-select"
								value={variable.value != null ? (typeof variable.value === 'string' ? variable.value : String(variable.value)) : ''}
								onchange={(e) => {
									const current = { ...variables };
									current[name] = { ...current[name], value: (e.target as HTMLSelectElement).value };
									onUpdate(current);
								}}
							>
								<option value="">(use global: {globalValue})</option>
								{#each effectiveOptions as opt}
									<option value={String(opt)}>{opt}</option>
								{/each}
							</select>
						{:else}
							<input
								type="text"
								value={variable.value != null && typeof variable.value === 'string' ? variable.value : ''}
								placeholder="(uses global: {globalValue})"
								class:invalid={error !== null}
								title={error || ''}
								oninput={(e) => {
									const current = { ...variables };
									const val = (e.target as HTMLInputElement).value;
									current[name] = { ...current[name], value: val || undefined };
									onUpdate(current);
								}}
							/>
						{/if}
						<button
							class="remove-btn"
							onclick={() => removeVar(name)}
							title="Remove variable"
						>
							&times;
						</button>
					</div>
					{#if error}
						<div class="var-error">{error}</div>
					{/if}
				{:else if inEnvMode}
					<div class="envvar-input-group">
						<div class="envvar-name-row">
							<span class="envvar-prefix">$</span>
							<input
								type="text"
								class="envvar-name-input"
								value={getEnvVarName(variable.value)}
								placeholder="ENV_VAR_NAME"
								oninput={(e) => updateVarEnvName(name, (e.target as HTMLInputElement).value)}
							/>
						</div>
						<div class="envvar-default-row">
							<span class="envvar-default-label">default:</span>
							<input
								type="text"
								class="envvar-default-input"
								value={getEnvVarDefault(variable.value) ?? ''}
								placeholder="(none)"
								oninput={(e) => updateVarEnvDefault(name, (e.target as HTMLInputElement).value)}
							/>
						</div>
					</div>
					<div class="var-edit-row envvar-controls">
						<select
							value={variable.type || 'str'}
							onchange={(e) => updateVarType(name, (e.target as HTMLSelectElement).value)}
						>
							{#each VAR_TYPES as t}
								<option value={t}>{t}</option>
							{/each}
						</select>
						<button
							class="remove-btn"
							onclick={() => removeVar(name)}
							title="Remove variable"
						>
							&times;
						</button>
					</div>
				{:else}
					{@const error = validateVarValue(variable.value, variable.type, variable.options)}
					<div class="var-edit-row">
						{#if variable.options?.length}
							<select
								class="options-select"
								value={variable.value != null ? (typeof variable.value === 'string' ? variable.value : String(variable.value)) : ''}
								onchange={(e) => updateVarValue(name, (e.target as HTMLSelectElement).value)}
							>
								{#if variable.value == null || variable.value === '' || (variable.options && !variable.options.map(String).includes(String(variable.value ?? '')))}
									<option value={variable.value != null ? (typeof variable.value === 'string' ? variable.value : String(variable.value)) : ''}>{variable.value == null || variable.value === '' ? '(select)' : variable.value}</option>
								{/if}
								{#each variable.options as opt}
									<option value={String(opt)}>{opt}</option>
								{/each}
							</select>
						{:else}
							<input
								type="text"
								value={variable.value != null && typeof variable.value === 'string' ? variable.value : ''}
								placeholder={variable.value == null ? '(unset)' : 'value'}
								class:invalid={error !== null}
								title={error || ''}
								oninput={(e) => updateVarValue(name, (e.target as HTMLInputElement).value)}
							/>
						{/if}
						<select
							value={variable.type || 'str'}
							onchange={(e) => updateVarType(name, (e.target as HTMLSelectElement).value)}
						>
							{#each VAR_TYPES as t}
								<option value={t}>{t}</option>
							{/each}
						</select>
						<button
							class="remove-btn"
							onclick={() => removeVar(name)}
							title="Remove variable"
						>
							&times;
						</button>
					</div>
					{#if error}
						<div class="var-error">{error}</div>
					{/if}
					{#if !inEnvMode}
						<div class="options-chips">
							{#if variable.options?.length}
								{#each variable.options as opt, i}
									<span class="option-chip">
										{opt}
										<button class="chip-remove" onclick={() => removeOption(name, i)}>&times;</button>
									</span>
								{/each}
							{/if}
							<input
								type="text"
								class="option-add-input"
								placeholder="+ option"
								onkeydown={(e) => {
									if (e.key === 'Enter') {
										addOption(name, (e.target as HTMLInputElement).value);
										(e.target as HTMLInputElement).value = '';
									}
								}}
							/>
						</div>
					{/if}
				{/if}
			</div>
		{/each}
	{/if}

	<!-- Add new variable form -->
	<div class="add-var-form">
		<div class="add-var-row">
			<input
				type="text"
				bind:value={newName}
				placeholder="name"
				class="add-name"
				onkeydown={(e) => { if (e.key === 'Enter' && !newError) addVar(); }}
			/>
			{#if newEnvVarMode}
				<div class="add-envvar-inputs">
					<div class="envvar-name-row">
						<span class="envvar-prefix">$</span>
						<input
							type="text"
							class="envvar-name-input"
							bind:value={newEnvVarName}
							placeholder="ENV_VAR_NAME"
							onkeydown={(e) => { if (e.key === 'Enter') addVar(); }}
						/>
					</div>
					<div class="envvar-default-row">
						<span class="envvar-default-label">default:</span>
						<input
							type="text"
							class="envvar-default-input"
							bind:value={newEnvVarDefault}
							placeholder="(none)"
							onkeydown={(e) => { if (e.key === 'Enter') addVar(); }}
						/>
					</div>
				</div>
			{:else}
				<input
					type="text"
					bind:value={newValue}
					placeholder="value"
					class="add-value"
					class:invalid={newValue !== '' && newError !== null}
					title={newError || ''}
					onkeydown={(e) => { if (e.key === 'Enter' && !newError) addVar(); }}
				/>
			{/if}
			<select bind:value={newType} class="add-type">
				{#each VAR_TYPES as t}
					<option value={t}>{t}</option>
				{/each}
			</select>
			<button
				class="envvar-toggle"
				class:active={newEnvVarMode}
				title={newEnvVarMode ? 'Switch to literal value' : 'Switch to environment variable'}
				onclick={() => { newEnvVarMode = !newEnvVarMode; }}
			>$</button>
		</div>
		{#if !newEnvVarMode && newValue !== '' && newError}
			<div class="var-error">{newError}</div>
		{/if}
		<button
			class="add-btn"
			onclick={addVar}
			disabled={!newName.trim() || (!newEnvVarMode && newValue !== '' && newError !== null)}
		>
			+ Add Variable
		</button>
	</div>
</div>

<style>
	.variables-section {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.empty-hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		text-align: center;
		padding: 8px;
		margin: 0;
	}

	.var-row {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 8px;
	}

	.var-row.inherited {
		opacity: 0.7;
		border-style: dashed;
	}

	.var-header {
		display: flex;
		align-items: center;
		gap: 6px;
		margin-bottom: 6px;
	}

	.var-name {
		font-size: 12px;
		font-weight: 500;
		color: var(--text-primary, #fff);
	}

	.var-type-badge {
		font-size: 9px;
		padding: 1px 4px;
		background: var(--bg-tertiary, #2d2d2d);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
	}

	.var-badge {
		font-size: 9px;
		padding: 1px 4px;
		border-radius: 3px;
		margin-left: auto;
	}

	.var-badge.default {
		background: rgba(59, 130, 246, 0.15);
		color: #60a5fa;
	}

	.var-badge.override {
		background: rgba(234, 179, 8, 0.15);
		color: #eab308;
	}

	.var-badge.inherit {
		background: rgba(34, 197, 94, 0.15);
		color: #22c55e;
	}

	.var-edit-row {
		display: flex;
		gap: 4px;
		align-items: center;
	}

	.var-edit-row input {
		flex: 1;
		min-width: 0;
		padding: 4px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.var-edit-row input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.var-edit-row input.invalid {
		border-color: var(--error-color, #ef4444);
		background: rgba(239, 68, 68, 0.05);
	}

	.var-edit-row input.invalid:focus {
		border-color: var(--error-color, #ef4444);
		box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.15);
	}

	.var-error {
		font-size: 10px;
		color: var(--error-color, #ef4444);
		margin-top: 3px;
		padding-left: 2px;
	}

	.options-select {
		flex: 1;
		min-width: 0;
		padding: 4px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
	}

	.options-select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.var-edit-row select {
		padding: 4px 4px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 11px;
		cursor: pointer;
		width: 52px;
	}

	.var-edit-row select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.var-inherited-row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.var-value-preview {
		flex: 1;
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.override-btn {
		padding: 2px 8px;
		font-size: 10px;
		background: transparent;
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		white-space: nowrap;
	}

	.override-btn:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.remove-btn {
		background: transparent;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		padding: 2px 6px;
		font-size: 16px;
		line-height: 1;
		cursor: pointer;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
	}

	.add-var-form {
		margin-top: 4px;
	}

	.add-var-row {
		display: flex;
		gap: 4px;
		margin-bottom: 4px;
	}

	.add-name {
		flex: 1;
		min-width: 0;
		padding: 4px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.add-value {
		flex: 1;
		min-width: 0;
		padding: 4px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.add-type {
		padding: 4px 4px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 11px;
		cursor: pointer;
		width: 52px;
	}

	.add-value.invalid {
		border-color: var(--error-color, #ef4444);
		background: rgba(239, 68, 68, 0.05);
	}

	.add-name:focus,
	.add-value:focus,
	.add-type:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.add-btn {
		width: 100%;
		padding: 6px;
		font-size: 12px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
	}

	.add-btn:hover:not(:disabled) {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.add-btn:disabled {
		opacity: 0.5;
		cursor: not-allowed;
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
		padding: 4px 4px 4px 6px;
		color: var(--accent-color, #3b82f6);
		font-size: 12px;
		font-weight: 700;
		user-select: none;
	}

	.envvar-name-input {
		flex: 1;
		padding: 4px 6px 4px 0;
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

	.envvar-controls {
		margin-top: 4px;
		justify-content: flex-end;
	}

	.add-envvar-inputs {
		flex: 1;
		display: flex;
		flex-direction: column;
		gap: 4px;
		min-width: 0;
	}

	.add-var-row .envvar-toggle {
		margin-left: 0;
	}

	.options-chips {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
		margin-top: 4px;
		align-items: center;
	}

	.option-chip {
		display: inline-flex;
		align-items: center;
		gap: 2px;
		padding: 1px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 10px;
		font-size: 11px;
		color: var(--text-primary, #fff);
	}

	.chip-remove {
		background: transparent;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
		padding: 0 2px;
		cursor: pointer;
		line-height: 1;
	}

	.chip-remove:hover {
		color: var(--error-color, #ef4444);
	}

	.option-add-input {
		flex: 0 1 80px;
		min-width: 60px;
		padding: 2px 6px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 10px;
		color: var(--text-primary, #fff);
		font-size: 11px;
	}

	.option-add-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
		border-style: solid;
	}
</style>
