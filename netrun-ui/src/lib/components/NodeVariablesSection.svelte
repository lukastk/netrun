<script lang="ts">
	import { validateVarValue, type NodeVariable } from '$lib/stores/variablesStore';

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
	let newError = $derived(validateVarValue(newValue, newType));

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

	function updateVarValue(name: string, value: string) {
		const current = { ...variables };
		const existing = current[name] || { value: '', type: 'str' };
		current[name] = { ...existing, value };
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
		onUpdate(rest);
	}

	function addVar() {
		const trimmedName = newName.trim();
		if (!trimmedName) return;
		const current = { ...variables };
		current[trimmedName] = { value: newValue, type: newType === 'str' ? undefined : newType };
		onUpdate(current);
		newName = '';
		newValue = '';
		newType = 'str';
	}

	function overrideInherited(name: string) {
		const inherited = inheritedVariables[name];
		if (!inherited) return;
		const current = { ...variables };
		current[name] = { ...inherited };
		onUpdate(current);
	}
</script>

<div class="variables-section">
	{#if displayVars.length === 0}
		<p class="empty-hint">No variables defined</p>
	{:else}
		{#each displayVars as { name, variable, source } (name)}
			<div class="var-row" class:inherited={source === 'inherited'}>
				<div class="var-header">
					<span class="var-name">{name}</span>
					<span class="var-type-badge">{variable.type || 'str'}</span>
					{#if source === 'inherited'}
						<span class="var-badge default">default</span>
					{:else if level === 'node' && name in inheritedVariables}
						<span class="var-badge override">override</span>
					{/if}
				</div>
				{#if source === 'inherited'}
					<div class="var-inherited-row">
						<span class="var-value-preview">{variable.value}</span>
						<button
							class="override-btn"
							onclick={() => overrideInherited(name)}
							title="Override this variable at node level"
						>
							Override
						</button>
					</div>
				{:else}
					{@const error = validateVarValue(variable.value, variable.type)}
					<div class="var-edit-row">
						<input
							type="text"
							value={variable.value}
							placeholder="value"
							class:invalid={error !== null}
							title={error || ''}
							oninput={(e) => updateVarValue(name, (e.target as HTMLInputElement).value)}
						/>
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
			<input
				type="text"
				bind:value={newValue}
				placeholder="value"
				class="add-value"
				class:invalid={newValue !== '' && newError !== null}
				title={newError || ''}
				onkeydown={(e) => { if (e.key === 'Enter' && !newError) addVar(); }}
			/>
			<select bind:value={newType} class="add-type">
				{#each VAR_TYPES as t}
					<option value={t}>{t}</option>
				{/each}
			</select>
		</div>
		{#if newValue !== '' && newError}
			<div class="var-error">{newError}</div>
		{/if}
		<button
			class="add-btn"
			onclick={addVar}
			disabled={!newName.trim() || (newValue !== '' && newError !== null)}
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
</style>
