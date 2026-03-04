<script lang="ts">
	import { validateVarValue, type NodeVariable } from '$lib/stores/variablesStore';
	import { updateNodeNodeVars, projectNodeVars } from '$lib/stores/variablesStore';
	import { pushHistory } from '$lib/stores/flowStore';
	import { isEnvVar, makeEnvVar, getEnvVarName, getEnvVarDefault } from '$lib/utils/envvar';

	const VAR_TYPES = ['str', 'int', 'float', 'bool', 'json'] as const;

	interface NodeVarsEntry {
		nodeId: string;
		nodeName: string;
		vars: Record<string, NodeVariable>;
	}

	interface Props {
		allNodesVars: NodeVarsEntry[];
	}

	let { allNodesVars }: Props = $props();

	// Track which variables are in env var mode, keyed by `${nodeId}:${varName}`
	let envVarMode: Record<string, boolean> = $state({});

	// Initialize env var mode from current values
	$effect(() => {
		const newMode: Record<string, boolean> = {};
		for (const entry of allNodesVars) {
			for (const [name, variable] of Object.entries(entry.vars)) {
				const key = `${entry.nodeId}:${name}`;
				if (isEnvVar(variable.value)) {
					newMode[key] = true;
				}
			}
		}
		envVarMode = newMode;
	});

	function toggleEnvVarMode(nodeId: string, vars: Record<string, NodeVariable>, name: string) {
		const key = `${nodeId}:${name}`;
		const wasEnvVar = envVarMode[key] || false;
		envVarMode[key] = !wasEnvVar;

		const updated = { ...vars };
		const existing = updated[name];
		if (!existing) return;

		if (wasEnvVar) {
			const defaultVal = isEnvVar(existing.value) ? getEnvVarDefault(existing.value) : null;
			updated[name] = { ...existing, value: defaultVal != null ? String(defaultVal) : '' };
		} else {
			const literalVal = typeof existing.value === 'string' ? existing.value : '';
			updated[name] = { ...existing, value: makeEnvVar('', literalVal || undefined) };
		}
		updateNodeNodeVars(nodeId, updated);
		pushHistory();
	}

	function updateVarValue(nodeId: string, vars: Record<string, NodeVariable>, name: string, value: string) {
		const updated = { ...vars };
		updated[name] = { ...updated[name], value };
		updateNodeNodeVars(nodeId, updated);
	}

	function updateVarEnvName(nodeId: string, vars: Record<string, NodeVariable>, name: string, envName: string) {
		const updated = { ...vars };
		const existing = updated[name];
		if (!existing) return;
		const currentDefault = isEnvVar(existing.value) ? getEnvVarDefault(existing.value) : null;
		updated[name] = { ...existing, value: makeEnvVar(envName, currentDefault) };
		updateNodeNodeVars(nodeId, updated);
	}

	function updateVarEnvDefault(nodeId: string, vars: Record<string, NodeVariable>, name: string, defaultVal: string) {
		const updated = { ...vars };
		const existing = updated[name];
		if (!existing) return;
		const currentName = isEnvVar(existing.value) ? getEnvVarName(existing.value) : '';
		updated[name] = { ...existing, value: makeEnvVar(currentName, defaultVal || undefined) };
		updateNodeNodeVars(nodeId, updated);
	}

	function updateVarType(nodeId: string, vars: Record<string, NodeVariable>, name: string, type: string) {
		const updated = { ...vars };
		updated[name] = { ...updated[name], type };
		updateNodeNodeVars(nodeId, updated);
	}

	function addOption(nodeId: string, vars: Record<string, NodeVariable>, name: string, optionValue: string) {
		const trimmed = optionValue.trim();
		if (!trimmed) return;
		const updated = { ...vars };
		const existing = updated[name];
		if (!existing) return;
		const opts = existing.options ? [...existing.options] : [];
		if (!opts.map(String).includes(trimmed)) {
			opts.push(trimmed);
		}
		updated[name] = { ...existing, options: opts };
		updateNodeNodeVars(nodeId, updated);
		pushHistory();
	}

	function removeOption(nodeId: string, vars: Record<string, NodeVariable>, name: string, index: number) {
		const updated = { ...vars };
		const existing = updated[name];
		if (!existing || !existing.options) return;
		const opts = existing.options.filter((_, i) => i !== index);
		updated[name] = { ...existing, options: opts.length > 0 ? opts : undefined };
		updateNodeNodeVars(nodeId, updated);
		pushHistory();
	}

	function removeVar(nodeId: string, vars: Record<string, NodeVariable>, name: string) {
		const { [name]: _, ...rest } = vars;
		delete envVarMode[`${nodeId}:${name}`];
		updateNodeNodeVars(nodeId, rest);
		pushHistory();
	}
</script>

<div class="all-vars-section">
	{#if allNodesVars.length === 0}
		<p class="empty-hint">No nodes have variables defined</p>
	{:else}
		{#each allNodesVars as entry (entry.nodeId)}
			<div class="node-group">
				<div class="node-group-header">{entry.nodeName}</div>
				{#each Object.entries(entry.vars) as [name, variable] (name)}
					{@const isNetDefault = name in $projectNodeVars}
					{@const isInheritVar = variable.inherit === true}
					{@const globalVar = $projectNodeVars[name]}
					{@const effectiveType = isInheritVar && globalVar ? (globalVar.type || 'str') : (variable.type || 'str')}
					{@const effectiveOptions = isInheritVar && globalVar ? globalVar.options : variable.options}
					{@const key = `${entry.nodeId}:${name}`}
					{@const inEnvMode = envVarMode[key] || false}
					{@const error = validateVarValue(variable.value, effectiveType, effectiveOptions)}
					<div class="var-row">
						<div class="var-header">
							<span class="var-name">{name}</span>
							<span class="var-type-badge">{effectiveType}</span>
							{#if isInheritVar}
								<span class="var-badge inherit">inherit</span>
							{:else if isNetDefault}
								<span class="var-badge override">override</span>
							{/if}
							<button
								class="envvar-toggle"
								class:active={inEnvMode}
								title={inEnvMode ? 'Switch to literal value' : 'Switch to environment variable'}
								onclick={() => toggleEnvVarMode(entry.nodeId, entry.vars, name)}
							>$</button>
						</div>
						{#if inEnvMode}
							<div class="envvar-input-group">
								<div class="envvar-name-row">
									<span class="envvar-prefix">$</span>
									<input
										type="text"
										class="envvar-name-input"
										value={getEnvVarName(variable.value)}
										placeholder="ENV_VAR_NAME"
										oninput={(e) => updateVarEnvName(entry.nodeId, entry.vars, name, (e.target as HTMLInputElement).value)}
										onblur={() => pushHistory()}
									/>
								</div>
								<div class="envvar-default-row">
									<span class="envvar-default-label">default:</span>
									<input
										type="text"
										class="envvar-default-input"
										value={getEnvVarDefault(variable.value) ?? ''}
										placeholder="(none)"
										oninput={(e) => updateVarEnvDefault(entry.nodeId, entry.vars, name, (e.target as HTMLInputElement).value)}
										onblur={() => pushHistory()}
									/>
								</div>
							</div>
							<div class="var-edit-row envvar-controls">
								<select
									value={variable.type || 'str'}
									onchange={(e) => {
										updateVarType(entry.nodeId, entry.vars, name, (e.target as HTMLSelectElement).value);
										pushHistory();
									}}
								>
									{#each VAR_TYPES as t}
										<option value={t}>{t}</option>
									{/each}
								</select>
								<button
									class="remove-btn"
									onclick={() => removeVar(entry.nodeId, entry.vars, name)}
									title="Remove variable"
								>
									&times;
								</button>
							</div>
						{:else}
							<div class="var-edit-row">
								{#if effectiveOptions?.length}
									<select
										class="options-select"
										value={variable.value != null ? (typeof variable.value === 'string' ? variable.value : String(variable.value)) : ''}
										onchange={(e) => {
											updateVarValue(entry.nodeId, entry.vars, name, (e.target as HTMLSelectElement).value);
											pushHistory();
										}}
									>
										{#if variable.value == null || variable.value === '' || (effectiveOptions && !effectiveOptions.map(String).includes(String(variable.value ?? '')))}
											<option value={variable.value != null ? (typeof variable.value === 'string' ? variable.value : String(variable.value)) : ''}>{variable.value == null || variable.value === '' ? '(select)' : variable.value}</option>
										{/if}
										{#each effectiveOptions as opt}
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
										oninput={(e) => updateVarValue(entry.nodeId, entry.vars, name, (e.target as HTMLInputElement).value)}
										onblur={() => pushHistory()}
									/>
								{/if}
								{#if !isInheritVar}
									<select
										value={variable.type || 'str'}
										onchange={(e) => {
											updateVarType(entry.nodeId, entry.vars, name, (e.target as HTMLSelectElement).value);
											pushHistory();
										}}
									>
										{#each VAR_TYPES as t}
											<option value={t}>{t}</option>
										{/each}
									</select>
								{/if}
								<button
									class="remove-btn"
									onclick={() => removeVar(entry.nodeId, entry.vars, name)}
									title="Remove variable"
								>
									&times;
								</button>
							</div>
							{#if error}
								<div class="var-error">{error}</div>
							{/if}
							{#if !inEnvMode && !isInheritVar}
								<div class="options-chips">
									{#if variable.options?.length}
										{#each variable.options as opt, i}
											<span class="option-chip">
												{opt}
												<button class="chip-remove" onclick={() => removeOption(entry.nodeId, entry.vars, name, i)}>&times;</button>
											</span>
										{/each}
									{/if}
									<input
										type="text"
										class="option-add-input"
										placeholder="+ option"
										onkeydown={(e) => {
											if (e.key === 'Enter') {
												addOption(entry.nodeId, entry.vars, name, (e.target as HTMLInputElement).value);
												(e.target as HTMLInputElement).value = '';
											}
										}}
									/>
								</div>
							{/if}
						{/if}
					</div>
				{/each}
			</div>
		{/each}
	{/if}
</div>

<style>
	.all-vars-section {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.empty-hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		text-align: center;
		padding: 8px;
		margin: 0;
	}

	.node-group {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		overflow: hidden;
	}

	.node-group-header {
		padding: 6px 8px;
		font-size: 11px;
		font-weight: 600;
		color: var(--text-primary, #fff);
		background: var(--bg-tertiary, #2d2d2d);
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.var-row {
		padding: 6px 8px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.var-row:last-child {
		border-bottom: none;
	}

	.var-header {
		display: flex;
		align-items: center;
		gap: 6px;
		margin-bottom: 4px;
	}

	.var-name {
		font-size: 11px;
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
		padding: 3px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 11px;
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
		margin-top: 2px;
		padding-left: 2px;
	}

	.options-select {
		flex: 1;
		min-width: 0;
		padding: 3px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 11px;
		cursor: pointer;
	}

	.options-select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.var-edit-row select {
		padding: 3px 4px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 10px;
		cursor: pointer;
		width: 48px;
	}

	.var-edit-row select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.remove-btn {
		background: transparent;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		padding: 2px 6px;
		font-size: 14px;
		line-height: 1;
		cursor: pointer;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
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
		padding: 3px 4px 3px 6px;
		color: var(--accent-color, #3b82f6);
		font-size: 11px;
		font-weight: 700;
		user-select: none;
	}

	.envvar-name-input {
		flex: 1;
		padding: 3px 6px 3px 0;
		background: transparent;
		border: none;
		color: var(--text-primary, #fff);
		font-size: 11px;
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
		padding: 2px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 10px;
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
		font-size: 10px;
		color: var(--text-primary, #fff);
	}

	.chip-remove {
		background: transparent;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
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
		font-size: 10px;
	}

	.option-add-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
		border-style: solid;
	}
</style>
