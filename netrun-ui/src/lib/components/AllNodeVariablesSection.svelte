<script lang="ts">
	import { validateVarValue, type NodeVariable } from '$lib/stores/variablesStore';
	import { updateNodeNodeVars, projectNodeVars } from '$lib/stores/variablesStore';
	import { pushHistory } from '$lib/stores/flowStore';

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

	function updateVarValue(nodeId: string, vars: Record<string, NodeVariable>, name: string, value: string) {
		const updated = { ...vars };
		updated[name] = { ...updated[name], value };
		updateNodeNodeVars(nodeId, updated);
	}

	function updateVarType(nodeId: string, vars: Record<string, NodeVariable>, name: string, type: string) {
		const updated = { ...vars };
		updated[name] = { ...updated[name], type };
		updateNodeNodeVars(nodeId, updated);
	}

	function removeVar(nodeId: string, vars: Record<string, NodeVariable>, name: string) {
		const { [name]: _, ...rest } = vars;
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
					{@const error = validateVarValue(variable.value, variable.type)}
					<div class="var-row">
						<div class="var-header">
							<span class="var-name">{name}</span>
							<span class="var-type-badge">{variable.type || 'str'}</span>
							{#if isNetDefault}
								<span class="var-badge override">override</span>
							{/if}
						</div>
						<div class="var-edit-row">
							<input
								type="text"
								value={variable.value}
								placeholder="value"
								class:invalid={error !== null}
								title={error || ''}
								oninput={(e) => updateVarValue(entry.nodeId, entry.vars, name, (e.target as HTMLInputElement).value)}
								onblur={() => pushHistory()}
							/>
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
						{#if error}
							<div class="var-error">{error}</div>
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
</style>
