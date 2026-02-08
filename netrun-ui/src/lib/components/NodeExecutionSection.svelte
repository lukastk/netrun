<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';
	import { configSchema, getFieldDescription } from '$lib/stores/schemaStore';
	import AutoConfigFields from './AutoConfigFields.svelte';

	function desc(field: string): string | undefined {
		return getFieldDescription($configSchema, 'NodeExecutionConfig', field);
	}

	interface NodeExecutionConfig {
		pools?: string[];
		// Execution functions (regular nodes only)
		exec_node_func?: string | null;
		start_node_func?: string | null;
		stop_node_func?: string | null;
		on_node_failure?: string | null;
		[key: string]: unknown;
	}

	interface Props {
		executionConfig: Record<string, unknown> | null | undefined;
		availablePools: string[];
		isFactory?: boolean;
		onUpdate: (config: Record<string, unknown> | null) => void;
	}

	let { executionConfig, availablePools, isFactory = false, onUpdate }: Props = $props();

	let execSchema = $derived($configSchema?.models['NodeExecutionConfig'] ?? null);

	// Type assertion helper
	function getConfig(): NodeExecutionConfig {
		return (executionConfig || {}) as NodeExecutionConfig;
	}

	// Get current value or default
	function getValue<K extends keyof NodeExecutionConfig>(key: K): NodeExecutionConfig[K] {
		const config = getConfig();
		return key in config ? config[key] : undefined;
	}

	// Get pools with validation
	let selectedPools = $derived(getValue('pools') || ['main']);

	// Check if a pool exists in available pools
	function poolExists(poolName: string): boolean {
		return availablePools.includes(poolName);
	}

	// Get invalid pools (selected but don't exist)
	let invalidPools = $derived(selectedPools.filter(p => !poolExists(p)));

	// Update a single field
	function updateFieldWithHistory<K extends keyof NodeExecutionConfig>(key: K, value: NodeExecutionConfig[K]) {
		const current = getConfig();
		onUpdate({
			...current,
			[key]: value,
		} as Record<string, unknown>);
		pushHistory();
	}

	function updateFieldLive<K extends keyof NodeExecutionConfig>(key: K, value: NodeExecutionConfig[K]) {
		const current = getConfig();
		onUpdate({
			...current,
			[key]: value,
		} as Record<string, unknown>);
	}

	// Pool selection handlers
	function togglePool(poolName: string) {
		const current = selectedPools;
		let newPools: string[];
		if (current.includes(poolName)) {
			newPools = current.filter(p => p !== poolName);
			if (newPools.length === 0) {
				newPools = ['main'];
			}
		} else {
			newPools = [...current, poolName];
		}
		updateFieldWithHistory('pools', newPools);
	}

	function addCustomPool(poolName: string) {
		if (!poolName.trim()) return;
		const current = selectedPools;
		if (!current.includes(poolName)) {
			updateFieldWithHistory('pools', [...current, poolName]);
		}
	}

	function removePool(poolName: string) {
		const current = selectedPools;
		const newPools = current.filter(p => p !== poolName);
		if (newPools.length === 0) {
			updateFieldWithHistory('pools', ['main']);
		} else {
			updateFieldWithHistory('pools', newPools);
		}
	}

	// Custom pool input
	let customPoolInput = $state('');
</script>

<div class="execution-section">
	<!-- Pools Selection (custom) -->
	<div class="field" title={desc('pools')}>
		<label>Pools</label>
		<div class="pools-selection">
			{#each availablePools as poolName}
				<label class="pool-checkbox">
					<input
						type="checkbox"
						checked={selectedPools.includes(poolName)}
						onchange={() => togglePool(poolName)}
					/>
					<span class="pool-name">{poolName}</span>
				</label>
			{/each}
			{#if availablePools.length === 0}
				<span class="empty-hint">No pools configured</span>
			{/if}
		</div>

		{#if invalidPools.length > 0}
			<div class="invalid-pools">
				<span class="warning-icon">⚠</span>
				<span class="warning-text">Unknown pools:</span>
				{#each invalidPools as poolName}
					<span class="invalid-pool">
						{poolName}
						<button class="remove-invalid" onclick={() => removePool(poolName)} title="Remove">×</button>
					</span>
				{/each}
			</div>
		{/if}

		<div class="add-custom-pool">
			<input
				type="text"
				bind:value={customPoolInput}
				placeholder="Add pool name..."
				onkeydown={(e) => {
					if (e.key === 'Enter') {
						addCustomPool(customPoolInput);
						customPoolInput = '';
					}
				}}
			/>
			<button
				class="add-btn-small"
				onclick={() => {
					addCustomPool(customPoolInput);
					customPoolInput = '';
				}}
				disabled={!customPoolInput.trim()}
			>
				+
			</button>
		</div>
	</div>

	<!-- Execution Functions (custom) -->
	<div class="field-group">
		<div class="field-group-header">Execution Functions</div>
		<div class="field" title={desc('exec_node_func')}>
			<label>{isFactory ? 'Exec Function Override' : 'Exec Function'}</label>
			<input
				type="text"
				value={getValue('exec_node_func') ?? ''}
				placeholder={isFactory ? 'override factory default' : 'module.path.func'}
				oninput={(e) => {
					const val = (e.target as HTMLInputElement).value;
					updateFieldLive('exec_node_func', val || null);
				}}
				onblur={() => pushHistory()}
			/>
		</div>
		<div class="field" title={desc('start_node_func')}>
			<label>Start Function</label>
			<input
				type="text"
				value={getValue('start_node_func') ?? ''}
				placeholder="module.path.func"
				oninput={(e) => {
					const val = (e.target as HTMLInputElement).value;
					updateFieldLive('start_node_func', val || null);
				}}
				onblur={() => pushHistory()}
			/>
		</div>
		<div class="field" title={desc('stop_node_func')}>
			<label>Stop Function</label>
			<input
				type="text"
				value={getValue('stop_node_func') ?? ''}
				placeholder="module.path.func"
				oninput={(e) => {
					const val = (e.target as HTMLInputElement).value;
					updateFieldLive('stop_node_func', val || null);
				}}
				onblur={() => pushHistory()}
			/>
		</div>
		<div class="field" title={desc('on_node_failure')}>
			<label>On Failure Function</label>
			<input
				type="text"
				value={getValue('on_node_failure') ?? ''}
				placeholder="module.path.func"
				oninput={(e) => {
					const val = (e.target as HTMLInputElement).value;
					updateFieldLive('on_node_failure', val || null);
				}}
				onblur={() => pushHistory()}
			/>
		</div>
	</div>

	<!-- Auto-rendered scalar fields (from schema) -->
	{#if execSchema}
		<div class="field-group">
			<div class="field-group-header">Configuration</div>
			<AutoConfigFields
				modelName="NodeExecutionConfig"
				schema={execSchema}
				values={getConfig() as Record<string, unknown>}
				onUpdate={(updates) => { onUpdate(updates); pushHistory(); }}
				onUpdateLive={(updates) => onUpdate(updates)}
			/>
		</div>
	{/if}
</div>

<style>
	.execution-section {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.field label {
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.field select,
	.field input[type="text"],
	.field input[type="number"] {
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.field select {
		cursor: pointer;
	}

	.field input:focus,
	.field select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field input[type="number"] {
		width: 100%;
	}

	.field-group {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 8px;
	}

	.field-group-header {
		font-size: 10px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
		margin-bottom: 8px;
	}

	.field-group .field {
		margin-bottom: 8px;
	}

	.field-group .field:last-child {
		margin-bottom: 0;
	}

	.checkbox-field {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 12px;
		color: var(--text-primary, #fff);
		cursor: pointer;
		margin-bottom: 6px;
	}

	.checkbox-field:last-child {
		margin-bottom: 0;
	}

	.checkbox-field input[type="checkbox"] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}

	/* Pools selection */
	.pools-selection {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		margin-bottom: 8px;
	}

	.pool-checkbox {
		display: flex;
		align-items: center;
		gap: 4px;
		padding: 4px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		cursor: pointer;
		font-size: 11px;
	}

	.pool-checkbox:hover {
		border-color: var(--accent-color, #3b82f6);
	}

	.pool-checkbox input {
		width: 12px;
		height: 12px;
		cursor: pointer;
	}

	.pool-name {
		color: var(--text-primary, #fff);
	}

	.empty-hint {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		font-style: italic;
	}

	.invalid-pools {
		display: flex;
		align-items: center;
		flex-wrap: wrap;
		gap: 6px;
		padding: 6px 8px;
		background: rgba(239, 146, 68, 0.1);
		border: 1px solid rgba(239, 146, 68, 0.3);
		border-radius: 4px;
		margin-bottom: 8px;
	}

	.warning-icon {
		font-size: 12px;
	}

	.warning-text {
		font-size: 11px;
		color: #ef9244;
	}

	.invalid-pool {
		display: inline-flex;
		align-items: center;
		gap: 4px;
		padding: 2px 6px;
		background: rgba(239, 146, 68, 0.2);
		border-radius: 3px;
		font-size: 11px;
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		color: #ef9244;
	}

	.remove-invalid {
		background: transparent;
		border: none;
		color: #ef9244;
		cursor: pointer;
		padding: 0 2px;
		font-size: 12px;
		line-height: 1;
	}

	.remove-invalid:hover {
		color: #ef4444;
	}

	.add-custom-pool {
		display: flex;
		gap: 4px;
	}

	.add-custom-pool input {
		flex: 1;
		padding: 4px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 11px;
	}

	.add-custom-pool input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.add-btn-small {
		padding: 4px 8px;
		background: transparent;
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		font-size: 12px;
	}

	.add-btn-small:hover:not(:disabled) {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.add-btn-small:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
</style>
