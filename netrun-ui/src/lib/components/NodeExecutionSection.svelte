<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';

	// Pool allocation methods (from Python RunAllocationMethod enum)
	type PoolAllocationMethod = 'ROUND_ROBIN' | 'RANDOM' | 'LEAST_BUSY';

	interface NodeExecutionConfig {
		pools?: string[];
		max_parallel_epochs?: number | null;
		rate_limit_per_second?: number | null;
		retries?: number;
		retry_wait?: number;
		timeout?: number | null;
		capture_prints?: boolean;
		print_flush_interval?: number;
		print_buffer_max_size?: number | null;
		print_echo_stdout?: boolean;
		pool_allocation_method?: PoolAllocationMethod | null;
		defer_startup?: boolean;
	}

	interface Props {
		executionConfig: Record<string, unknown> | null | undefined;
		availablePools: string[];
		onUpdate: (config: Record<string, unknown> | null) => void;
	}

	let { executionConfig, availablePools, onUpdate }: Props = $props();

	// Type assertion helper
	function getConfig(): NodeExecutionConfig {
		return (executionConfig || {}) as NodeExecutionConfig;
	}

	// Defaults
	const defaultConfig: NodeExecutionConfig = {
		pools: ['main'],
		retries: 0,
		retry_wait: 0,
		timeout: null,
		max_parallel_epochs: null,
		rate_limit_per_second: null,
		capture_prints: true,
		print_flush_interval: 0.1,
		print_buffer_max_size: null,
		print_echo_stdout: false,
		pool_allocation_method: null,
		defer_startup: false,
	};

	// Get current value or default
	function getValue<K extends keyof NodeExecutionConfig>(key: K): NodeExecutionConfig[K] {
		const config = getConfig();
		return key in config ? config[key] : defaultConfig[key];
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
	function updateField<K extends keyof NodeExecutionConfig>(key: K, value: NodeExecutionConfig[K]) {
		const current = getConfig();
		onUpdate({
			...current,
			[key]: value,
		} as Record<string, unknown>);
	}

	function updateFieldLive<K extends keyof NodeExecutionConfig>(key: K, value: NodeExecutionConfig[K]) {
		const current = getConfig();
		onUpdate({
			...current,
			[key]: value,
		} as Record<string, unknown>);
	}

	function updateFieldWithHistory<K extends keyof NodeExecutionConfig>(key: K, value: NodeExecutionConfig[K]) {
		updateField(key, value);
		pushHistory();
	}

	// Pool selection handlers
	function togglePool(poolName: string) {
		const current = selectedPools;
		let newPools: string[];
		if (current.includes(poolName)) {
			// Remove pool (but keep at least one)
			newPools = current.filter(p => p !== poolName);
			if (newPools.length === 0) {
				newPools = ['main']; // Fallback to main
			}
		} else {
			// Add pool
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

	// Allocation method labels
	const allocationMethods: { value: PoolAllocationMethod | 'default'; label: string }[] = [
		{ value: 'default', label: 'Default (from Net)' },
		{ value: 'ROUND_ROBIN', label: 'Round Robin' },
		{ value: 'RANDOM', label: 'Random' },
		{ value: 'LEAST_BUSY', label: 'Least Busy' },
	];

	// Custom pool input
	let customPoolInput = $state('');
</script>

<div class="execution-section">
	<!-- Pools Selection -->
	<div class="field">
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

		<!-- Show invalid pools with warning -->
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

		<!-- Add custom pool -->
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

	<!-- Pool Allocation Method (only show if multiple pools) -->
	{#if selectedPools.length > 1}
		<div class="field">
			<label>Allocation Method</label>
			<select
				value={getValue('pool_allocation_method') || 'default'}
				onchange={(e) => {
					const value = (e.target as HTMLSelectElement).value;
					updateFieldWithHistory('pool_allocation_method', value === 'default' ? null : value as PoolAllocationMethod);
				}}
			>
				{#each allocationMethods as method}
					<option value={method.value}>{method.label}</option>
				{/each}
			</select>
		</div>
	{/if}

	<!-- Execution Limits -->
	<div class="field-group">
		<div class="field-group-header">Execution Limits</div>
		<div class="field-row">
			<div class="field">
				<label>Max Parallel Epochs</label>
				<input
					type="number"
					min="1"
					value={getValue('max_parallel_epochs') ?? ''}
					placeholder="unlimited"
					oninput={(e) => {
						const val = (e.target as HTMLInputElement).value;
						updateFieldLive('max_parallel_epochs', val ? parseInt(val) : null);
					}}
					onblur={() => pushHistory()}
				/>
			</div>
			<div class="field">
				<label>Rate Limit (/sec)</label>
				<input
					type="number"
					min="0"
					step="0.1"
					value={getValue('rate_limit_per_second') ?? ''}
					placeholder="unlimited"
					oninput={(e) => {
						const val = (e.target as HTMLInputElement).value;
						updateFieldLive('rate_limit_per_second', val ? parseFloat(val) : null);
					}}
					onblur={() => pushHistory()}
				/>
			</div>
		</div>
	</div>

	<!-- Retry Settings -->
	<div class="field-group">
		<div class="field-group-header">Retry Settings</div>
		<div class="field-row">
			<div class="field">
				<label>Retries</label>
				<input
					type="number"
					min="0"
					value={getValue('retries') ?? 0}
					oninput={(e) => updateFieldLive('retries', parseInt((e.target as HTMLInputElement).value) || 0)}
					onblur={() => pushHistory()}
				/>
			</div>
			<div class="field">
				<label>Retry Wait (s)</label>
				<input
					type="number"
					min="0"
					step="0.1"
					value={getValue('retry_wait') ?? 0}
					oninput={(e) => updateFieldLive('retry_wait', parseFloat((e.target as HTMLInputElement).value) || 0)}
					onblur={() => pushHistory()}
				/>
			</div>
		</div>
		<div class="field">
			<label>Timeout (s)</label>
			<input
				type="number"
				min="0"
				step="0.1"
				value={getValue('timeout') ?? ''}
				placeholder="none"
				oninput={(e) => {
					const val = (e.target as HTMLInputElement).value;
					updateFieldLive('timeout', val ? parseFloat(val) : null);
				}}
				onblur={() => pushHistory()}
			/>
		</div>
	</div>

	<!-- Print Settings -->
	<div class="field-group">
		<div class="field-group-header">Print Capture</div>
		<label class="checkbox-field">
			<input
				type="checkbox"
				checked={getValue('capture_prints') ?? true}
				onchange={(e) => updateFieldWithHistory('capture_prints', (e.target as HTMLInputElement).checked)}
			/>
			<span>Capture prints</span>
		</label>
		{#if getValue('capture_prints') ?? true}
			<div class="field-row">
				<div class="field">
					<label>Flush Interval (s)</label>
					<input
						type="number"
						min="0"
						step="0.01"
						value={getValue('print_flush_interval') ?? 0.1}
						oninput={(e) => updateFieldLive('print_flush_interval', parseFloat((e.target as HTMLInputElement).value) || 0.1)}
						onblur={() => pushHistory()}
					/>
				</div>
				<div class="field">
					<label>Buffer Max Size</label>
					<input
						type="number"
						min="0"
						value={getValue('print_buffer_max_size') ?? ''}
						placeholder="unlimited"
						oninput={(e) => {
							const val = (e.target as HTMLInputElement).value;
							updateFieldLive('print_buffer_max_size', val ? parseInt(val) : null);
						}}
						onblur={() => pushHistory()}
					/>
				</div>
			</div>
			<label class="checkbox-field">
				<input
					type="checkbox"
					checked={getValue('print_echo_stdout') ?? false}
					onchange={(e) => updateFieldWithHistory('print_echo_stdout', (e.target as HTMLInputElement).checked)}
				/>
				<span>Echo to stdout</span>
			</label>
		{/if}
	</div>

	<!-- Advanced -->
	<div class="field-group">
		<div class="field-group-header">Advanced</div>
		<label class="checkbox-field">
			<input
				type="checkbox"
				checked={getValue('defer_startup') ?? false}
				onchange={(e) => updateFieldWithHistory('defer_startup', (e.target as HTMLInputElement).checked)}
			/>
			<span>Defer startup</span>
		</label>
	</div>
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

	.field-row {
		display: flex;
		gap: 8px;
	}

	.field-row .field {
		flex: 1;
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
