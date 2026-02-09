<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';
	import { configSchema, getFieldDescription } from '$lib/stores/schemaStore';
	import AutoConfigFields from './AutoConfigFields.svelte';
	import { tooltip } from '$lib/utils/tooltip';
	import { isEnvVar, isVarRef, makeEnvVar, getEnvVarName, getEnvVarDefault } from '$lib/utils/envvar';
	import { availableVarNames } from '$lib/stores/variablesStore';

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

	// Env var mode tracking for custom fields
	const envVarFields = ['pools', 'exec_node_func', 'start_node_func', 'stop_node_func', 'on_node_failure'] as const;
	let envVarModes: Record<string, boolean> = $state({});

	$effect(() => {
		const config = getConfig();
		const modes: Record<string, boolean> = {};
		for (const f of envVarFields) {
			if (isEnvVar(config[f])) modes[f] = true;
		}
		envVarModes = modes;
	});

	function toggleFieldEnvVar(field: string) {
		const config = getConfig();
		const wasEnv = envVarModes[field] || false;
		envVarModes[field] = !wasEnv;

		if (wasEnv) {
			// env → value
			const def = isEnvVar(config[field]) ? getEnvVarDefault(config[field]) : null;
			updateFieldWithHistory(field as keyof NodeExecutionConfig, def as any);
		} else {
			// value → env
			const current = config[field];
			onUpdate({ ...config, [field]: makeEnvVar('', current) } as Record<string, unknown>);
			pushHistory();
		}
	}

	function updateFieldEnvVarName(field: string, envName: string) {
		const config = getConfig();
		const def = isEnvVar(config[field]) ? getEnvVarDefault(config[field]) : null;
		onUpdate({ ...config, [field]: makeEnvVar(envName, def) } as Record<string, unknown>);
	}

	function updateFieldEnvVarDefault(field: string, defaultVal: unknown) {
		const config = getConfig();
		const name = isEnvVar(config[field]) ? getEnvVarName(config[field]) : '';
		onUpdate({ ...config, [field]: makeEnvVar(name, defaultVal) } as Record<string, unknown>);
	}
</script>

<div class="execution-section">
	<!-- Pools Selection (custom) -->
	<div class="field">
		<label>Pools{#if desc('pools')}<span class="has-tooltip-icon" use:tooltip={desc('pools')}>?</span>{/if}
			<button
				class="envvar-toggle"
				class:active={envVarModes['pools']}
				title={envVarModes['pools'] ? 'Switch to literal value' : 'Switch to environment variable'}
				onclick={() => toggleFieldEnvVar('pools')}
			>$</button>
		</label>
		{#if envVarModes['pools']}
			<div class="envvar-input-group">
				<div class="envvar-name-row">
					<span class="envvar-prefix">$</span>
					<input
						type="text"
						class="envvar-name-input"
						value={getEnvVarName(getConfig().pools)}
						placeholder="ENV_VAR_NAME"
						oninput={(e) => updateFieldEnvVarName('pools', (e.target as HTMLInputElement).value)}
						onblur={() => pushHistory()}
					/>
				</div>
				<div class="envvar-default-row">
					<span class="envvar-default-label">default:</span>
					<input
						type="text"
						class="envvar-default-input"
						value={(() => { const d = getEnvVarDefault(getConfig().pools); return Array.isArray(d) ? d.join(', ') : d ?? ''; })()}
						placeholder="main"
						oninput={(e) => {
							const v = (e.target as HTMLInputElement).value;
							const arr = v ? v.split(',').map(s => s.trim()).filter(Boolean) : null;
							updateFieldEnvVarDefault('pools', arr);
						}}
						onblur={() => pushHistory()}
					/>
				</div>
			</div>
		{:else}
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
		{/if}
	</div>

	<!-- Execution Functions (custom) -->
	<div class="field-group">
		<div class="field-group-header">Execution Functions</div>
		{#each [
			{ key: 'exec_node_func', label: isFactory ? 'Exec Function Override' : 'Exec Function', placeholder: isFactory ? 'override factory default' : 'module.path.func' },
			{ key: 'start_node_func', label: 'Start Function', placeholder: 'module.path.func' },
			{ key: 'stop_node_func', label: 'Stop Function', placeholder: 'module.path.func' },
			{ key: 'on_node_failure', label: 'On Failure Function', placeholder: 'module.path.func' },
		] as funcField (funcField.key)}
			<div class="field">
				<label>
					{funcField.label}{#if desc(funcField.key)}<span class="has-tooltip-icon" use:tooltip={desc(funcField.key)}>?</span>{/if}
					<button
						class="envvar-toggle"
						class:active={envVarModes[funcField.key]}
						title={envVarModes[funcField.key] ? 'Switch to literal value' : 'Switch to environment variable'}
						onclick={() => toggleFieldEnvVar(funcField.key)}
					>$</button>
				</label>
				{#if envVarModes[funcField.key]}
					<div class="envvar-input-group">
						<div class="envvar-name-row">
							<span class="envvar-prefix">$</span>
							<input
								type="text"
								class="envvar-name-input"
								value={getEnvVarName(getConfig()[funcField.key])}
								placeholder="ENV_VAR_NAME"
								oninput={(e) => updateFieldEnvVarName(funcField.key, (e.target as HTMLInputElement).value)}
								onblur={() => pushHistory()}
							/>
						</div>
						<div class="envvar-default-row">
							<span class="envvar-default-label">default:</span>
							<input
								type="text"
								class="envvar-default-input"
								value={getEnvVarDefault(getConfig()[funcField.key]) ?? ''}
								placeholder={funcField.placeholder}
								oninput={(e) => updateFieldEnvVarDefault(funcField.key, (e.target as HTMLInputElement).value || null)}
								onblur={() => pushHistory()}
							/>
						</div>
					</div>
				{:else}
					<input
						type="text"
						value={getValue(funcField.key as keyof NodeExecutionConfig) ?? ''}
						placeholder={funcField.placeholder}
						oninput={(e) => {
							const val = (e.target as HTMLInputElement).value;
							updateFieldLive(funcField.key as keyof NodeExecutionConfig, val || null);
						}}
						onblur={() => pushHistory()}
					/>
				{/if}
			</div>
		{/each}
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
				availableVarNames={$availableVarNames}
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
		display: flex;
		align-items: center;
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

	/* Env var toggle and inputs */
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
</style>
