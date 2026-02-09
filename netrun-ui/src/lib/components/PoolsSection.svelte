<script lang="ts">
	import { pushHistory, renamePoolInAllNodes } from '$lib/stores/flowStore';
	import { isEnvVar, makeEnvVar, getEnvVarName, getEnvVarDefault } from '$lib/utils/envvar';

	// Pool spec types
	type PoolSpecType = 'main' | 'thread' | 'multiprocess' | 'remote';

	interface MainPoolSpec {
		type: 'main';
	}

	interface ThreadPoolSpec {
		type: 'thread';
		num_workers: number;
	}

	interface MultiprocessPoolSpec {
		type: 'multiprocess';
		num_processes: number;
		threads_per_process: number;
	}

	interface RemotePoolSpec {
		type: 'remote';
		url: string;
		worker_name: string;
		num_processes: number;
		threads_per_process: number;
	}

	type PoolSpec = MainPoolSpec | ThreadPoolSpec | MultiprocessPoolSpec | RemotePoolSpec;

	interface PoolConfig {
		spec: PoolSpec;
		print_flush_interval?: number;
		capture_prints?: boolean;
	}

	interface Props {
		pools: Record<string, unknown> | null | undefined;
		onUpdate: (pools: Record<string, unknown> | null) => void;
	}

	let { pools, onUpdate }: Props = $props();

	// Type assertion helper
	function getTypedPools(): Record<string, PoolConfig> | null {
		if (pools === null || pools === undefined) return null;
		return pools as Record<string, PoolConfig>;
	}

	// Determine if using defaults (pools is null/undefined)
	let useDefaults = $derived(pools === null || pools === undefined);

	// Get pool entries for display
	let poolEntries = $derived.by(() => {
		const typedPools = getTypedPools();
		return typedPools ? Object.entries(typedPools) : [];
	});

	// Validate: at most one pool may have type 'main'
	let mainPoolCount = $derived(poolEntries.filter(([, c]) => c.spec?.type === 'main').length);
	let hasMultipleMainPools = $derived(mainPoolCount > 1);

	function toggleDefaults() {
		if (useDefaults) {
			// Switch to explicit: start with empty pools
			onUpdate({} as Record<string, unknown>);
		} else {
			// Switch to defaults
			onUpdate(null);
		}
		pushHistory();
	}

	function addPool() {
		const current = getTypedPools() || {};
		const name = generateUniqueName(current, 'pool');
		onUpdate({
			...current,
			[name]: { spec: { type: 'thread', num_workers: 4 } },
		} as Record<string, unknown>);
		pushHistory();
	}

	function removePool(name: string) {
		const current = { ...(getTypedPools() || {}) };
		delete current[name];
		// If no pools left, switch to defaults
		if (Object.keys(current).length === 0) {
			onUpdate(null);
		} else {
			onUpdate(current as Record<string, unknown>);
		}
		pushHistory();
	}

	function updatePoolName(oldName: string, newName: string) {
		if (oldName === newName || !newName.trim()) return;
		const current = getTypedPools() || {};
		if (newName in current) return; // Name already exists

		const updated: Record<string, PoolConfig> = {};
		for (const [name, config] of Object.entries(current)) {
			if (name === oldName) {
				updated[newName] = config;
			} else {
				updated[name] = config;
			}
		}
		onUpdate(updated as Record<string, unknown>);

		// Update all nodes that reference this pool in their execution config
		renamePoolInAllNodes(oldName, newName);

		pushHistory();
	}

	function updatePoolSpec(name: string, spec: PoolSpec) {
		const current = getTypedPools() || {};
		onUpdate({
			...current,
			[name]: { ...(current[name] || {}), spec },
		} as Record<string, unknown>);
	}

	function updatePoolSpecLive(name: string, spec: PoolSpec) {
		const current = getTypedPools() || {};
		onUpdate({
			...current,
			[name]: { ...(current[name] || {}), spec },
		} as Record<string, unknown>);
	}

	function changePoolType(name: string, newType: PoolSpecType) {
		let newSpec: PoolSpec;
		switch (newType) {
			case 'main':
				newSpec = { type: 'main' };
				break;
			case 'thread':
				newSpec = { type: 'thread', num_workers: 4 };
				break;
			case 'multiprocess':
				newSpec = { type: 'multiprocess', num_processes: 2, threads_per_process: 2 };
				break;
			case 'remote':
				newSpec = { type: 'remote', url: 'ws://localhost:8765', worker_name: 'worker', num_processes: 1, threads_per_process: 1 };
				break;
		}
		updatePoolSpec(name, newSpec);
		pushHistory();
	}

	function generateUniqueName(existing: Record<string, unknown>, base: string): string {
		if (!(base in existing)) return base;
		let i = 1;
		while (`${base}_${i}` in existing) {
			i++;
		}
		return `${base}_${i}`;
	}

	// Track pool name input values while editing (original name → current input value)
	let editingNames: Record<string, string> = $state({});

	function getPoolNameError(originalName: string): string | null {
		const editedValue = editingNames[originalName];
		if (editedValue === undefined) return null; // Not being edited
		const trimmed = editedValue.trim();
		if (trimmed === originalName) return null; // Unchanged
		if (!trimmed) return 'Name cannot be empty';
		const current = getTypedPools() || {};
		if (trimmed in current && trimmed !== originalName) {
			return `Pool "${trimmed}" already exists`;
		}
		return null;
	}

	// Pool type labels
	const poolTypeLabels: Record<PoolSpecType, string> = {
		main: 'Main Thread',
		thread: 'Thread Pool',
		multiprocess: 'Multiprocess',
		remote: 'Remote',
	};

	// Env var mode tracking: "poolName.fieldName" → boolean
	let poolEnvModes: Record<string, boolean> = $state({});

	// Initialize from current values
	$effect(() => {
		const modes: Record<string, boolean> = {};
		for (const [name, config] of poolEntries) {
			const spec = config.spec as unknown as Record<string, unknown>;
			if (spec) {
				for (const [key, val] of Object.entries(spec)) {
					if (key !== 'type' && isEnvVar(val)) {
						modes[`${name}.${key}`] = true;
					}
				}
			}
		}
		poolEnvModes = modes;
	});

	function togglePoolFieldEnvVar(poolName: string, fieldName: string, currentValue: unknown) {
		const key = `${poolName}.${fieldName}`;
		const wasEnv = poolEnvModes[key] || false;
		poolEnvModes[key] = !wasEnv;

		const current = getTypedPools() || {};
		const poolConfig = current[poolName];
		if (!poolConfig) return;

		const spec = { ...poolConfig.spec } as Record<string, unknown>;
		if (wasEnv) {
			// env → value
			const def = isEnvVar(currentValue) ? getEnvVarDefault(currentValue) : null;
			spec[fieldName] = def ?? getPoolFieldDefault(fieldName);
		} else {
			// value → env
			spec[fieldName] = makeEnvVar('', currentValue);
		}
		onUpdate({
			...current,
			[poolName]: { ...poolConfig, spec },
		} as Record<string, unknown>);
		pushHistory();
	}

	function getPoolFieldDefault(fieldName: string): unknown {
		const defaults: Record<string, unknown> = {
			num_workers: 4,
			num_processes: 2,
			threads_per_process: 2,
			url: 'ws://localhost:8765',
			worker_name: 'worker',
		};
		return defaults[fieldName] ?? null;
	}

	function updatePoolSpecFieldEnvVar(poolName: string, fieldName: string, spec: Record<string, unknown>, envName: string) {
		const current = getTypedPools() || {};
		const poolConfig = current[poolName];
		if (!poolConfig) return;

		const currentVal = spec[fieldName];
		const def = isEnvVar(currentVal) ? getEnvVarDefault(currentVal) : null;
		const newSpec = { ...spec, [fieldName]: makeEnvVar(envName, def) };
		onUpdate({
			...current,
			[poolName]: { ...poolConfig, spec: newSpec },
		} as Record<string, unknown>);
	}

	function updatePoolSpecFieldEnvDefault(poolName: string, fieldName: string, spec: Record<string, unknown>, defaultVal: unknown) {
		const current = getTypedPools() || {};
		const poolConfig = current[poolName];
		if (!poolConfig) return;

		const currentVal = spec[fieldName];
		const name = isEnvVar(currentVal) ? getEnvVarName(currentVal) : '';
		const newSpec = { ...spec, [fieldName]: makeEnvVar(name, defaultVal) };
		onUpdate({
			...current,
			[poolName]: { ...poolConfig, spec: newSpec },
		} as Record<string, unknown>);
	}
</script>

<div class="pools-section">
	<label class="defaults-toggle">
		<input type="checkbox" checked={useDefaults} onchange={toggleDefaults} />
		<span>Use defaults (auto-generate)</span>
	</label>

	{#if useDefaults}
		<div class="pools-list">
			<div class="pool-editor default-pool">
				<div class="pool-header">
					<span class="pool-name-readonly">main</span>
					<span class="pool-type-badge">Main Thread</span>
				</div>
			</div>
		</div>
	{:else}
		<div class="pools-list">
			{#each poolEntries as [name, config]}
				{@const spec = config.spec}
				{@const nameError = getPoolNameError(name)}
				<div class="pool-editor">
					<div class="pool-header">
						<input
							type="text"
							class="pool-name-input"
							class:pool-name-error={nameError}
							value={name}
							oninput={(e) => { editingNames[name] = (e.target as HTMLInputElement).value; }}
							onblur={(e) => {
								const val = (e.target as HTMLInputElement).value;
								if (!getPoolNameError(name)) {
									updatePoolName(name, val);
								} else {
									(e.target as HTMLInputElement).value = name;
								}
								delete editingNames[name];
							}}
							onkeydown={(e) => {
								if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
								if (e.key === 'Escape') {
									(e.target as HTMLInputElement).value = name;
									delete editingNames[name];
									(e.target as HTMLInputElement).blur();
								}
							}}
						/>
						<button
							class="remove-btn"
							onclick={() => removePool(name)}
							title="Remove pool"
						>
							×
						</button>
					</div>
					{#if nameError}
						<div class="pool-name-error-msg">{nameError}</div>
					{/if}

					<div class="pool-body">
						<div class="field">
							<label>Type</label>
							<select
								value={spec.type}
								onchange={(e) => changePoolType(name, (e.target as HTMLSelectElement).value as PoolSpecType)}
							>
								<option value="main">{poolTypeLabels.main}</option>
								<option value="thread">{poolTypeLabels.thread}</option>
								<option value="multiprocess">{poolTypeLabels.multiprocess}</option>
								<option value="remote">{poolTypeLabels.remote}</option>
							</select>
						</div>

						{#if spec.type === 'thread'}
							<div class="field">
								<label>Workers
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.num_workers`]}
										title={poolEnvModes[`${name}.num_workers`] ? 'Switch to literal' : 'Switch to env var'}
										onclick={() => togglePoolFieldEnvVar(name, 'num_workers', spec.num_workers)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.num_workers`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.num_workers)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'num_workers', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="number" class="envvar-default-input" value={getEnvVarDefault(spec.num_workers) ?? ''} placeholder="4"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'num_workers', spec as any, parseInt((e.target as HTMLInputElement).value) || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="number" min="1" value={spec.num_workers}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, num_workers: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
						{:else if spec.type === 'multiprocess'}
							<div class="field">
								<label>Processes
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.num_processes`]}
										onclick={() => togglePoolFieldEnvVar(name, 'num_processes', spec.num_processes)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.num_processes`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.num_processes)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'num_processes', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="number" class="envvar-default-input" value={getEnvVarDefault(spec.num_processes) ?? ''} placeholder="2"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'num_processes', spec as any, parseInt((e.target as HTMLInputElement).value) || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="number" min="1" value={spec.num_processes}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, num_processes: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
							<div class="field">
								<label>Threads/Process
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.threads_per_process`]}
										onclick={() => togglePoolFieldEnvVar(name, 'threads_per_process', spec.threads_per_process)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.threads_per_process`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.threads_per_process)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'threads_per_process', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="number" class="envvar-default-input" value={getEnvVarDefault(spec.threads_per_process) ?? ''} placeholder="2"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'threads_per_process', spec as any, parseInt((e.target as HTMLInputElement).value) || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="number" min="1" value={spec.threads_per_process}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, threads_per_process: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
						{:else if spec.type === 'remote'}
							<div class="field">
								<label>URL
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.url`]}
										onclick={() => togglePoolFieldEnvVar(name, 'url', spec.url)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.url`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.url)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'url', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="text" class="envvar-default-input" value={getEnvVarDefault(spec.url) ?? ''} placeholder="ws://localhost:8765"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'url', spec as any, (e.target as HTMLInputElement).value || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="text" value={spec.url} placeholder="ws://localhost:8765"
										oninput={(e) => updatePoolSpecLive(name, { ...spec, url: (e.target as HTMLInputElement).value })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
							<div class="field">
								<label>Worker Name
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.worker_name`]}
										onclick={() => togglePoolFieldEnvVar(name, 'worker_name', spec.worker_name)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.worker_name`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.worker_name)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'worker_name', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="text" class="envvar-default-input" value={getEnvVarDefault(spec.worker_name) ?? ''} placeholder="worker"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'worker_name', spec as any, (e.target as HTMLInputElement).value || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="text" value={spec.worker_name} placeholder="worker"
										oninput={(e) => updatePoolSpecLive(name, { ...spec, worker_name: (e.target as HTMLInputElement).value })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
							<div class="field">
								<label>Processes
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.num_processes`]}
										onclick={() => togglePoolFieldEnvVar(name, 'num_processes', spec.num_processes)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.num_processes`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.num_processes)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'num_processes', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="number" class="envvar-default-input" value={getEnvVarDefault(spec.num_processes) ?? ''} placeholder="1"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'num_processes', spec as any, parseInt((e.target as HTMLInputElement).value) || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="number" min="1" value={spec.num_processes}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, num_processes: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
							<div class="field">
								<label>Threads/Process
									<button class="envvar-toggle" class:active={poolEnvModes[`${name}.threads_per_process`]}
										onclick={() => togglePoolFieldEnvVar(name, 'threads_per_process', spec.threads_per_process)}>$</button>
								</label>
								{#if poolEnvModes[`${name}.threads_per_process`]}
									<div class="envvar-input-group">
										<div class="envvar-name-row">
											<span class="envvar-prefix">$</span>
											<input type="text" class="envvar-name-input" value={getEnvVarName(spec.threads_per_process)} placeholder="ENV_VAR"
												oninput={(e) => updatePoolSpecFieldEnvVar(name, 'threads_per_process', spec as any, (e.target as HTMLInputElement).value)} onblur={() => pushHistory()} />
										</div>
										<div class="envvar-default-row">
											<span class="envvar-default-label">default:</span>
											<input type="number" class="envvar-default-input" value={getEnvVarDefault(spec.threads_per_process) ?? ''} placeholder="1"
												oninput={(e) => updatePoolSpecFieldEnvDefault(name, 'threads_per_process', spec as any, parseInt((e.target as HTMLInputElement).value) || null)} onblur={() => pushHistory()} />
										</div>
									</div>
								{:else}
									<input type="number" min="1" value={spec.threads_per_process}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, threads_per_process: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()} />
								{/if}
							</div>
						{/if}
						<!-- main type has no additional fields -->
					</div>
				</div>
			{/each}
		</div>

		{#if hasMultipleMainPools}
			<div class="pool-error">
				Only one pool may have type "Main Thread". Found {mainPoolCount}.
			</div>
		{/if}

		<button class="add-btn" onclick={addPool}>+ Add Pool</button>
	{/if}
</div>

<style>
	.pools-section {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.defaults-toggle {
		display: flex;
		align-items: center;
		gap: 8px;
		cursor: pointer;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}

	.defaults-toggle input[type='checkbox'] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}

	.defaults-toggle:hover {
		color: var(--text-primary, #fff);
	}

	.pools-list {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.pool-editor {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		overflow: hidden;
	}

	.pool-editor.default-pool {
		opacity: 0.6;
	}

	.pool-name-readonly {
		font-size: 12px;
		font-weight: 500;
		color: var(--text-primary, #fff);
		padding: 4px 6px;
	}

	.pool-type-badge {
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		padding: 2px 6px;
	}

	.pool-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		gap: 8px;
	}

	.pool-name-input {
		flex: 1;
		font-size: 12px;
		font-weight: 500;
		padding: 4px 6px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid transparent;
		border-radius: 3px;
		color: var(--text-primary, #fff);
	}

	.pool-name-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.pool-name-input.pool-name-error {
		border-color: var(--error-color, #ef4444);
	}

	.pool-name-error-msg {
		font-size: 10px;
		color: var(--error-color, #ef4444);
		padding: 2px 8px 4px;
		background: var(--bg-tertiary, #2d2d2d);
	}

	.remove-btn {
		background: transparent;
		color: var(--text-secondary, #a0a0a0);
		padding: 2px 6px;
		font-size: 14px;
		line-height: 1;
		border: none;
		cursor: pointer;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
	}

	.pool-body {
		padding: 8px;
		display: flex;
		flex-direction: column;
		gap: 8px;
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
	.field input {
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

	.field input[type='number'] {
		width: 100%;
	}

	.field-row {
		display: flex;
		gap: 8px;
	}

	.field-row .field {
		flex: 1;
	}

	.pool-error {
		font-size: 11px;
		color: var(--error-color, #ef4444);
		padding: 6px 8px;
		background: rgba(239, 68, 68, 0.1);
		border: 1px solid rgba(239, 68, 68, 0.3);
		border-radius: 4px;
	}

	.add-btn {
		width: 100%;
		padding: 6px;
		font-size: 11px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
	}

	.add-btn:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
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
