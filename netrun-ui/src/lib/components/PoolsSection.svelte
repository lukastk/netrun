<script lang="ts">
	import { pushHistory, renamePoolInAllNodes } from '$lib/stores/flowStore';

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

	function toggleDefaults() {
		if (useDefaults) {
			// Switch to explicit: create a default "main" pool
			onUpdate({
				main: { spec: { type: 'main' } },
			} as Record<string, unknown>);
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

	// Pool type labels
	const poolTypeLabels: Record<PoolSpecType, string> = {
		main: 'Main Thread',
		thread: 'Thread Pool',
		multiprocess: 'Multiprocess',
		remote: 'Remote',
	};
</script>

<div class="pools-section">
	<label class="defaults-toggle">
		<input type="checkbox" checked={useDefaults} onchange={toggleDefaults} />
		<span>Use defaults (auto-generate)</span>
	</label>

	{#if !useDefaults}
		<div class="pools-list">
			{#each poolEntries as [name, config]}
				{@const spec = config.spec}
				<div class="pool-editor">
					<div class="pool-header">
						<input
							type="text"
							class="pool-name-input"
							value={name}
							onblur={(e) => updatePoolName(name, (e.target as HTMLInputElement).value)}
							onkeydown={(e) => {
								if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
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
								<label>Workers</label>
								<input
									type="number"
									min="1"
									value={spec.num_workers}
									oninput={(e) => updatePoolSpecLive(name, { ...spec, num_workers: parseInt((e.target as HTMLInputElement).value) || 1 })}
									onblur={() => pushHistory()}
								/>
							</div>
						{:else if spec.type === 'multiprocess'}
							<div class="field-row">
								<div class="field">
									<label>Processes</label>
									<input
										type="number"
										min="1"
										value={spec.num_processes}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, num_processes: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()}
									/>
								</div>
								<div class="field">
									<label>Threads/Process</label>
									<input
										type="number"
										min="1"
										value={spec.threads_per_process}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, threads_per_process: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()}
									/>
								</div>
							</div>
						{:else if spec.type === 'remote'}
							<div class="field">
								<label>URL</label>
								<input
									type="text"
									value={spec.url}
									placeholder="ws://localhost:8765"
									oninput={(e) => updatePoolSpecLive(name, { ...spec, url: (e.target as HTMLInputElement).value })}
									onblur={() => pushHistory()}
								/>
							</div>
							<div class="field">
								<label>Worker Name</label>
								<input
									type="text"
									value={spec.worker_name}
									placeholder="worker"
									oninput={(e) => updatePoolSpecLive(name, { ...spec, worker_name: (e.target as HTMLInputElement).value })}
									onblur={() => pushHistory()}
								/>
							</div>
							<div class="field-row">
								<div class="field">
									<label>Processes</label>
									<input
										type="number"
										min="1"
										value={spec.num_processes}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, num_processes: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()}
									/>
								</div>
								<div class="field">
									<label>Threads/Process</label>
									<input
										type="number"
										min="1"
										value={spec.threads_per_process}
										oninput={(e) => updatePoolSpecLive(name, { ...spec, threads_per_process: parseInt((e.target as HTMLInputElement).value) || 1 })}
										onblur={() => pushHistory()}
									/>
								</div>
							</div>
						{/if}
						<!-- main type has no additional fields -->
					</div>
				</div>
			{/each}
		</div>

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
</style>
