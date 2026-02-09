<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';
	import { configSchema } from '$lib/stores/schemaStore';
	import AutoConfigFields from './AutoConfigFields.svelte';

	type BackendType = 'local' | 's3' | 'gcs' | 'ssh' | 'rclone';

	interface Props {
		storage: Record<string, unknown> | null | undefined;
		onUpdate: (storage: Record<string, unknown> | null) => void;
	}

	let { storage, onUpdate }: Props = $props();

	let enabled = $derived(storage != null);

	// --- Schema lookups ---
	let storageSchema = $derived($configSchema?.models['StorageConfig'] ?? null);
	let cacheSchema = $derived($configSchema?.models['CacheConfig'] ?? null);
	let backendSchemas = $derived({
		local: $configSchema?.models['LocalBackendConfig'] ?? null,
		s3: $configSchema?.models['S3BackendConfig'] ?? null,
		gcs: $configSchema?.models['GCSBackendConfig'] ?? null,
		ssh: $configSchema?.models['SSHBackendConfig'] ?? null,
		rclone: $configSchema?.models['RcloneBackendConfig'] ?? null,
	});

	// --- Storage fields ---
	function getStorage(): Record<string, unknown> {
		return (storage ?? {}) as Record<string, unknown>;
	}

	function toggleEnabled() {
		if (enabled) {
			onUpdate(null);
		} else {
			onUpdate({});
		}
		pushHistory();
	}

	// --- Cache sub-section ---
	let cacheData = $derived(getStorage().cache as Record<string, unknown> | null | undefined);
	let cacheEnabled = $derived(cacheData != null);

	function toggleCache() {
		const s = getStorage();
		if (cacheEnabled) {
			onUpdate({ ...s, cache: undefined });
		} else {
			onUpdate({ ...s, cache: { enabled: false } });
		}
		pushHistory();
	}

	function updateCache(updates: Record<string, unknown>) {
		const s = getStorage();
		onUpdate({ ...s, cache: updates });
	}

	// --- Backends sub-section ---
	let backends = $derived((getStorage().backends ?? {}) as Record<string, Record<string, unknown>>);
	let backendEntries = $derived(Object.entries(backends));

	function addBackend() {
		const s = getStorage();
		const current = { ...backends };
		const name = generateUniqueName(current, 'backend');
		current[name] = { type: 'local', base_path: '' };
		onUpdate({ ...s, backends: current });
		pushHistory();
	}

	function removeBackend(name: string) {
		const s = getStorage();
		const current = { ...backends };
		delete current[name];
		onUpdate({ ...s, backends: Object.keys(current).length > 0 ? current : undefined });
		pushHistory();
	}

	function updateBackendName(oldName: string, newName: string) {
		if (oldName === newName || !newName.trim()) return;
		if (newName in backends) return;
		const s = getStorage();
		const updated: Record<string, Record<string, unknown>> = {};
		for (const [name, config] of Object.entries(backends)) {
			updated[name === oldName ? newName : name] = config;
		}
		onUpdate({ ...s, backends: updated });
		pushHistory();
	}

	function changeBackendType(name: string, newType: BackendType) {
		const s = getStorage();
		const defaults: Record<BackendType, Record<string, unknown>> = {
			local: { type: 'local', base_path: '' },
			s3: { type: 's3', bucket: '' },
			gcs: { type: 'gcs', bucket: '' },
			ssh: { type: 'ssh', host: '', base_path: '' },
			rclone: { type: 'rclone', remote: '' },
		};
		onUpdate({ ...s, backends: { ...backends, [name]: defaults[newType] } });
		pushHistory();
	}

	function updateBackendConfig(name: string, updates: Record<string, unknown>) {
		const s = getStorage();
		onUpdate({ ...s, backends: { ...backends, [name]: updates } });
	}

	function getBackendModelName(type: string): string {
		const map: Record<string, string> = {
			local: 'LocalBackendConfig',
			s3: 'S3BackendConfig',
			gcs: 'GCSBackendConfig',
			ssh: 'SSHBackendConfig',
			rclone: 'RcloneBackendConfig',
		};
		return map[type] ?? 'LocalBackendConfig';
	}

	function generateUniqueName(existing: Record<string, unknown>, base: string): string {
		if (!(base in existing)) return base;
		let i = 1;
		while (`${base}_${i}` in existing) i++;
		return `${base}_${i}`;
	}

	// --- Custom fields: include_nodes / exclude_nodes (comma-separated) ---
	function getListValue(key: string): string {
		const val = cacheData?.[key];
		if (Array.isArray(val)) return val.join(', ');
		return '';
	}

	function setListValue(key: string, value: string) {
		if (!cacheData) return;
		const items = value.split(',').map(s => s.trim()).filter(Boolean);
		updateCache({ ...cacheData, [key]: items.length > 0 ? items : undefined });
	}

	// --- Custom field: pickling_args (JSON) ---
	function getDictValue(data: Record<string, unknown> | null | undefined, key: string): string {
		const val = data?.[key];
		if (val && typeof val === 'object' && Object.keys(val as object).length > 0) {
			return JSON.stringify(val);
		}
		return '';
	}

	function setDictValue(
		data: Record<string, unknown> | null | undefined,
		key: string,
		value: string,
		updateFn: (updates: Record<string, unknown>) => void,
	) {
		if (!data) return;
		if (!value.trim()) {
			updateFn({ ...data, [key]: undefined });
			return;
		}
		try {
			const parsed = JSON.parse(value);
			updateFn({ ...data, [key]: parsed });
		} catch {
			// Don't update on invalid JSON
		}
	}

	const backendTypeLabels: Record<BackendType, string> = {
		local: 'Local',
		s3: 'S3',
		gcs: 'GCS',
		ssh: 'SSH',
		rclone: 'Rclone',
	};
</script>

<div class="storage-section">
	<label class="defaults-toggle">
		<input type="checkbox" checked={enabled} onchange={toggleEnabled} />
		<span>Enable storage</span>
	</label>

	{#if enabled}
		<!-- Auto-rendered scalar fields from StorageConfig (file_storage_metadata_path) -->
		{#if storageSchema}
			<AutoConfigFields
				modelName="StorageConfig"
				schema={storageSchema}
				values={getStorage()}
				onUpdate={(updates) => { onUpdate(updates); pushHistory(); }}
				onUpdateLive={(updates) => onUpdate(updates)}
			/>
		{/if}

		<!-- Cache sub-section -->
		<div class="subsection">
			<div class="subsection-header">
				<label class="defaults-toggle">
					<input type="checkbox" checked={cacheEnabled} onchange={toggleCache} />
					<span>Cache</span>
				</label>
			</div>

			{#if cacheEnabled && cacheData}
				{#if cacheSchema}
					<AutoConfigFields
						modelName="CacheConfig"
						schema={cacheSchema}
						values={cacheData}
						onUpdate={(updates) => { updateCache(updates); pushHistory(); }}
						onUpdateLive={(updates) => updateCache(updates)}
					/>
				{/if}

				<!-- Custom: include_nodes -->
				<div class="field">
					<label>Include Nodes</label>
					<input
						type="text"
						value={getListValue('include_nodes')}
						placeholder="node_a, node_b (comma-separated)"
						oninput={(e) => setListValue('include_nodes', (e.target as HTMLInputElement).value)}
						onblur={() => pushHistory()}
					/>
				</div>

				<!-- Custom: exclude_nodes -->
				<div class="field">
					<label>Exclude Nodes</label>
					<input
						type="text"
						value={getListValue('exclude_nodes')}
						placeholder="node_x, node_y (comma-separated)"
						oninput={(e) => setListValue('exclude_nodes', (e.target as HTMLInputElement).value)}
						onblur={() => pushHistory()}
					/>
				</div>

				<!-- Custom: pickling_args -->
				<div class="field">
					<label>Pickling Args</label>
					<input
						type="text"
						value={getDictValue(cacheData, 'pickling_args')}
						placeholder={'{"protocol": 5} (JSON)'}
						oninput={(e) => setDictValue(cacheData, 'pickling_args', (e.target as HTMLInputElement).value, updateCache)}
						onblur={() => pushHistory()}
						class="mono"
					/>
				</div>
			{/if}
		</div>

		<!-- Backends sub-section -->
		<div class="subsection">
			<div class="subsection-header">Backends</div>

			<div class="backends-list">
				{#each backendEntries as [name, config]}
					{@const backendType = (config.type as BackendType) ?? 'local'}
					{@const modelName = getBackendModelName(backendType)}
					{@const schema = backendSchemas[backendType]}
					<div class="backend-editor">
						<div class="backend-header">
							<input
								type="text"
								class="backend-name-input"
								value={name}
								onblur={(e) => updateBackendName(name, (e.target as HTMLInputElement).value)}
								onkeydown={(e) => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur(); }}
							/>
							<button
								class="remove-btn"
								onclick={() => removeBackend(name)}
								title="Remove backend"
							>
								&times;
							</button>
						</div>
						<div class="backend-body">
							<div class="field">
								<label>Type</label>
								<select
									value={backendType}
									onchange={(e) => changeBackendType(name, (e.target as HTMLSelectElement).value as BackendType)}
								>
									{#each Object.entries(backendTypeLabels) as [value, label]}
										<option {value}>{label}</option>
									{/each}
								</select>
							</div>

							{#if schema}
								<AutoConfigFields
									{modelName}
									{schema}
									values={config}
									onUpdate={(updates) => { updateBackendConfig(name, updates); pushHistory(); }}
									onUpdateLive={(updates) => updateBackendConfig(name, updates)}
								/>
							{/if}
						</div>
					</div>
				{/each}
			</div>

			<button class="add-btn" onclick={addBackend}>+ Add Backend</button>
		</div>
	{/if}
</div>

<style>
	.storage-section {
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

	.subsection {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 10px;
	}

	.subsection-header {
		font-size: 11px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
		margin-bottom: 10px;
	}

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

	.field input.mono {
		font-size: 11px;
	}

	.backends-list {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.backend-editor {
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		overflow: hidden;
	}

	.backend-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 6px 8px;
		background: var(--bg-primary, #1a1a1a);
		gap: 8px;
	}

	.backend-name-input {
		flex: 1;
		font-size: 12px;
		font-weight: 500;
		padding: 4px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid transparent;
		border-radius: 3px;
		color: var(--text-primary, #fff);
	}

	.backend-name-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.backend-body {
		padding: 8px;
		display: flex;
		flex-direction: column;
		gap: 8px;
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

	.add-btn {
		width: 100%;
		padding: 6px;
		margin-top: 8px;
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
