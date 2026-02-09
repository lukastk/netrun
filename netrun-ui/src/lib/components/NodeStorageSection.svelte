<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';
	import { configSchema } from '$lib/stores/schemaStore';
	import AutoConfigFields from './AutoConfigFields.svelte';

	type StorageMode = 'none' | 'cache' | 'file_storage';

	interface Props {
		storage: Record<string, unknown> | null | undefined;
		availableBackends: string[];
		onUpdate: (storage: Record<string, unknown> | null) => void;
	}

	let { storage, availableBackends, onUpdate }: Props = $props();

	// --- Schema lookups ---
	let cacheSchema = $derived($configSchema?.models['NodeCacheConfig'] ?? null);
	let fileStorageSchema = $derived($configSchema?.models['NodeFileStorageConfig'] ?? null);

	// --- Mode derivation ---
	let mode = $derived.by((): StorageMode => {
		if (!storage) return 'none';
		const s = storage as Record<string, unknown>;
		if (s.cache != null) return 'cache';
		if (s.file_storage != null) return 'file_storage';
		return 'none';
	});

	let cacheData = $derived((storage as Record<string, unknown> | null)?.cache as Record<string, unknown> | null ?? null);
	let fileStorageData = $derived((storage as Record<string, unknown> | null)?.file_storage as Record<string, unknown> | null ?? null);

	function setMode(newMode: StorageMode) {
		switch (newMode) {
			case 'none':
				onUpdate(null);
				break;
			case 'cache':
				onUpdate({ cache: {} });
				break;
			case 'file_storage':
				onUpdate({ file_storage: { backend: '' } });
				break;
		}
		pushHistory();
	}

	// --- Cache updates ---
	function updateCache(updates: Record<string, unknown>) {
		onUpdate({ cache: updates });
	}

	// --- File storage updates ---
	function updateFileStorage(updates: Record<string, unknown>) {
		onUpdate({ file_storage: updates });
	}

	// --- Custom field helpers: JSON dict fields ---
	function getDictValue(data: Record<string, unknown> | null, key: string): string {
		const val = data?.[key];
		if (val && typeof val === 'object' && Object.keys(val as object).length > 0) {
			return JSON.stringify(val);
		}
		return '';
	}

	function setDictValue(
		data: Record<string, unknown> | null,
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

	// --- Custom: output_names (JSON) ---
	function getOutputNamesValue(): string {
		const val = fileStorageData?.output_names;
		if (val && typeof val === 'object') {
			return JSON.stringify(val, null, 2);
		}
		return '';
	}

	function setOutputNames(value: string) {
		if (!fileStorageData) return;
		if (!value.trim()) {
			updateFileStorage({ ...fileStorageData, output_names: undefined });
			return;
		}
		try {
			const parsed = JSON.parse(value);
			updateFileStorage({ ...fileStorageData, output_names: parsed });
		} catch {
			// Don't update on invalid JSON
		}
	}
</script>

<div class="node-storage-section">
	<!-- Mode selector -->
	<div class="mode-selector">
		<button
			class="mode-btn"
			class:active={mode === 'none'}
			onclick={() => setMode('none')}
		>None</button>
		<button
			class="mode-btn"
			class:active={mode === 'cache'}
			onclick={() => setMode('cache')}
		>Cache</button>
		<button
			class="mode-btn"
			class:active={mode === 'file_storage'}
			onclick={() => setMode('file_storage')}
		>File Storage</button>
	</div>

	<!-- Cache mode -->
	{#if mode === 'cache' && cacheData != null}
		<div class="mode-content">
			{#if cacheSchema}
				<AutoConfigFields
					modelName="NodeCacheConfig"
					schema={cacheSchema}
					values={cacheData}
					onUpdate={(updates) => { updateCache(updates); pushHistory(); }}
					onUpdateLive={(updates) => updateCache(updates)}
				/>
			{/if}

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
		</div>
	{/if}

	<!-- File storage mode -->
	{#if mode === 'file_storage' && fileStorageData != null}
		<div class="mode-content">
			<!-- Custom: backend (select from net-level backends or type manually) -->
			<div class="field">
				<label>Backend</label>
				{#if availableBackends.length > 0}
					<select
						value={typeof fileStorageData.backend === 'string' ? fileStorageData.backend : ''}
						onchange={(e) => {
							updateFileStorage({ ...fileStorageData, backend: (e.target as HTMLSelectElement).value });
							pushHistory();
						}}
					>
						<option value="">Select a backend...</option>
						{#each availableBackends as name}
							<option value={name}>{name}</option>
						{/each}
					</select>
				{:else}
					<input
						type="text"
						value={typeof fileStorageData.backend === 'string' ? fileStorageData.backend : ''}
						placeholder="backend_name"
						oninput={(e) => updateFileStorage({ ...fileStorageData, backend: (e.target as HTMLInputElement).value || '' })}
						onblur={() => pushHistory()}
						class="mono"
					/>
				{/if}
				<span class="field-hint">Name of a backend defined in Storage &gt; Backends</span>
			</div>

			{#if fileStorageSchema}
				<AutoConfigFields
					modelName="NodeFileStorageConfig"
					schema={fileStorageSchema}
					values={fileStorageData}
					onUpdate={(updates) => { updateFileStorage(updates); pushHistory(); }}
					onUpdateLive={(updates) => updateFileStorage(updates)}
				/>
			{/if}

			<!-- Custom: pickling_args -->
			<div class="field">
				<label>Pickling Args</label>
				<input
					type="text"
					value={getDictValue(fileStorageData, 'pickling_args')}
					placeholder={'{"protocol": 5} (JSON)'}
					oninput={(e) => setDictValue(fileStorageData, 'pickling_args', (e.target as HTMLInputElement).value, updateFileStorage)}
					onblur={() => pushHistory()}
					class="mono"
				/>
			</div>

			<!-- Custom: hash_pickling_args -->
			<div class="field">
				<label>Hash Pickling Args</label>
				<input
					type="text"
					value={getDictValue(fileStorageData, 'hash_pickling_args')}
					placeholder={'{"protocol": 5} (JSON)'}
					oninput={(e) => setDictValue(fileStorageData, 'hash_pickling_args', (e.target as HTMLInputElement).value, updateFileStorage)}
					onblur={() => pushHistory()}
					class="mono"
				/>
			</div>

			<!-- Custom: output_names -->
			<div class="field">
				<label>Output Names</label>
				<textarea
					value={getOutputNamesValue()}
					placeholder={'{"node": {"port": "name"}} (JSON)'}
					oninput={(e) => setOutputNames((e.target as HTMLTextAreaElement).value)}
					onblur={() => pushHistory()}
					class="mono"
					rows="3"
				></textarea>
			</div>
		</div>
	{/if}
</div>

<style>
	.node-storage-section {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.mode-selector {
		display: flex;
		gap: 0;
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		overflow: hidden;
	}

	.mode-btn {
		flex: 1;
		padding: 6px 8px;
		font-size: 11px;
		background: var(--bg-primary, #1a1a1a);
		border: none;
		border-right: 1px solid var(--border-color, #404040);
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.mode-btn:last-child {
		border-right: none;
	}

	.mode-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.mode-btn.active {
		background: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.mode-content {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 10px;
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
	.field textarea,
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
	.field textarea:focus,
	.field select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field select {
		cursor: pointer;
	}

	.field input.mono,
	.field textarea.mono {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
	}

	.field textarea {
		resize: vertical;
		font-family: inherit;
	}

	.field-hint {
		display: block;
		font-size: 10px;
		color: var(--text-secondary, #666);
		margin-top: 4px;
	}
</style>
