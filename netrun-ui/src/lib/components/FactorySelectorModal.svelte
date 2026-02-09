<script lang="ts">
	import { api, type BuiltinFactory } from '$lib/api';

	interface Props {
		factories: string[];  // User-defined factories from extraData
		onSelect: (factoryPath: string) => void;
		onCancel: () => void;
	}

	let { factories, onSelect, onCancel }: Props = $props();

	// Built-in factories loaded from API
	let builtinFactories = $state<BuiltinFactory[]>([]);
	let isLoading = $state(true);
	let loadError = $state<string | null>(null);
	let importErrors = $state<string[]>([]);

	// Combined factories for display
	interface DisplayFactory {
		path: string;
		name: string;
		docstring: string | null;
		isBuiltin: boolean;
	}

	const allFactories = $derived.by(() => {
		const result: DisplayFactory[] = [];

		// Add built-in factories first
		for (const f of builtinFactories) {
			result.push({
				path: f.import_path,
				name: f.name,
				docstring: f.docstring,
				isBuiltin: true,
			});
		}

		// Add user-defined factories (skip duplicates)
		const builtinPaths = new Set(builtinFactories.map(f => f.import_path));
		for (const path of factories) {
			if (!builtinPaths.has(path)) {
				result.push({
					path,
					name: path.split('.').pop() || path,
					docstring: null,
					isBuiltin: false,
				});
			}
		}

		return result;
	});

	// Determine if we have any factories to show
	const hasFactories = $derived(allFactories.length > 0);

	let mode = $state<'select' | 'custom'>('select');
	let selectedFactory = $state('');
	let customPath = $state('');
	let inputElement: HTMLInputElement | undefined = $state();

	// Load built-in factories on mount
	$effect(() => {
		loadBuiltinFactories();
	});

	// Set default selection when factories load
	$effect(() => {
		const factories = allFactories;
		if (factories.length > 0 && !selectedFactory) {
			selectedFactory = factories[0].path;
		}
	});

	// Switch to custom mode if no factories available
	$effect(() => {
		if (!isLoading && !hasFactories) {
			mode = 'custom';
		}
	});

	// Focus input when switching to custom mode
	$effect(() => {
		if (mode === 'custom' && inputElement) {
			inputElement.focus();
		}
	});

	async function loadBuiltinFactories() {
		isLoading = true;
		loadError = null;
		importErrors = [];
		try {
			const response = await api.listBuiltinFactories();
			builtinFactories = response.factories;
			importErrors = response.errors || [];
			if (importErrors.length > 0) {
				console.warn('Some factories failed to import:', importErrors);
			}
		} catch (e) {
			loadError = (e as Error).message;
			console.warn('Failed to load built-in factories:', e);
		} finally {
			isLoading = false;
		}
	}

	function handleSubmit() {
		const path = mode === 'select' ? selectedFactory : customPath.trim();
		if (path) {
			onSelect(path);
		}
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.preventDefault();
			onCancel();
		} else if (event.key === 'Enter' && mode === 'custom') {
			event.preventDefault();
			handleSubmit();
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="modal-backdrop" onclick={onCancel} onkeydown={() => {}} role="presentation">
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="modal" onclick={(e) => e.stopPropagation()}>
		<div class="modal-header">
			<h2>Add Factory Node</h2>
			<button class="close-btn" onclick={onCancel}>×</button>
		</div>

		<div class="modal-body">
			{#if !isLoading && hasFactories}
				<div class="mode-tabs">
					<button
						class="mode-tab"
						class:active={mode === 'select'}
						onclick={() => mode = 'select'}
					>
						Choose Factory
					</button>
					<button
						class="mode-tab"
						class:active={mode === 'custom'}
						onclick={() => mode = 'custom'}
					>
						Custom Path
					</button>
				</div>
			{/if}

			{#if isLoading}
				<div class="loading">Loading factories...</div>
			{:else if importErrors.length > 0 && !hasFactories}
				<div class="error-box">
					<p class="error-title">Failed to load built-in factories:</p>
					{#each importErrors as error}
						<p class="error-detail">{error}</p>
					{/each}
					<p class="error-hint">You may need to reinstall netrun in the backend environment.</p>
				</div>
			{:else if mode === 'select' && hasFactories}
				<div class="factory-list">
					{#each allFactories as factory}
						<button
							class="factory-option"
							class:selected={selectedFactory === factory.path}
							onclick={() => selectedFactory = factory.path}
							ondblclick={handleSubmit}
						>
							<div class="factory-header">
								<span class="factory-name">{factory.name}</span>
								{#if factory.isBuiltin}
									<span class="builtin-badge">built-in</span>
								{:else}
									<span class="custom-badge">project</span>
								{/if}
							</div>
							<span class="factory-path">{factory.path}</span>
							{#if factory.docstring}
								<span class="factory-doc">{factory.docstring.split('\n')[0]}</span>
							{/if}
						</button>
					{/each}
				</div>
			{:else}
				<div class="custom-input">
					<label for="factory-path">Factory Import Path</label>
					<input
						id="factory-path"
						type="text"
						bind:value={customPath}
						bind:this={inputElement}
						placeholder="mymodule.factories.create_node"
					/>
					<p class="hint">
						Enter the full Python import path to your factory function
					</p>
				</div>
			{/if}
		</div>

		<div class="modal-footer">
			<button class="btn btn-secondary" onclick={onCancel}>
				Cancel
			</button>
			<button
				class="btn btn-primary"
				onclick={handleSubmit}
				disabled={mode === 'select' ? !selectedFactory : !customPath.trim()}
			>
				Add Node
			</button>
		</div>
	</div>
</div>

<style>
	.modal-backdrop {
		position: fixed;
		inset: 0;
		background: rgba(0, 0, 0, 0.6);
		backdrop-filter: blur(4px);
		display: flex;
		align-items: center;
		justify-content: center;
		z-index: 2000;
	}

	.modal {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 12px;
		width: 520px;
		max-width: 90vw;
		max-height: 80vh;
		display: flex;
		flex-direction: column;
		box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
	}

	.modal-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 16px 20px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.modal-header h2 {
		margin: 0;
		font-size: 16px;
		font-weight: 600;
		color: var(--text-primary, #fff);
	}

	.close-btn {
		background: none;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		font-size: 20px;
		cursor: pointer;
		padding: 4px 8px;
		line-height: 1;
		border-radius: 4px;
	}

	.close-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.modal-body {
		padding: 20px;
		overflow-y: auto;
		flex: 1;
	}

	.loading {
		text-align: center;
		color: var(--text-secondary, #a0a0a0);
		padding: 20px;
	}

	.error-box {
		background: rgba(239, 68, 68, 0.1);
		border: 1px solid rgba(239, 68, 68, 0.3);
		border-radius: 6px;
		padding: 12px;
		margin-bottom: 16px;
	}

	.error-title {
		color: #ef4444;
		font-weight: 500;
		font-size: 13px;
		margin: 0 0 8px 0;
	}

	.error-detail {
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
		margin: 4px 0;
		word-break: break-word;
	}

	.error-hint {
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
		margin: 8px 0 0 0;
		font-style: italic;
	}

	.mode-tabs {
		display: flex;
		gap: 4px;
		margin-bottom: 16px;
		background: var(--bg-primary, #1a1a1a);
		padding: 4px;
		border-radius: 8px;
	}

	.mode-tab {
		flex: 1;
		padding: 8px 12px;
		background: transparent;
		border: none;
		border-radius: 6px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 13px;
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.mode-tab:hover {
		color: var(--text-primary, #fff);
	}

	.mode-tab.active {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.factory-list {
		display: flex;
		flex-direction: column;
		gap: 4px;
		max-height: 350px;
		overflow-y: auto;
	}

	.factory-option {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 3px;
		padding: 10px 12px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		cursor: pointer;
		transition: all 0.15s ease;
		text-align: left;
		width: 100%;
	}

	.factory-option:hover {
		border-color: var(--accent-color, #3b82f6);
	}

	.factory-option.selected {
		border-color: var(--accent-color, #3b82f6);
		background: rgba(59, 130, 246, 0.1);
	}

	.factory-header {
		display: flex;
		align-items: center;
		gap: 8px;
		width: 100%;
	}

	.factory-name {
		font-size: 14px;
		font-weight: 500;
		color: var(--text-primary, #fff);
	}

	.builtin-badge {
		font-size: 10px;
		padding: 2px 6px;
		background: rgba(124, 58, 237, 0.2);
		color: #a78bfa;
		border-radius: 4px;
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.custom-badge {
		font-size: 10px;
		padding: 2px 6px;
		background: rgba(59, 130, 246, 0.2);
		color: #93c5fd;
		border-radius: 4px;
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.factory-path {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}

	.factory-doc {
		font-size: 11px;
		color: var(--text-tertiary, #707070);
		margin-top: 2px;
	}

	.custom-input {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.custom-input label {
		font-size: 12px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
	}

	.custom-input input {
		width: 100%;
		padding: 10px 12px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		color: var(--text-primary, #fff);
		font-size: 14px;
		box-sizing: border-box;
	}

	.custom-input input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		margin: 0;
	}

	.modal-footer {
		display: flex;
		justify-content: flex-end;
		gap: 8px;
		padding: 16px 20px;
		border-top: 1px solid var(--border-color, #404040);
	}

	.btn {
		padding: 8px 16px;
		border-radius: 6px;
		font-size: 13px;
		font-weight: 500;
		cursor: pointer;
		border: none;
		transition: all 0.15s ease;
	}

	.btn:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.btn-primary {
		background: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.btn-primary:hover:not(:disabled) {
		background: var(--accent-hover, #2563eb);
	}

	.btn-secondary {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
		border: 1px solid var(--border-color, #404040);
	}

	.btn-secondary:hover {
		background: var(--border-color, #404040);
	}
</style>
