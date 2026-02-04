<script lang="ts">
	interface Props {
		factories: string[];
		onSelect: (factoryPath: string) => void;
		onCancel: () => void;
	}

	let { factories, onSelect, onCancel }: Props = $props();

	let mode = $state<'select' | 'custom'>(factories.length > 0 ? 'select' : 'custom');
	let selectedFactory = $state(factories[0] || '');
	let customPath = $state('');
	let inputElement: HTMLInputElement | undefined = $state();

	// Focus input when switching to custom mode
	$effect(() => {
		if (mode === 'custom' && inputElement) {
			inputElement.focus();
		}
	});

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

	function getFactoryDisplayName(path: string): string {
		// Extract the last part of the import path for display
		const parts = path.split('.');
		return parts[parts.length - 1] || path;
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
			{#if factories.length > 0}
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

			{#if mode === 'select' && factories.length > 0}
				<div class="factory-list">
					{#each factories as factory}
						<button
							class="factory-option"
							class:selected={selectedFactory === factory}
							onclick={() => selectedFactory = factory}
							ondblclick={handleSubmit}
						>
							<span class="factory-name">{getFactoryDisplayName(factory)}</span>
							<span class="factory-path">{factory}</span>
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
		width: 480px;
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
		max-height: 300px;
		overflow-y: auto;
	}

	.factory-option {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 2px;
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

	.factory-name {
		font-size: 14px;
		font-weight: 500;
		color: var(--text-primary, #fff);
	}

	.factory-path {
		font-size: 11px;
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		color: var(--text-secondary, #a0a0a0);
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
		font-family: 'SF Mono', Monaco, Consolas, monospace;
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
