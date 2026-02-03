<script lang="ts">
	import {
		nodes,
		addNode,
		createRegularNode,
		createFactoryNode,
		undo,
		redo,
		history,
		currentFilePath,
		isDirty
	} from '$lib/stores/flowStore';

	// Add node at center of viewport
	// TODO: Get actual viewport center from SvelteFlow
	function handleAddNode() {
		const newNode = createRegularNode({ x: 200, y: 200 });
		addNode(newNode);
	}

	function handleAddFactoryNode() {
		const factory = prompt('Enter factory import path:', 'netrun.node_factories.function');
		if (factory) {
			const newNode = createFactoryNode({ x: 200, y: 200 }, factory);
			addNode(newNode);
		}
	}

	function handleSave() {
		// TODO: Implement save via backend API
		alert('Save not yet implemented - needs backend API');
	}

	function handleOpen() {
		// TODO: Implement open via backend API
		alert('Open not yet implemented - needs backend API');
	}

	function handleUndo() {
		undo();
	}

	function handleRedo() {
		redo();
	}

	// Keyboard shortcuts
	function handleKeydown(event: KeyboardEvent) {
		// Cmd/Ctrl + Z for undo
		if ((event.metaKey || event.ctrlKey) && event.key === 'z' && !event.shiftKey) {
			event.preventDefault();
			handleUndo();
		}
		// Cmd/Ctrl + Shift + Z for redo
		if ((event.metaKey || event.ctrlKey) && event.key === 'z' && event.shiftKey) {
			event.preventDefault();
			handleRedo();
		}
		// Cmd/Ctrl + S for save
		if ((event.metaKey || event.ctrlKey) && event.key === 's') {
			event.preventDefault();
			handleSave();
		}
		// Cmd/Ctrl + O for open
		if ((event.metaKey || event.ctrlKey) && event.key === 'o') {
			event.preventDefault();
			handleOpen();
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<header class="toolbar">
	<div class="toolbar-section left">
		<button onclick={handleOpen} title="Open file (Cmd+O)">
			<span class="icon">📂</span>
			<span class="label">Open</span>
		</button>
		<button onclick={handleSave} title="Save file (Cmd+S)" disabled={!$isDirty}>
			<span class="icon">💾</span>
			<span class="label">Save</span>
		</button>
		<div class="separator"></div>
		<button onclick={handleUndo} title="Undo (Cmd+Z)" disabled={$history.past.length === 0}>
			<span class="icon">↩</span>
			<span class="label">Undo</span>
		</button>
		<button onclick={handleRedo} title="Redo (Cmd+Shift+Z)" disabled={$history.future.length === 0}>
			<span class="icon">↪</span>
			<span class="label">Redo</span>
		</button>
	</div>

	<div class="toolbar-section center">
		{#if $currentFilePath}
			<span class="file-name">
				{$currentFilePath.split('/').pop()}
				{#if $isDirty}<span class="dirty-indicator">*</span>{/if}
			</span>
		{:else}
			<span class="file-name untitled">Untitled</span>
		{/if}
	</div>

	<div class="toolbar-section right">
		<button onclick={handleAddNode} title="Add regular node">
			<span class="icon">+</span>
			<span class="label">Add Node</span>
		</button>
		<button onclick={handleAddFactoryNode} title="Add factory node" class="factory">
			<span class="icon">⚙</span>
			<span class="label">Add Factory</span>
		</button>
	</div>
</header>

<style>
	.toolbar {
		height: var(--toolbar-height, 48px);
		background: var(--bg-secondary, #242424);
		border-bottom: 1px solid var(--border-color, #404040);
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 0 12px;
		gap: 16px;
	}

	.toolbar-section {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.toolbar-section.center {
		flex: 1;
		justify-content: center;
	}

	.separator {
		width: 1px;
		height: 24px;
		background: var(--border-color, #404040);
		margin: 0 8px;
	}

	button {
		display: flex;
		align-items: center;
		gap: 6px;
		padding: 6px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid transparent;
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
		transition: all 0.15s ease;
	}

	button:hover:not(:disabled) {
		background: var(--border-color, #404040);
		border-color: var(--border-color, #404040);
	}

	button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	button.factory {
		background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
	}

	button.factory:hover:not(:disabled) {
		background: linear-gradient(135deg, #4338ca 0%, #6d28d9 100%);
	}

	.icon {
		font-size: 14px;
	}

	.label {
		font-weight: 500;
	}

	.file-name {
		font-size: 13px;
		color: var(--text-primary, #fff);
		font-weight: 500;
	}

	.file-name.untitled {
		color: var(--text-secondary, #a0a0a0);
		font-style: italic;
	}

	.dirty-indicator {
		color: var(--accent-color, #3b82f6);
		margin-left: 2px;
	}
</style>
