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
		isDirty,
		loadFromFile,
		saveToFile,
		clearFlow,
		updateFactoryNodePreview,
		createTab,
		closeActiveTab,
		switchToTabIndex,
		switchToNextTab,
		switchToPreviousTab,
		copySelectedNodes,
		pasteNodes,
		cutSelectedNodes,
		selectedNodeIds,
		validateAllNodes,
	} from '$lib/stores/flowStore';
	import { api } from '$lib/api';

	// Validation state
	let lastValidationResult = $state<{ valid: boolean; errorCount: number } | null>(null);

	// State for loading/saving
	let isLoading = $state(false);
	let errorMessage = $state<string | null>(null);

	// Add node at center of viewport
	// TODO: Get actual viewport center from SvelteFlow
	function handleAddNode() {
		const newNode = createRegularNode({ x: 200, y: 200 });
		addNode(newNode);
	}

	async function handleAddFactoryNode() {
		const factory = prompt('Enter factory import path:', 'netrun.node_factories.function');
		if (factory) {
			const newNode = createFactoryNode({ x: 200, y: 200 }, factory);
			addNode(newNode);

			// Try to get preview from backend
			try {
				await updateFactoryNodePreview(newNode.id);
			} catch (e) {
				// Factory preview failed, but node is still added
				console.warn('Could not get factory preview:', e);
			}
		}
	}

	async function handleSave() {
		errorMessage = null;

		// If no current file, prompt for path
		let path = $currentFilePath;
		if (!path) {
			path = prompt('Enter file path (e.g., /path/to/file.netrun.json):');
			if (!path) return;
		}

		isLoading = true;
		try {
			await saveToFile(path);
		} catch (e) {
			errorMessage = (e as Error).message;
			alert(`Save failed: ${errorMessage}`);
		} finally {
			isLoading = false;
		}
	}

	async function handleSaveAs() {
		errorMessage = null;

		const path = prompt('Enter file path (e.g., /path/to/file.netrun.json):');
		if (!path) return;

		isLoading = true;
		try {
			await saveToFile(path);
		} catch (e) {
			errorMessage = (e as Error).message;
			alert(`Save failed: ${errorMessage}`);
		} finally {
			isLoading = false;
		}
	}

	async function handleOpen() {
		errorMessage = null;

		const path = prompt('Enter file path to open:');
		if (!path) return;

		isLoading = true;
		try {
			await loadFromFile(path);
		} catch (e) {
			errorMessage = (e as Error).message;
			alert(`Open failed: ${errorMessage}`);
		} finally {
			isLoading = false;
		}
	}

	function handleNew() {
		// If there are unsaved changes, confirm
		if ($isDirty) {
			if (!confirm('You have unsaved changes. Create new file anyway?')) {
				return;
			}
		}
		clearFlow();
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
		// Cmd/Ctrl + N for new file (clears current tab)
		if ((event.metaKey || event.ctrlKey) && event.key === 'n' && !event.shiftKey) {
			event.preventDefault();
			handleNew();
		}
		// Cmd/Ctrl + T for new tab
		if ((event.metaKey || event.ctrlKey) && event.key === 't') {
			event.preventDefault();
			createTab();
		}
		// Cmd/Ctrl + W for close tab
		if ((event.metaKey || event.ctrlKey) && event.key === 'w') {
			event.preventDefault();
			closeActiveTab();
		}
		// Cmd/Ctrl + 1-9 for switching to tabs by index
		if ((event.metaKey || event.ctrlKey) && event.key >= '1' && event.key <= '9') {
			event.preventDefault();
			const tabIndex = parseInt(event.key) - 1;
			switchToTabIndex(tabIndex);
		}
		// Ctrl + Tab for next tab
		if (event.ctrlKey && event.key === 'Tab' && !event.shiftKey) {
			event.preventDefault();
			switchToNextTab();
		}
		// Ctrl + Shift + Tab for previous tab
		if (event.ctrlKey && event.key === 'Tab' && event.shiftKey) {
			event.preventDefault();
			switchToPreviousTab();
		}
		// Cmd/Ctrl + C for copy
		if ((event.metaKey || event.ctrlKey) && event.key === 'c') {
			// Only handle if we have selected nodes and not in an input field
			if ($selectedNodeIds.size > 0 && !isInputFocused()) {
				event.preventDefault();
				copySelectedNodes();
			}
		}
		// Cmd/Ctrl + V for paste
		if ((event.metaKey || event.ctrlKey) && event.key === 'v') {
			if (!isInputFocused()) {
				event.preventDefault();
				pasteNodes();
			}
		}
		// Cmd/Ctrl + X for cut
		if ((event.metaKey || event.ctrlKey) && event.key === 'x') {
			if ($selectedNodeIds.size > 0 && !isInputFocused()) {
				event.preventDefault();
				cutSelectedNodes();
			}
		}
	}

	// Check if an input element is focused (to avoid interfering with text editing)
	function isInputFocused(): boolean {
		const activeElement = document.activeElement;
		return activeElement instanceof HTMLInputElement ||
			activeElement instanceof HTMLTextAreaElement ||
			activeElement?.getAttribute('contenteditable') === 'true';
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<header class="toolbar">
	<div class="toolbar-section left">
		<button onclick={handleNew} title="New file (Cmd+N)">
			<span class="icon">📄</span>
			<span class="label">New</span>
		</button>
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
		<button
			onclick={() => {
				lastValidationResult = validateAllNodes();
			}}
			title="Validate all nodes"
			class:has-errors={lastValidationResult && !lastValidationResult.valid}
		>
			<span class="icon">✓</span>
			<span class="label">Validate</span>
			{#if lastValidationResult && !lastValidationResult.valid}
				<span class="error-badge">{lastValidationResult.errorCount}</span>
			{/if}
		</button>
		<div class="separator"></div>
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

	button.has-errors {
		background: var(--error-color, #ef4444);
		border-color: var(--error-color, #ef4444);
	}

	button.has-errors:hover:not(:disabled) {
		background: #dc2626;
		border-color: #dc2626;
	}

	.error-badge {
		background: #fff;
		color: var(--error-color, #ef4444);
		font-size: 10px;
		font-weight: 700;
		padding: 1px 5px;
		border-radius: 8px;
		margin-left: 4px;
	}
</style>
