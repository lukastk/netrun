<script lang="ts">
	import {
		addNode,
		createRegularNode,
		createFactoryNode,
		undo,
		redo,
		history,
		currentFilePath,
		isDirty,
		isInlineSubgraph,
		loadFromFile,
		saveToFile,
		clearFlow,
		updateFactoryNodePreview,
		selectedNodeIds,
		validateAllNodes,
		createSubgraphFromSelection,
		nodes,
		edges,
	} from '$lib/stores/flowStore';
	import { tick } from 'svelte';
	import { openCommandPalette } from '$lib/stores/commandStore';
	import { resolveFilePath } from '$lib/stores/fileExplorerStore';
	import { showPrompt, showAlert, showConfirm } from '$lib/stores/modalStore';
	import { showFactorySelector } from '$lib/stores/factorySelectorStore';

	import type { ValidationResult } from '$lib/stores/flowStore';

	// Validation state
	let lastValidationResult = $state<ValidationResult | null>(null);
	let isValidating = $state(false);

	// Reset validation state when nodes or edges change (but not during validation itself)
	$effect(() => {
		$nodes;
		$edges;
		if (!isValidating) {
			lastValidationResult = null;
		}
	});

	// State for loading/saving
	let isLoading = $state(false);
	let errorMessage = $state<string | null>(null);

	// Add node at center of viewport
	function handleAddNode() {
		const newNode = createRegularNode({ x: 200, y: 200 });
		addNode(newNode);
	}

	async function handleAddFactoryNode() {
		const factoryPath = await showFactorySelector();
		if (factoryPath) {
			const newNode = createFactoryNode({ x: 200, y: 200 }, factoryPath);
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

	async function handleCreateSubgraph() {
		if ($selectedNodeIds.size < 2) {
			await showAlert({
				title: 'Cannot Create Subgraph',
				message: 'Please select at least 2 nodes to create a subgraph.',
			});
			return;
		}

		const name = await showPrompt({
			title: 'Create Subgraph',
			message: 'Enter a name for the subgraph',
			placeholder: 'MySubgraph',
			defaultValue: 'MySubgraph',
			confirmText: 'Create',
		});
		if (!name) return;

		try {
			const success = await createSubgraphFromSelection(name);
			if (!success) {
				await showAlert({
					title: 'Error',
					message: 'Failed to create subgraph.',
				});
			}
		} catch (e) {
			await showAlert({
				title: 'Error',
				message: `Error creating subgraph: ${(e as Error).message}`,
			});
		}
	}

	async function handleSave() {
		errorMessage = null;

		// Inline subgraphs save to their parent tab
		if ($isInlineSubgraph) {
			isLoading = true;
			try {
				await saveToFile();
				await showAlert({
					title: 'Saved',
					message: 'Subgraph changes saved to parent. Save the parent file to persist.',
				});
			} catch (e) {
				errorMessage = (e as Error).message;
				await showAlert({
					title: 'Error',
					message: `Save failed: ${errorMessage}`,
				});
			} finally {
				isLoading = false;
			}
			return;
		}

		// If no current file, prompt for path
		let path = $currentFilePath;
		if (!path) {
			path = await showPrompt({
				title: 'Save File',
				message: 'Enter file path',
				placeholder: '/path/to/file.netrun.json',
				inputType: 'path',
				confirmText: 'Save',
			});
			if (!path) return;
		}

		isLoading = true;
		try {
			await saveToFile(path);
		} catch (e) {
			errorMessage = (e as Error).message;
			await showAlert({
				title: 'Error',
				message: `Save failed: ${errorMessage}`,
			});
		} finally {
			isLoading = false;
		}
	}

	async function handleOpen() {
		errorMessage = null;

		const path = await showPrompt({
			title: 'Open File',
			message: 'Enter file path to open',
			placeholder: '/path/to/file.netrun.json',
			inputType: 'path',
			confirmText: 'Open',
		});
		if (!path) return;

		isLoading = true;
		try {
			await loadFromFile(path);
		} catch (e) {
			errorMessage = (e as Error).message;
			await showAlert({
				title: 'Error',
				message: `Open failed: ${errorMessage}`,
			});
		} finally {
			isLoading = false;
		}
	}

	async function handleNew() {
		if ($isDirty) {
			const confirmed = await showConfirm({
				title: 'Unsaved Changes',
				message: 'You have unsaved changes. Create new file anyway?',
				confirmText: 'Create New',
				cancelText: 'Cancel',
			});
			if (!confirmed) return;
		}
		const inputPath = await showPrompt({
			title: 'Create New File',
			message: 'Enter filename (relative to current folder) or absolute path',
			placeholder: 'my_flow.netrun.json',
			defaultValue: 'my_flow.netrun.json',
			inputType: 'path',
			confirmText: 'Create',
		});
		if (!inputPath) return;

		const fullPath = resolveFilePath(inputPath);
		// Determine format from extension
		const format = fullPath.endsWith('.toml') ? 'toml' : 'json';
		const fileName = fullPath.split('/').pop() || 'Untitled';

		clearFlow(format, fileName);

		try {
			await saveToFile(fullPath);
		} catch (e) {
			await showAlert({
				title: 'Error',
				message: `Failed to create file: ${(e as Error).message}`,
			});
		}
	}

	function handleUndo() {
		undo();
	}

	function handleRedo() {
		redo();
	}
</script>

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
		<button onclick={openCommandPalette} title="Command Palette (Cmd+Shift+P)" class="command-palette">
			<span class="icon">⌘</span>
		</button>
		<div class="separator"></div>
		<button
			onclick={async () => {
				isValidating = true;
				lastValidationResult = await validateAllNodes();
				isValidating = false;
				if (!lastValidationResult.valid && lastValidationResult.configErrors.length > 0) {
					await showAlert({
						title: 'Config Errors',
						message: lastValidationResult.configErrors.join('\n'),
					});
				}
			}}
			title="Validate all nodes"
			disabled={isValidating}
			class:has-errors={lastValidationResult && !lastValidationResult.valid}
			class:has-success={lastValidationResult && lastValidationResult.valid}
		>
			<span class="icon">✓</span>
			{#if isValidating}
				<span class="label">Validating...</span>
			{:else if lastValidationResult && lastValidationResult.valid}
				<span class="label">Valid</span>
			{:else}
				<span class="label">Validate</span>
			{/if}
			{#if lastValidationResult && !lastValidationResult.valid}
				<span class="error-badge">{lastValidationResult.errorCount + lastValidationResult.configErrors.length}</span>
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
		<button
			onclick={handleCreateSubgraph}
			title="Create subgraph from selection (Cmd+G)"
			class="subgraph"
			disabled={$selectedNodeIds.size < 2}
		>
			<span class="icon">SG</span>
			<span class="label">Subgraph</span>
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

	button.subgraph {
		background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
	}

	button.subgraph:hover:not(:disabled) {
		background: linear-gradient(135deg, #16a34a 0%, #15803d 100%);
	}

	button.command-palette {
		padding: 6px 10px;
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

	button.has-success {
		background: var(--success-color, #22c55e);
		border-color: var(--success-color, #22c55e);
	}

	button.has-success:hover:not(:disabled) {
		background: #16a34a;
		border-color: #16a34a;
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
