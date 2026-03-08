<script lang="ts">
	import { onMount } from 'svelte';
	import TabBar from '$lib/components/TabBar.svelte';
	import FileExplorer from '$lib/components/FileExplorer.svelte';
	import EditorShell from '$lib/components/EditorShell.svelte';
	import '$lib/configFieldRegistrations';
	import { api } from '$lib/api';
	import { loadConfigSchema } from '$lib/stores/schemaStore';
	import { loadSignalTypes } from '$lib/stores/signalStore';
	import { loadControlTypes } from '$lib/stores/controlStore';
	import { nodes, currentFilePath, activeTab, activeTabId, recentFiles, loadFromFile, clearFlow, isNewFile, saveToFile, selectNodeByName, hasUnsavedChanges } from '$lib/stores/flowStore';
	import { restoreExpansionState, refreshAllExpandedSubgraphs } from '$lib/stores/subgraphExpandStore';
	import { resolveFilePath } from '$lib/stores/fileExplorerStore';
	import { showPrompt, showAlert } from '$lib/stores/modalStore';
	import { initializeCommands } from '$lib/commands';
	import type { PageData } from './$types';

	// Page data from load function (URL query parameters)
	let { data }: { data: PageData } = $props();

	// Track if we've processed initial files to avoid re-processing on HMR
	let initialFilesProcessed = $state(false);

	// Track tabs we've already restored expansion state for
	let restoredTabs = new Set<string>();

	// Restore subgraph expansion state when switching to a tab we haven't processed yet
	$effect(() => {
		const tab = $activeTab;
		if (tab && tab.nodes.length > 0 && !restoredTabs.has(tab.id)) {
			restoredTabs.add(tab.id);
			// Defer to avoid issues during initialization
			setTimeout(() => restoreExpansionState(), 50);
		}
	});

	// Refresh expanded subgraphs when switching tabs (picks up edits from child tabs)
	let previousEffectTabId: string | null = null;
	$effect(() => {
		const tab = $activeTab;
		if (tab && tab.id !== previousEffectTabId) {
			const prev = previousEffectTabId;
			previousEffectTabId = tab.id;
			if (prev !== null) {
				// Tab switched — refresh expanded subgraphs to pick up changes
				setTimeout(() => refreshAllExpandedSubgraphs(), 50);
			}
		}
	});

	// Initial path for file explorer - fetched from server, with fallback
	let initialPath = $state('~');

	// Warn before closing with unsaved changes
	function handleBeforeUnload(event: BeforeUnloadEvent) {
		if (hasUnsavedChanges()) {
			event.preventDefault();
		}
	}

	// Initialize command system and process URL parameters
	onMount(async () => {
		initializeCommands();
		loadConfigSchema();
		loadSignalTypes();
		loadControlTypes();

		// Fetch working directory from server
		let firstNetrunFile: string | null = null;
		try {
			const config = await api.getServerConfig();
			if (config.working_dir) {
				initialPath = config.working_dir;
			}
			firstNetrunFile = config.first_netrun_file;
		} catch (e) {
			// Fall back to VITE_INITIAL_PATH or home directory
			initialPath = import.meta.env.VITE_INITIAL_PATH || '~';
		}

		// Process initial files from URL query parameters
		if (!initialFilesProcessed && data.initialFiles && data.initialFiles.length > 0) {
			initialFilesProcessed = true;
			processInitialFiles(data.initialFiles, data.initialNode);
		} else if (firstNetrunFile) {
			// Auto-open first netrun file found in working directory
			try {
				await loadFromFile(firstNetrunFile);
			} catch (e) {
				console.warn('Failed to auto-open first netrun file:', e);
			}
		}
	});

	// Process files passed via URL parameters
	async function processInitialFiles(files: string[], nodeToSelect: string | null) {
		const errors: string[] = [];

		for (const filePath of files) {
			try {
				await loadFromFile(filePath);
			} catch (e) {
				errors.push(`${filePath}: ${(e as Error).message}`);
			}
		}

		// If there were errors, show them
		if (errors.length > 0) {
			await showAlert({
				title: 'Error Opening Files',
				message: errors.join('\n'),
			});
		}

		// If a node was specified, try to select it
		if (nodeToSelect && errors.length < files.length) {
			// Give the flow a moment to render
			setTimeout(() => {
				selectNodeByName(nodeToSelect);
			}, 100);
		}
	}

	// File explorer visibility state
	let showFileExplorer = $state(true);

	// File explorer resize state
	const MIN_EXPLORER_WIDTH = 150;
	const MAX_EXPLORER_WIDTH = 500;
	let explorerWidth = $state(250);
	let isExplorerResizing = $state(false);

	function startExplorerResize(e: MouseEvent) {
		e.preventDefault();
		isExplorerResizing = true;
		document.addEventListener('mousemove', handleExplorerResize);
		document.addEventListener('mouseup', stopExplorerResize);
		document.body.style.cursor = 'ew-resize';
		document.body.style.userSelect = 'none';
	}

	function handleExplorerResize(e: MouseEvent) {
		if (!isExplorerResizing) return;
		explorerWidth = Math.min(MAX_EXPLORER_WIDTH, Math.max(MIN_EXPLORER_WIDTH, e.clientX));
	}

	function stopExplorerResize() {
		isExplorerResizing = false;
		document.removeEventListener('mousemove', handleExplorerResize);
		document.removeEventListener('mouseup', stopExplorerResize);
		document.body.style.cursor = '';
		document.body.style.userSelect = '';
	}

	// Handle opening a recent file
	async function openRecentFile(path: string) {
		try {
			await loadFromFile(path);
		} catch (e) {
			await showAlert({
				title: 'Error',
				message: `Failed to open: ${(e as Error).message}`,
			});
		}
	}
</script>

<svelte:window onbeforeunload={handleBeforeUnload} />

<div class="app">
	<TabBar />
	<div class="main-content">
		<!-- File Explorer (left, collapsible, resizable) -->
		{#if showFileExplorer}
			<div class="file-explorer-container" style="width: {explorerWidth}px">
				<FileExplorer initialPath={initialPath} onClose={() => showFileExplorer = false} />
				<div
					class="explorer-resize-handle"
					class:resizing={isExplorerResizing}
					onmousedown={startExplorerResize}
					role="separator"
					aria-orientation="vertical"
					tabindex="0"
				></div>
			</div>
		{:else}
			<button class="show-explorer-btn" onclick={() => showFileExplorer = true} title="Show file explorer">
				📁
			</button>
		{/if}

		<!-- Editor (center + sidebar) -->
		<div class="editor-container">
			<EditorShell />

			<!-- Empty state overlay when no nodes, no file open, and not a new file -->
			{#if $nodes.length === 0 && !$currentFilePath && !$isNewFile}
				<div class="empty-state">
					<div class="empty-content">
						<h2>Welcome to netrun-ui</h2>
						<p>Visual editor for NetConfig files</p>
						<div class="empty-actions">
							<button class="primary" onclick={async () => {
								const path = await showPrompt({
									title: 'Open File',
									message: 'Enter file path to open',
									placeholder: '/path/to/file.netrun.json',
									inputType: 'path',
									confirmText: 'Open',
								});
								if (path) {
									openRecentFile(path);
								}
							}}>
								Open File
							</button>
							<button onclick={async () => {
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
							}}>
								New File
							</button>
						</div>

						{#if $recentFiles.length > 0}
							<div class="recent-files">
								<p class="recent-title">Recent Files</p>
								<div class="recent-list">
									{#each $recentFiles.slice(0, 5) as file}
										<button
											class="recent-file"
											onclick={() => openRecentFile(file.path)}
											title={file.path}
										>
											{file.name}
										</button>
									{/each}
								</div>
							</div>
						{:else}
							<p class="hint">Or use Cmd+O to open, Cmd+N for new file</p>
						{/if}
					</div>
				</div>
			{/if}
		</div>
	</div>

</div>

<style>
	.app {
		height: 100vh;
		width: 100vw;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.main-content {
		flex: 1;
		display: flex;
		overflow: hidden;
	}

	.file-explorer-container {
		flex-shrink: 0;
		height: 100%;
		position: relative;
	}

	.explorer-resize-handle {
		position: absolute;
		right: 0;
		top: 0;
		bottom: 0;
		width: 4px;
		cursor: ew-resize;
		background: transparent;
		z-index: 10;
		transition: background-color 0.15s ease;
	}

	.explorer-resize-handle:hover,
	.explorer-resize-handle.resizing {
		background: var(--accent-color, #3b82f6);
	}

	.show-explorer-btn {
		position: absolute;
		left: 8px;
		top: 8px;
		z-index: 5;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 8px;
		font-size: 16px;
		cursor: pointer;
	}

	.show-explorer-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
	}

	.editor-container {
		flex: 1;
		height: 100%;
		display: flex;
		flex-direction: column;
		position: relative;
	}

	.empty-state {
		position: absolute;
		inset: 0;
		display: flex;
		align-items: center;
		justify-content: center;
		background: rgba(26, 26, 26, 0.9);
		z-index: 10;
	}

	.empty-content {
		text-align: center;
		padding: 48px;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 12px;
		max-width: 400px;
	}

	.empty-content h2 {
		font-size: 24px;
		font-weight: 600;
		margin-bottom: 8px;
		color: var(--text-primary, #fff);
	}

	.empty-content p {
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 24px;
	}

	.empty-actions {
		display: flex;
		gap: 12px;
		justify-content: center;
		margin-bottom: 16px;
	}

	.empty-actions button {
		padding: 12px 24px;
		font-size: 14px;
		font-weight: 500;
		border-radius: 6px;
		border: 1px solid var(--border-color, #404040);
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.empty-actions button:hover {
		background: var(--border-color, #404040);
	}

	.empty-actions button.primary {
		background: var(--accent-color, #3b82f6);
		border-color: var(--accent-color, #3b82f6);
	}

	.empty-actions button.primary:hover {
		background: var(--accent-hover, #2563eb);
	}

	.hint {
		font-size: 12px;
		color: var(--text-secondary, #666);
		margin-bottom: 0;
	}

	.recent-files {
		margin-top: 24px;
		padding-top: 24px;
		border-top: 1px solid var(--border-color, #404040);
	}

	.recent-title {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
		margin-bottom: 12px;
	}

	.recent-list {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.recent-file {
		padding: 8px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid transparent;
		border-radius: 4px;
		color: var(--accent-color, #3b82f6);
		font-size: 13px;
		text-align: left;
		cursor: pointer;
		transition: all 0.15s ease;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.recent-file:hover {
		background: var(--border-color, #404040);
		border-color: var(--accent-color, #3b82f6);
	}
</style>
