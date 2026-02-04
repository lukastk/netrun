<script lang="ts">
	import { api, type FileEntry } from '$lib/api';
	import { loadFromFile, recentFiles, removeRecentFile } from '$lib/stores/flowStore';
	import { fileExplorerRefreshTrigger, setCurrentExplorerPath } from '$lib/stores/fileExplorerStore';
	import { toasts } from '$lib/stores/toastStore';
	import TextInputModal from './TextInputModal.svelte';

	// Props
	interface Props {
		initialPath?: string;
		onClose?: () => void;
	}

	let { initialPath = '~', onClose }: Props = $props();

	// State
	let currentPath = $state(initialPath);
	let entries = $state<FileEntry[]>([]);
	let parentPath = $state<string | null>(null);
	let isLoading = $state(false);
	let error = $state<string | null>(null);
	let expandedDirs = $state<Set<string>>(new Set());
	let showNavigateModal = $state(false);
	let recentsCollapsed = $state(true);

	// Load directory contents
	async function loadDirectory(path: string) {
		isLoading = true;
		error = null;

		try {
			const response = await api.listDirectory(path);
			currentPath = response.path;
			parentPath = response.parent;
			entries = response.entries;
			// Update global current path for relative path resolution
			setCurrentExplorerPath(response.path);
		} catch (e) {
			error = (e as Error).message;
		} finally {
			isLoading = false;
		}
	}

	// Initial load
	$effect(() => {
		loadDirectory(initialPath);
	});

	// Refresh when triggered externally (e.g., after file save)
	$effect(() => {
		// Subscribe to refresh trigger
		const trigger = $fileExplorerRefreshTrigger;
		if (trigger > 0) {
			loadDirectory(currentPath);
		}
	});

	// Handle clicking on an entry
	async function handleEntryClick(entry: FileEntry) {
		if (entry.is_dir) {
			// Navigate into directory
			await loadDirectory(entry.path);
		} else if (entry.is_netrun_file) {
			// Open the file
			try {
				await loadFromFile(entry.path);
			} catch (e) {
				toasts.error(`Failed to open file: ${(e as Error).message}`);
			}
		}
	}

	// Go to parent directory
	async function handleGoUp() {
		if (parentPath) {
			await loadDirectory(parentPath);
		}
	}

	// Refresh current directory
	async function handleRefresh() {
		await loadDirectory(currentPath);
	}

	// Navigate to a specific path
	function handleNavigate() {
		showNavigateModal = true;
	}

	async function navigateToPath(path: string) {
		showNavigateModal = false;
		await loadDirectory(path);
	}
</script>

<div class="file-explorer">
	<div class="explorer-header">
		<h3>Files</h3>
		<div class="header-actions">
			<button onclick={handleRefresh} title="Refresh" class="icon-btn">↻</button>
			{#if onClose}
				<button onclick={onClose} title="Close" class="icon-btn">×</button>
			{/if}
		</div>
	</div>

	<!-- Recent Files Section -->
	{#if $recentFiles.length > 0}
		<div class="recent-files-section">
			<button class="section-header" onclick={() => recentsCollapsed = !recentsCollapsed}>
				<span class="section-title">Recent</span>
				<span class="section-toggle">{recentsCollapsed ? '+' : '−'}</span>
			</button>
			{#if !recentsCollapsed}
				<div class="recent-files-list">
					{#each $recentFiles.slice(0, 5) as file}
						<button
							class="file-entry netrun-file"
							onclick={() => loadFromFile(file.path).catch(e => {
								toasts.error(`Failed to open: ${e.message}`);
								removeRecentFile(file.path);
							})}
							title={file.path}
						>
							<span class="entry-icon">📊</span>
							<span class="entry-name">{file.name}</span>
						</button>
					{/each}
				</div>
			{/if}
		</div>
	{/if}

	<div class="path-bar">
		<button onclick={handleGoUp} disabled={!parentPath} class="icon-btn" title="Go up">↑</button>
		<button onclick={handleNavigate} class="path-display" title="Click to navigate">
			{currentPath}
		</button>
	</div>

	<div class="file-list">
		{#if isLoading}
			<div class="loading">Loading...</div>
		{:else if error}
			<div class="error">{error}</div>
		{:else if entries.length === 0}
			<div class="empty">No files found</div>
		{:else}
			{#each entries as entry}
				<button
					class="file-entry"
					class:directory={entry.is_dir}
					class:netrun-file={entry.is_netrun_file}
					onclick={() => handleEntryClick(entry)}
				>
					<span class="entry-icon">
						{#if entry.is_dir}
							📁
						{:else if entry.is_netrun_file}
							📊
						{:else}
							📄
						{/if}
					</span>
					<span class="entry-name">{entry.name}</span>
				</button>
			{/each}
		{/if}
	</div>
</div>

{#if showNavigateModal}
	<TextInputModal
		title="Navigate to Path"
		label="Path"
		placeholder="/path/to/directory"
		initialValue={currentPath}
		submitLabel="Go"
		onSubmit={navigateToPath}
		onCancel={() => showNavigateModal = false}
	/>
{/if}

<style>
	.file-explorer {
		width: 100%;
		height: 100%;
		display: flex;
		flex-direction: column;
		background: var(--bg-secondary, #242424);
		border-right: 1px solid var(--border-color, #404040);
	}

	.explorer-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 12px 16px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.explorer-header h3 {
		font-size: 14px;
		font-weight: 600;
		margin: 0;
		color: var(--text-primary, #fff);
	}

	.header-actions {
		display: flex;
		gap: 4px;
	}

	.icon-btn {
		background: transparent;
		border: none;
		padding: 4px 8px;
		font-size: 14px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		border-radius: 4px;
	}

	.icon-btn:hover:not(:disabled) {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.icon-btn:disabled {
		opacity: 0.3;
		cursor: not-allowed;
	}

	.recent-files-section {
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.section-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		width: 100%;
		padding: 8px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border: none;
		cursor: pointer;
	}

	.section-header:hover {
		background: var(--border-color, #404040);
	}

	.section-title {
		font-size: 11px;
		font-weight: 600;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.section-toggle {
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
	}

	.recent-files-list {
		padding: 4px 8px;
	}

	.path-bar {
		display: flex;
		align-items: center;
		padding: 8px;
		gap: 4px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.path-display {
		flex: 1;
		text-align: left;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 6px 10px;
		font-size: 11px;
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.path-display:hover {
		border-color: var(--accent-color, #3b82f6);
	}

	.file-list {
		flex: 1;
		overflow-y: auto;
		padding: 8px;
	}

	.loading, .error, .empty {
		padding: 16px;
		text-align: center;
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
	}

	.error {
		color: var(--error-color, #ef4444);
	}

	.file-entry {
		display: flex;
		align-items: center;
		gap: 8px;
		width: 100%;
		padding: 6px 8px;
		background: transparent;
		border: none;
		border-radius: 4px;
		font-size: 13px;
		color: var(--text-primary, #fff);
		cursor: pointer;
		text-align: left;
	}

	.file-entry:hover {
		background: var(--bg-tertiary, #2d2d2d);
	}

	.file-entry.directory {
		color: var(--text-primary, #fff);
	}

	.file-entry.netrun-file {
		color: var(--accent-color, #3b82f6);
	}

	.file-entry:not(.directory):not(.netrun-file) {
		color: var(--text-secondary, #666);
		opacity: 0.7;
	}

	.entry-icon {
		flex-shrink: 0;
		width: 16px;
		text-align: center;
	}

	.entry-name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
</style>
