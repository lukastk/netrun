<script lang="ts">
	import { api, type FileEntry } from '$lib/api';
	import { loadFromFile } from '$lib/stores/flowStore';

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

	// Load directory contents
	async function loadDirectory(path: string) {
		isLoading = true;
		error = null;

		try {
			const response = await api.listDirectory(path);
			currentPath = response.path;
			parentPath = response.parent;
			entries = response.entries;
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
				alert(`Failed to open file: ${(e as Error).message}`);
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
	async function handleNavigate() {
		const path = prompt('Enter path:', currentPath);
		if (path) {
			await loadDirectory(path);
		}
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
