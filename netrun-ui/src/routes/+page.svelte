<script lang="ts">
	import { SvelteFlowProvider } from '@xyflow/svelte';
	import Toolbar from '$lib/components/Toolbar.svelte';
	import TabBar from '$lib/components/TabBar.svelte';
	import Sidebar from '$lib/components/Sidebar.svelte';
	import FlowEditor from '$lib/components/FlowEditor.svelte';
	import FileExplorer from '$lib/components/FileExplorer.svelte';
	import { nodes, currentFilePath, activeTab, recentFiles, loadFromFile } from '$lib/stores/flowStore';

	// Initial path for file explorer (from environment variable or default to home)
	const initialPath = import.meta.env.VITE_INITIAL_PATH || '~';

	// File explorer visibility state
	let showFileExplorer = $state(true);

	// Handle opening a recent file
	async function openRecentFile(path: string) {
		try {
			await loadFromFile(path);
		} catch (e) {
			alert(`Failed to open: ${(e as Error).message}`);
		}
	}
</script>

<div class="app">
	<Toolbar />
	<TabBar />
	<div class="main-content">
		<!-- File Explorer (left, collapsible) -->
		{#if showFileExplorer}
			<div class="file-explorer-container">
				<FileExplorer initialPath={initialPath} onClose={() => showFileExplorer = false} />
			</div>
		{:else}
			<button class="show-explorer-btn" onclick={() => showFileExplorer = true} title="Show file explorer">
				📁
			</button>
		{/if}

		<!-- Canvas (center) -->
		<div class="canvas-container">
			<SvelteFlowProvider>
				<FlowEditor />

				<!-- Empty state overlay when no nodes and no file open -->
				{#if $nodes.length === 0 && !$currentFilePath}
					<div class="empty-state">
						<div class="empty-content">
							<h2>Welcome to netrun-ui</h2>
							<p>Visual editor for NetConfig files</p>
							<div class="empty-actions">
								<button class="primary" onclick={() => {
									const path = prompt('Enter file path to open:');
									if (path) {
										openRecentFile(path);
									}
								}}>
									Open File
								</button>
								<button onclick={() => {
									// Just dismiss the empty state - user can start adding nodes
									import('$lib/stores/flowStore').then(({ clearFlow }) => {
										clearFlow();
									});
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
			</SvelteFlowProvider>
		</div>

		<!-- Properties Sidebar (right) -->
		<Sidebar />
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
		width: 250px;
		flex-shrink: 0;
		height: 100%;
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

	.canvas-container {
		flex: 1;
		height: 100%;
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
