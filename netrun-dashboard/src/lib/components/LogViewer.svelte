<script lang="ts">
	import type { LogEntry } from '../types.js';

	interface Props {
		logs: LogEntry[];
	}

	let { logs }: Props = $props();

	let filterNode = $state('');
	let autoScroll = $state(true);
	let listEl: HTMLDivElement | undefined = $state();

	let filtered = $derived(
		filterNode
			? logs.filter((l) => l.node_name?.toLowerCase().includes(filterNode.toLowerCase()))
			: logs,
	);

	function formatTime(iso: string): string {
		try {
			const d = new Date(iso);
			return d.toLocaleTimeString('en-US', {
				hour12: false,
				hour: '2-digit',
				minute: '2-digit',
				second: '2-digit',
				fractionalSecondDigits: 3,
			});
		} catch {
			return iso;
		}
	}

	// Auto-scroll to bottom when new logs arrive
	$effect(() => {
		// Subscribe to filtered length
		filtered.length;
		if (autoScroll && listEl) {
			requestAnimationFrame(() => {
				listEl!.scrollTop = listEl!.scrollHeight;
			});
		}
	});
</script>

<div class="log-viewer">
	<div class="toolbar">
		<input
			type="text"
			placeholder="Filter by node..."
			bind:value={filterNode}
			class="filter-input"
		/>
		<label class="auto-scroll-toggle">
			<input type="checkbox" bind:checked={autoScroll} />
			Auto-scroll
		</label>
		<span class="log-count">{filtered.length} entries</span>
	</div>

	<div class="log-list" bind:this={listEl}>
		{#if filtered.length === 0}
			<div class="empty">No logs</div>
		{:else}
			{#each filtered as log, i (i)}
				<div class="log-entry">
					<span class="log-time">{formatTime(log.timestamp)}</span>
					{#if log.node_name}
						<span class="log-node">{log.node_name}</span>
					{/if}
					<span class="log-message">{log.message}</span>
				</div>
			{/each}
		{/if}
	</div>
</div>

<style>
	.log-viewer {
		display: flex;
		flex-direction: column;
		height: 100%;
		overflow: hidden;
	}

	.toolbar {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 4px 8px;
		border-bottom: 1px solid var(--border-color);
		flex-shrink: 0;
	}

	.filter-input {
		width: 160px;
		padding: 3px 8px;
		font-size: 11px;
	}

	.auto-scroll-toggle {
		display: flex;
		align-items: center;
		gap: 4px;
		font-size: 11px;
		color: var(--text-secondary);
		cursor: pointer;
		user-select: none;
	}

	.auto-scroll-toggle input {
		margin: 0;
		width: auto;
		padding: 0;
	}

	.log-count {
		margin-left: auto;
		font-size: 11px;
		color: var(--text-secondary);
	}

	.log-list {
		flex: 1;
		overflow: auto;
		font-size: 11px;
		padding: 4px 0;
	}

	.empty {
		padding: 16px;
		text-align: center;
		color: var(--text-secondary);
		font-size: 12px;
	}

	.log-entry {
		display: flex;
		gap: 8px;
		padding: 2px 8px;
		line-height: 1.5;
	}

	.log-entry:hover {
		background: var(--bg-tertiary);
	}

	.log-time {
		color: var(--text-secondary);
		flex-shrink: 0;
		font-variant-numeric: tabular-nums;
	}

	.log-node {
		color: var(--accent-color);
		flex-shrink: 0;
		font-weight: 600;
		min-width: 80px;
	}

	.log-message {
		color: var(--text-primary);
		white-space: pre-wrap;
		word-break: break-word;
	}
</style>
