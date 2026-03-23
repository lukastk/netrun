<script lang="ts">
	import type { EpochInfo } from '../types.js';
	import { formatDuration, formatTime, stateClass, outcomeClass } from '../format.js';
	import EpochDetail from './EpochDetail.svelte';

	interface Props {
		epochs: EpochInfo[];
		onNodeHighlight?: (nodeName: string | null) => void;
	}

	let { epochs, onNodeHighlight }: Props = $props();

	// Clear: filter out epochs before this timestamp
	let clearedAt = $state<string | null>(null);

	let visible = $derived(
		clearedAt ? epochs.filter((e) => e.created_at > clearedAt!) : epochs,
	);

	// Sorting
	type SortKey = 'node_name' | 'state' | 'outcome' | 'duration_ms' | 'queue_time_ms' | 'started_at';
	let sortKey = $state<SortKey>('started_at');
	let sortAsc = $state(false);

	function toggleSort(key: SortKey) {
		if (sortKey === key) {
			sortAsc = !sortAsc;
		} else {
			sortKey = key;
			sortAsc = key === 'node_name'; // default asc for name, desc for others
		}
	}

	let sorted = $derived.by(() => {
		const arr = [...visible];
		const dir = sortAsc ? 1 : -1;
		arr.sort((a, b) => {
			const av = a[sortKey];
			const bv = b[sortKey];
			if (av == null && bv == null) return 0;
			if (av == null) return 1;
			if (bv == null) return -1;
			if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
			return String(av).localeCompare(String(bv)) * dir;
		});
		return arr;
	});

	let expandedId = $state<string | null>(null);

	function toggle(epoch: EpochInfo) {
		const wasExpanded = expandedId === epoch.epoch_id;
		expandedId = wasExpanded ? null : epoch.epoch_id;
		onNodeHighlight?.(wasExpanded ? null : epoch.node_name);
	}

	function handleClear() {
		const now = new Date().toISOString();
		clearedAt = now;
	}

	function sortIndicator(key: SortKey): string {
		if (sortKey !== key) return '';
		return sortAsc ? ' ▲' : ' ▼';
	}
</script>

<div class="epoch-table">
	<div class="toolbar">
		<span class="count">{visible.length} epochs</span>
		<button class="clear-btn" onclick={handleClear}>Clear</button>
	</div>
	{#if sorted.length === 0}
		<div class="empty">No epochs</div>
	{:else}
		<table>
			<thead>
				<tr>
					<th class="sortable" onclick={() => toggleSort('node_name')}>Node{sortIndicator('node_name')}</th>
					<th class="sortable" onclick={() => toggleSort('state')}>State{sortIndicator('state')}</th>
					<th class="sortable" onclick={() => toggleSort('outcome')}>Outcome{sortIndicator('outcome')}</th>
					<th class="sortable" onclick={() => toggleSort('duration_ms')}>Duration{sortIndicator('duration_ms')}</th>
					<th class="sortable" onclick={() => toggleSort('queue_time_ms')}>Queue{sortIndicator('queue_time_ms')}</th>
					<th class="sortable" onclick={() => toggleSort('started_at')}>Started{sortIndicator('started_at')}</th>
					<th>Info</th>
				</tr>
			</thead>
			<tbody>
				{#each sorted as epoch (epoch.epoch_id)}
					<tr class="epoch-row" class:expanded={expandedId === epoch.epoch_id} onclick={() => toggle(epoch)}>
						<td class="node-name">{epoch.node_name}</td>
						<td><span class="badge {stateClass(epoch.state)}">{epoch.state}</span></td>
						<td>
							{#if epoch.outcome}
								<span class="badge {outcomeClass(epoch.outcome)}">{epoch.outcome}</span>
							{:else}
								<span class="muted">-</span>
							{/if}
						</td>
						<td class="mono">{formatDuration(epoch.duration_ms)}</td>
						<td class="mono">{formatDuration(epoch.queue_time_ms)}</td>
						<td class="mono">{epoch.started_at ? formatTime(epoch.started_at) : '-'}</td>
						<td>
							{#if epoch.was_cache_hit}
								<span class="tag">cache</span>
							{/if}
							{#if epoch.was_file_storage_hit}
								<span class="tag">file</span>
							{/if}
							{#if epoch.retry_count && epoch.retry_count > 0}
								<span class="tag">retry:{epoch.retry_count}</span>
							{/if}
							{#if epoch.error}
								<span class="tag tag-error">err</span>
							{/if}
							{#if epoch.node_log_entries.length > 0}
								<span class="tag tag-log">{epoch.node_log_entries.length} logs</span>
							{/if}
						</td>
					</tr>
					{#if expandedId === epoch.epoch_id}
						<tr class="detail-row">
							<td colspan="7">
								<EpochDetail {epoch} />
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	{/if}
</div>

<style>
	.epoch-table {
		height: 100%;
		overflow: auto;
		display: flex;
		flex-direction: column;
	}

	.toolbar {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 4px 8px;
		border-bottom: 1px solid var(--border-color);
		flex-shrink: 0;
	}

	.count {
		font-size: 11px;
		color: var(--text-secondary);
	}

	.clear-btn {
		margin-left: auto;
		padding: 2px 8px;
		font-size: 10px;
		background: var(--bg-tertiary);
		border: 1px solid var(--border-color);
	}

	.clear-btn:hover {
		background: var(--border-color);
	}

	.empty {
		padding: 16px;
		text-align: center;
		color: var(--text-secondary);
		font-size: 12px;
	}

	table {
		width: 100%;
		border-collapse: collapse;
		font-size: 12px;
	}

	thead {
		position: sticky;
		top: 0;
		z-index: 1;
	}

	th {
		background: var(--bg-tertiary);
		padding: 6px 10px;
		text-align: left;
		font-weight: 600;
		color: var(--text-secondary);
		font-size: 11px;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		border-bottom: 1px solid var(--border-color);
	}

	th.sortable {
		cursor: pointer;
		user-select: none;
	}

	th.sortable:hover {
		color: var(--text-primary);
	}

	td {
		padding: 5px 10px;
		border-bottom: 1px solid var(--border-color);
		white-space: nowrap;
	}

	.epoch-row {
		cursor: pointer;
	}

	.epoch-row:hover {
		background: var(--bg-tertiary);
	}

	.epoch-row.expanded {
		background: var(--bg-tertiary);
	}

	.node-name {
		font-weight: 600;
	}

	.mono {
		font-family: inherit;
		font-variant-numeric: tabular-nums;
	}

	.muted {
		color: var(--text-secondary);
	}

	.badge {
		display: inline-block;
		padding: 1px 6px;
		border-radius: 3px;
		font-size: 10px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.03em;
	}

	.state-finished { background: rgba(160, 160, 160, 0.15); color: var(--text-secondary); }
	.state-running { background: rgba(59, 130, 246, 0.15); color: var(--accent-color); }
	.state-startable { background: rgba(245, 158, 11, 0.15); color: var(--warning-color); }
	.state-cancelled { background: rgba(160, 160, 160, 0.1); color: var(--text-secondary); opacity: 0.7; }

	.outcome-success { background: rgba(34, 197, 94, 0.15); color: var(--success-color); }
	.outcome-error { background: rgba(239, 68, 68, 0.15); color: var(--error-color); }
	.outcome-cancelled { background: rgba(160, 160, 160, 0.1); color: var(--text-secondary); }

	.tag {
		display: inline-block;
		padding: 0 4px;
		border-radius: 2px;
		font-size: 10px;
		margin-right: 4px;
		background: rgba(59, 130, 246, 0.15);
		color: var(--accent-color);
	}

	.tag-error {
		background: rgba(239, 68, 68, 0.15);
		color: var(--error-color);
	}

	.tag-log {
		background: rgba(168, 85, 247, 0.15);
		color: var(--purple-color);
	}

	.detail-row td {
		padding: 0;
		border-bottom: 1px solid var(--border-color);
	}
</style>
