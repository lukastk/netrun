<script lang="ts">
	import type { EpochInfo } from '../types.js';

	interface Props {
		epochs: EpochInfo[];
	}

	let { epochs }: Props = $props();

	// Show most recent first
	let sorted = $derived([...epochs].sort((a, b) => b.created_at.localeCompare(a.created_at)));

	let expandedId = $state<string | null>(null);

	function toggle(id: string) {
		expandedId = expandedId === id ? null : id;
	}

	function formatDuration(ms: number | null): string {
		if (ms === null) return '-';
		if (ms < 1000) return `${ms.toFixed(0)}ms`;
		return `${(ms / 1000).toFixed(2)}s`;
	}

	function formatTime(iso: string): string {
		try {
			const d = new Date(iso);
			return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
		} catch {
			return iso;
		}
	}

	function stateClass(state: string): string {
		switch (state) {
			case 'finished': return 'state-finished';
			case 'running': return 'state-running';
			case 'startable': return 'state-startable';
			case 'cancelled': return 'state-cancelled';
			default: return '';
		}
	}

	function outcomeClass(outcome: string | null): string {
		if (!outcome) return '';
		if (outcome === 'success') return 'outcome-success';
		if (outcome === 'error') return 'outcome-error';
		if (outcome === 'cancelled') return 'outcome-cancelled';
		return '';
	}
</script>

<div class="epoch-table">
	{#if sorted.length === 0}
		<div class="empty">No epochs yet</div>
	{:else}
		<table>
			<thead>
				<tr>
					<th>Node</th>
					<th>State</th>
					<th>Outcome</th>
					<th>Duration</th>
					<th>Started</th>
					<th>Info</th>
				</tr>
			</thead>
			<tbody>
				{#each sorted as epoch (epoch.epoch_id)}
					<tr class="epoch-row" class:expanded={expandedId === epoch.epoch_id} onclick={() => toggle(epoch.epoch_id)}>
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
						<td class="mono">{epoch.started_at ? formatTime(epoch.started_at) : '-'}</td>
						<td class="info-cell">
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
						</td>
					</tr>
					{#if expandedId === epoch.epoch_id}
						<tr class="detail-row">
							<td colspan="6">
								<div class="detail">
									<div class="detail-grid">
										<span class="detail-label">Epoch ID</span>
										<span class="detail-value mono">{epoch.epoch_id}</span>
										{#if epoch.pool_id}
											<span class="detail-label">Pool</span>
											<span class="detail-value">{epoch.pool_id} / worker {epoch.worker_id ?? '-'}</span>
										{/if}
										{#if epoch.factory}
											<span class="detail-label">Factory</span>
											<span class="detail-value mono">{epoch.factory}</span>
										{/if}
										{#if epoch.created_at}
											<span class="detail-label">Created</span>
											<span class="detail-value mono">{formatTime(epoch.created_at)}</span>
										{/if}
										{#if epoch.ended_at}
											<span class="detail-label">Ended</span>
											<span class="detail-value mono">{formatTime(epoch.ended_at)}</span>
										{/if}
									</div>
									{#if epoch.error}
										<div class="error-block">
											<div class="error-type">{epoch.error_type ?? 'Error'}: {epoch.error}</div>
											{#if epoch.error_traceback}
												<pre class="traceback">{epoch.error_traceback}</pre>
											{/if}
										</div>
									{/if}
								</div>
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

	.info-cell {
		display: flex;
		gap: 4px;
		align-items: center;
	}

	.tag {
		display: inline-block;
		padding: 0 4px;
		border-radius: 2px;
		font-size: 10px;
		background: rgba(59, 130, 246, 0.15);
		color: var(--accent-color);
	}

	.tag-error {
		background: rgba(239, 68, 68, 0.15);
		color: var(--error-color);
	}

	.detail-row td {
		padding: 0;
		border-bottom: 1px solid var(--border-color);
	}

	.detail {
		padding: 8px 10px;
		background: var(--bg-primary);
	}

	.detail-grid {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 2px 12px;
		font-size: 11px;
	}

	.detail-label {
		color: var(--text-secondary);
	}

	.detail-value {
		color: var(--text-primary);
	}

	.error-block {
		margin-top: 8px;
		padding: 6px 8px;
		background: rgba(239, 68, 68, 0.08);
		border: 1px solid rgba(239, 68, 68, 0.2);
		border-radius: 4px;
	}

	.error-type {
		font-size: 11px;
		color: var(--error-color);
		font-weight: 600;
	}

	.traceback {
		margin-top: 4px;
		font-size: 10px;
		color: var(--text-secondary);
		white-space: pre-wrap;
		word-break: break-all;
		max-height: 200px;
		overflow: auto;
	}
</style>
