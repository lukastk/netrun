<script lang="ts">
	import type { EpochInfo } from '../types.js';
	import { formatDuration, formatTimeMs, formatFieldValue } from '../format.js';

	interface Props {
		epoch: EpochInfo;
	}

	let { epoch }: Props = $props();

	let expandedLogKey = $state<string | null>(null);
</script>

<div class="epoch-detail">
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
			<span class="detail-value mono">{formatTimeMs(epoch.created_at)}</span>
		{/if}
		{#if epoch.started_at}
			<span class="detail-label">Started</span>
			<span class="detail-value mono">{formatTimeMs(epoch.started_at)}</span>
		{/if}
		{#if epoch.ended_at}
			<span class="detail-label">Ended</span>
			<span class="detail-value mono">{formatTimeMs(epoch.ended_at)}</span>
		{/if}
		{#if epoch.queue_time_ms !== null && epoch.queue_time_ms !== undefined}
			<span class="detail-label">Queue</span>
			<span class="detail-value mono">{formatDuration(epoch.queue_time_ms)}</span>
		{/if}
		{#if epoch.in_salvo_ports.length > 0}
			<span class="detail-label">Input</span>
			<span class="detail-value">{epoch.in_salvo_ports.join(', ')} ({epoch.in_salvo_packet_count} packets)</span>
		{/if}
		{#if epoch.out_salvo_count > 0}
			<span class="detail-label">Output</span>
			<span class="detail-value">{epoch.out_salvo_count} salvos</span>
		{/if}
		{#if epoch.orphaned_packet_count > 0}
			<span class="detail-label">Orphaned</span>
			<span class="detail-value warn">{epoch.orphaned_packet_count} packets</span>
		{/if}
		{#if epoch.destroyed_packet_count > 0}
			<span class="detail-label">Destroyed</span>
			<span class="detail-value warn">{epoch.destroyed_packet_count} packets</span>
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
	{#if epoch.node_log_entries.length > 0}
		<div class="structured-logs">
			<div class="section-label">Structured Logs</div>
			{#each epoch.node_log_entries as entry, ei}
				{@const logKey = `${epoch.epoch_id}::${ei}`}
				{@const hasFields = Object.keys(entry.fields).length > 0}
				<div
					class="log-line"
					class:expandable={hasFields}
					onclick={() => hasFields && (expandedLogKey = expandedLogKey === logKey ? null : logKey)}
				>
					<span class="mono muted">{formatTimeMs(entry.timestamp)}</span>
					{#if entry.level === 'error'}
						<span class="error-badge">ERR</span>
					{/if}
					<span class="log-msg">{entry.message ?? ''}</span>
					{#if hasFields}
						<span class="field-indicator">&#9656; {Object.keys(entry.fields).length} fields</span>
					{/if}
				</div>
				{#if expandedLogKey === logKey}
					<div class="field-detail">
						{#each Object.entries(entry.fields) as [k, v]}
							<div class="field-row">
								<span class="field-key">{k}</span>
								<span class="field-value">{formatFieldValue(v)}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/each}
		</div>
	{/if}
</div>

<style>
	.epoch-detail {
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

	.detail-value.warn {
		color: var(--error-color);
	}

	.mono {
		font-variant-numeric: tabular-nums;
	}

	.muted {
		color: var(--text-secondary);
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

	.structured-logs {
		margin-top: 8px;
	}

	.section-label {
		font-size: 10px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		color: var(--text-secondary);
		margin-bottom: 4px;
	}

	.log-line {
		display: flex;
		gap: 6px;
		padding: 2px 0;
		font-size: 11px;
		flex-wrap: wrap;
		align-items: baseline;
	}

	.log-line.expandable {
		cursor: pointer;
	}

	.log-line.expandable:hover {
		background: var(--bg-tertiary);
	}

	.log-msg {
		color: var(--text-primary);
	}

	.error-badge {
		font-size: 9px;
		font-weight: 600;
		padding: 0 3px;
		border-radius: 2px;
		background: rgba(239, 68, 68, 0.15);
		color: var(--error-color);
	}

	.field-indicator {
		color: var(--text-secondary);
		font-size: 10px;
	}

	.field-detail {
		padding: 2px 0 4px 16px;
		border-left: 2px solid var(--border-color);
		margin-left: 4px;
	}

	.field-row {
		display: flex;
		gap: 8px;
		font-size: 10px;
		padding: 1px 0;
	}

	.field-key {
		color: var(--purple-color);
		flex-shrink: 0;
	}

	.field-value {
		color: var(--text-primary);
	}
</style>
