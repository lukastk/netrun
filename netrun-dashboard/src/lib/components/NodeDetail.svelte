<script lang="ts">
	import type { ObserveState } from '../types.js';
	import { formatDuration, formatTimeMs, formatFieldValue, stateClass, outcomeClass } from '../format.js';

	interface Props {
		nodeName: string;
		liveState: ObserveState | null;
		onClose: () => void;
	}

	let { nodeName, liveState, onClose }: Props = $props();

	let nodeStatus = $derived(liveState?.nodes.find((n) => n.name === nodeName) ?? null);

	let nodeEpochs = $derived(
		(liveState?.epochs ?? [])
			.filter((e) => e.node_name === nodeName)
			.sort((a, b) => b.created_at.localeCompare(a.created_at)),
	);

	let nodeLogs = $derived(
		(liveState?.logs ?? []).filter((l) => l.node_name === nodeName),
	);

	let expandedEpochId = $state<string | null>(null);

	// Collapsible sections
	let statusOpen = $state(true);
	let portsOpen = $state(true);
	let epochsOpen = $state(true);
	let logsOpen = $state(true);
</script>

<div class="node-detail">
	<div class="detail-header">
		<span class="detail-title">{nodeName}</span>
		<button class="close-btn" onclick={onClose}>&#x2715;</button>
	</div>

	{#if nodeStatus}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="section">
			<div class="section-title" onclick={() => (statusOpen = !statusOpen)}>
				<span class="chevron" class:open={statusOpen}>&#9656;</span> Status
			</div>
			{#if statusOpen}
				<div class="info-grid">
					<span class="label">Status</span>
					<span class="value">
						{#if !nodeStatus.enabled}
							<span class="badge state-cancelled">disabled</span>
						{:else if nodeStatus.is_busy}
							<span class="badge state-running">running</span>
						{:else}
							<span class="badge state-finished">idle</span>
						{/if}
					</span>
					<span class="label">Epochs</span>
					<span class="value">{nodeStatus.epoch_count}</span>
					{#if nodeStatus.pools.length > 0}
						<span class="label">Pools</span>
						<span class="value">{nodeStatus.pools.join(', ')}</span>
					{/if}
					{#if nodeStatus.factory}
						<span class="label">Factory</span>
						<span class="value mono">{nodeStatus.factory}</span>
					{/if}
				</div>
			{/if}
		</div>

		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="section">
			<div class="section-title" onclick={() => (portsOpen = !portsOpen)}>
				<span class="chevron" class:open={portsOpen}>&#9656;</span> Ports
			</div>
			{#if portsOpen}
				{#if nodeStatus.in_port_names.length > 0}
					<div class="port-group">
						<span class="port-label">In</span>
						{#each nodeStatus.in_port_names as port}
							<div class="port-item">
								<span class="port-name">{port}</span>
								{#if (nodeStatus.input_port_packet_counts[port] ?? 0) > 0}
									<span class="packet-badge">{nodeStatus.input_port_packet_counts[port]}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
				{#if nodeStatus.out_port_names.length > 0}
					<div class="port-group">
						<span class="port-label">Out</span>
						{#each nodeStatus.out_port_names as port}
							<div class="port-item">
								<span class="port-name">{port}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	{/if}

	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="section">
		<div class="section-title" onclick={() => (epochsOpen = !epochsOpen)}>
			<span class="chevron" class:open={epochsOpen}>&#9656;</span> Epoch History ({nodeEpochs.length})
		</div>
		{#if epochsOpen}
			<div class="epoch-list">
				{#each nodeEpochs.slice(0, 50) as epoch (epoch.epoch_id)}
					<div
						class="epoch-item"
						class:expanded={expandedEpochId === epoch.epoch_id}
						onclick={() => (expandedEpochId = expandedEpochId === epoch.epoch_id ? null : epoch.epoch_id)}
					>
						<span class="badge {stateClass(epoch.state)}">{epoch.state}</span>
						{#if epoch.outcome}
							<span class="badge {outcomeClass(epoch.outcome)}">{epoch.outcome}</span>
						{/if}
						<span class="mono">{formatDuration(epoch.duration_ms)}</span>
						<span class="mono muted">{epoch.started_at ? formatTimeMs(epoch.started_at) : ''}</span>
					</div>
					{#if expandedEpochId === epoch.epoch_id}
						<div class="epoch-detail">
							<div class="info-grid">
								<span class="label">Epoch ID</span>
								<span class="value mono">{epoch.epoch_id}</span>
								{#if epoch.pool_id}
									<span class="label">Pool</span>
									<span class="value">{epoch.pool_id} / worker {epoch.worker_id ?? '-'}</span>
								{/if}
								{#if epoch.queue_time_ms !== null}
									<span class="label">Queue</span>
									<span class="value mono">{formatDuration(epoch.queue_time_ms)}</span>
								{/if}
								{#if epoch.in_salvo_ports.length > 0}
									<span class="label">Input</span>
									<span class="value">{epoch.in_salvo_ports.join(', ')} ({epoch.in_salvo_packet_count} pkts)</span>
								{/if}
							</div>
							{#if epoch.error}
								<div class="error-block">
									<div class="error-type">{epoch.error_type}: {epoch.error}</div>
								</div>
							{/if}
							{#if epoch.node_log_entries.length > 0}
								<div class="structured-logs">
									{#each epoch.node_log_entries as entry}
										<div class="log-line">
											<span class="mono muted">{formatTimeMs(entry.timestamp)}</span>
											<span>{entry.message ?? ''}</span>
											{#each Object.entries(entry.fields) as [k, v]}
												<span class="field"><span class="field-key">{k}</span>={formatFieldValue(v)}</span>
											{/each}
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{/if}
				{/each}
				{#if nodeEpochs.length === 0}
					<div class="empty">No epochs</div>
				{/if}
			</div>
		{/if}
	</div>

	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="section">
		<div class="section-title" onclick={() => (logsOpen = !logsOpen)}>
			<span class="chevron" class:open={logsOpen}>&#9656;</span> Recent Logs ({nodeLogs.length})
		</div>
		{#if logsOpen}
			<div class="log-list">
				{#each nodeLogs.slice(-30) as log}
					<div class="log-line">
						<span class="mono muted">{formatTimeMs(log.timestamp)}</span>
						<span>{log.message}</span>
					</div>
				{/each}
				{#if nodeLogs.length === 0}
					<div class="empty">No logs</div>
				{/if}
			</div>
		{/if}
	</div>
</div>

<style>
	.node-detail {
		height: 100%;
		overflow-y: auto;
		font-size: 12px;
	}

	.detail-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 12px 16px;
		border-bottom: 1px solid var(--border-color);
		position: sticky;
		top: 0;
		background: var(--bg-secondary);
		z-index: 1;
	}

	.detail-title {
		font-size: 14px;
		font-weight: 700;
	}

	.close-btn {
		padding: 2px 6px;
		font-size: 11px;
		color: var(--text-secondary);
		background: transparent;
	}

	.close-btn:hover {
		color: var(--text-primary);
		background: transparent;
	}

	.section {
		padding: 10px 16px;
		border-bottom: 1px solid var(--border-color);
	}

	.section-title {
		font-size: 10px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		color: var(--text-secondary);
		margin-bottom: 6px;
		cursor: pointer;
		user-select: none;
	}

	.section-title:hover {
		color: var(--text-primary);
	}

	.chevron {
		display: inline-block;
		transition: transform 0.15s;
	}

	.chevron.open {
		transform: rotate(90deg);
	}

	.info-grid {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 2px 10px;
		font-size: 11px;
	}

	.label {
		color: var(--text-secondary);
	}

	.value {
		color: var(--text-primary);
	}

	.mono {
		font-variant-numeric: tabular-nums;
	}

	.muted {
		color: var(--text-secondary);
	}

	.badge {
		display: inline-block;
		padding: 1px 5px;
		border-radius: 3px;
		font-size: 9px;
		font-weight: 600;
		text-transform: uppercase;
	}

	.state-finished { background: rgba(160, 160, 160, 0.15); color: var(--text-secondary); }
	.state-running { background: rgba(59, 130, 246, 0.15); color: var(--accent-color); }
	.state-startable { background: rgba(245, 158, 11, 0.15); color: var(--warning-color); }
	.state-cancelled { background: rgba(160, 160, 160, 0.1); color: var(--text-secondary); }
	.outcome-success { background: rgba(34, 197, 94, 0.15); color: var(--success-color); }
	.outcome-error { background: rgba(239, 68, 68, 0.15); color: var(--error-color); }

	.port-group {
		margin-bottom: 4px;
	}

	.port-label {
		font-size: 10px;
		font-weight: 600;
		color: var(--text-secondary);
		text-transform: uppercase;
	}

	.port-item {
		display: flex;
		align-items: center;
		gap: 6px;
		padding: 1px 0 1px 12px;
		font-size: 11px;
	}

	.port-name {
		color: var(--text-primary);
	}

	.packet-badge {
		font-size: 9px;
		padding: 0 4px;
		border-radius: 8px;
		background: rgba(59, 130, 246, 0.2);
		color: var(--accent-color);
		font-weight: 600;
	}

	.epoch-list, .log-list {
		max-height: 300px;
		overflow-y: auto;
	}

	.epoch-item {
		display: flex;
		align-items: center;
		gap: 6px;
		padding: 3px 0;
		cursor: pointer;
		font-size: 11px;
	}

	.epoch-item:hover {
		background: var(--bg-tertiary);
	}

	.epoch-detail {
		padding: 6px 0 6px 12px;
		border-left: 2px solid var(--border-color);
		margin-left: 4px;
		margin-bottom: 4px;
	}

	.error-block {
		margin-top: 4px;
		padding: 4px 6px;
		background: rgba(239, 68, 68, 0.08);
		border-radius: 3px;
	}

	.error-type {
		font-size: 10px;
		color: var(--error-color);
		font-weight: 600;
	}

	.structured-logs {
		margin-top: 4px;
	}

	.log-line {
		display: flex;
		gap: 6px;
		padding: 1px 0;
		font-size: 10px;
		flex-wrap: wrap;
		align-items: baseline;
	}

	.field {
		color: var(--text-secondary);
		font-size: 10px;
	}

	.field-key {
		color: var(--purple-color);
	}

	.empty {
		color: var(--text-secondary);
		font-size: 11px;
		padding: 4px 0;
	}
</style>
