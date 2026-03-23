<script lang="ts">
	import type { NetActionEntry } from '../types.js';
	import { formatTimeMs, formatFieldValue } from '../format.js';

	interface Props {
		actions: NetActionEntry[];
	}

	let { actions }: Props = $props();

	let sorted = $derived([...actions].reverse());
	let filterKind = $state('');
	let expandedKey = $state<string | null>(null);

	function actionKey(a: NetActionEntry): string {
		return `${a.timestamp}::${a.action_kind}::${a.epoch_id ?? ''}`;
	}

	let filtered = $derived(
		filterKind
			? sorted.filter((a) => a.action_kind.toLowerCase().includes(filterKind.toLowerCase()))
			: sorted,
	);

	function summarizeDetail(detail: Record<string, unknown>): string {
		const parts: string[] = [];
		if (detail.node_name) parts.push(String(detail.node_name));
		if (detail.packet_id) parts.push(`pkt:${String(detail.packet_id).slice(0, 8)}`);
		if (detail.epoch_id) parts.push(`epoch:${String(detail.epoch_id).slice(0, 8)}`);
		return parts.join(' ') || '-';
	}
</script>

<div class="action-log">
	<div class="toolbar">
		<input
			type="text"
			placeholder="Filter by action kind..."
			bind:value={filterKind}
			class="filter-input"
		/>
		<span class="count">{filterKind ? `${filtered.length} / ${actions.length}` : actions.length} actions</span>
	</div>
	<div class="action-list">
		{#if filtered.length === 0}
			<div class="empty">
				{#if actions.length === 0}
					No net actions recorded. Ensure retain_net_action_logs is enabled.
				{:else}
					No actions match filter.
				{/if}
			</div>
		{:else}
			<table>
				<thead>
					<tr>
						<th>Time</th>
						<th>Action</th>
						<th>Detail</th>
						<th>Events</th>
						<th>Epoch</th>
					</tr>
				</thead>
				<tbody>
					{#each filtered as action (actionKey(action))}
						{@const key = actionKey(action)}
						<tr
							class="action-row"
							class:expanded={expandedKey === key}
							onclick={() => (expandedKey = expandedKey === key ? null : key)}
						>
							<td class="mono">{formatTimeMs(action.timestamp)}</td>
							<td><span class="action-kind">{action.action_kind}</span></td>
							<td class="detail-summary">{summarizeDetail(action.action_detail)}</td>
							<td class="mono">{action.events.length}</td>
							<td class="mono">{action.epoch_id?.slice(0, 8) ?? '-'}</td>
						</tr>
						{#if expandedKey === key}
							<tr class="detail-row">
								<td colspan="5">
									<div class="expanded-detail">
										{#if Object.keys(action.action_detail).length > 0}
											<div class="detail-section">
												<span class="detail-title">Action Detail</span>
												{#each Object.entries(action.action_detail) as [k, v]}
													<div class="kv"><span class="key">{k}</span> {formatFieldValue(v)}</div>
												{/each}
											</div>
										{/if}
										{#if action.events.length > 0}
											<div class="detail-section">
												<span class="detail-title">Events ({action.events.length})</span>
												{#each action.events as event}
													<div class="event-row">
														<span class="event-kind">{event.kind}</span>
														{#each Object.entries(event.detail) as [k, v]}
															<span class="kv"><span class="key">{k}</span>={formatFieldValue(v)}</span>
														{/each}
													</div>
												{/each}
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
</div>

<style>
	.action-log {
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
		width: 200px;
		padding: 3px 8px;
		font-size: 11px;
	}

	.count {
		margin-left: auto;
		font-size: 11px;
		color: var(--text-secondary);
	}

	.action-list {
		flex: 1;
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
		font-size: 11px;
	}

	thead {
		position: sticky;
		top: 0;
		z-index: 1;
	}

	th {
		background: var(--bg-tertiary);
		padding: 5px 8px;
		text-align: left;
		font-weight: 600;
		color: var(--text-secondary);
		font-size: 10px;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		border-bottom: 1px solid var(--border-color);
	}

	td {
		padding: 3px 8px;
		border-bottom: 1px solid var(--border-color);
		white-space: nowrap;
	}

	.action-row {
		cursor: pointer;
	}

	.action-row:hover {
		background: var(--bg-tertiary);
	}

	.action-row.expanded {
		background: var(--bg-tertiary);
	}

	.mono {
		font-variant-numeric: tabular-nums;
	}

	.action-kind {
		font-weight: 600;
		color: var(--accent-color);
	}

	.detail-summary {
		color: var(--text-secondary);
		max-width: 200px;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.detail-row td {
		padding: 0;
	}

	.expanded-detail {
		padding: 6px 8px;
		background: var(--bg-primary);
		font-size: 10px;
	}

	.detail-section {
		margin-bottom: 6px;
	}

	.detail-title {
		font-weight: 600;
		color: var(--text-secondary);
		text-transform: uppercase;
		font-size: 9px;
		letter-spacing: 0.05em;
		display: block;
		margin-bottom: 2px;
	}

	.kv {
		padding: 1px 0;
	}

	.key {
		color: var(--purple-color);
	}

	.event-row {
		display: flex;
		gap: 8px;
		padding: 1px 0;
		flex-wrap: wrap;
	}

	.event-kind {
		font-weight: 600;
		color: var(--text-primary);
	}
</style>
