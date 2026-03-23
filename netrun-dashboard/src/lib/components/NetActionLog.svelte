<script lang="ts">
	import type { NetActionEntry } from '../types.js';
	import { formatTimeMs, formatFieldValue } from '../format.js';

	interface Props {
		actions: NetActionEntry[];
	}

	let { actions }: Props = $props();

	let filterKind = $state('');
	let clearedAt = $state<string | null>(null);
	let expandedKey = $state<string | null>(null);

	// Sorting
	type SortKey = 'timestamp' | 'action_kind' | 'events' | 'epoch_id';
	let sortKey = $state<SortKey>('timestamp');
	let sortAsc = $state(false);

	function toggleSort(key: SortKey) {
		if (sortKey === key) {
			sortAsc = !sortAsc;
		} else {
			sortKey = key;
			sortAsc = key === 'action_kind';
		}
	}

	function sortIndicator(key: SortKey): string {
		if (sortKey !== key) return '';
		return sortAsc ? ' ▲' : ' ▼';
	}

	function actionKey(a: NetActionEntry): string {
		return `${a.timestamp}::${a.action_kind}::${a.epoch_id ?? ''}`;
	}

	let visible = $derived(
		clearedAt ? actions.filter((a) => a.timestamp > clearedAt!) : actions,
	);

	let sorted = $derived.by(() => {
		let arr = [...visible];
		if (filterKind) {
			arr = arr.filter((a) => a.action_kind.toLowerCase().includes(filterKind.toLowerCase()));
		}
		const dir = sortAsc ? 1 : -1;
		arr.sort((a, b) => {
			if (sortKey === 'events') return (a.events.length - b.events.length) * dir;
			const av = sortKey === 'epoch_id' ? (a.epoch_id ?? '') : (a as any)[sortKey];
			const bv = sortKey === 'epoch_id' ? (b.epoch_id ?? '') : (b as any)[sortKey];
			return String(av).localeCompare(String(bv)) * dir;
		});
		return arr;
	});

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
		<span class="count">{filterKind ? `${sorted.length} / ${visible.length}` : visible.length} actions</span>
		<button class="clear-btn" onclick={() => (clearedAt = new Date().toISOString())}>Clear</button>
	</div>
	<div class="action-list">
		{#if sorted.length === 0}
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
						<th class="sortable" onclick={() => toggleSort('timestamp')}>Time{sortIndicator('timestamp')}</th>
						<th class="sortable" onclick={() => toggleSort('action_kind')}>Action{sortIndicator('action_kind')}</th>
						<th>Detail</th>
						<th class="sortable" onclick={() => toggleSort('events')}>Events{sortIndicator('events')}</th>
						<th class="sortable" onclick={() => toggleSort('epoch_id')}>Epoch{sortIndicator('epoch_id')}</th>
					</tr>
				</thead>
				<tbody>
					{#each sorted as action (actionKey(action))}
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

	th.sortable {
		cursor: pointer;
		user-select: none;
	}

	th.sortable:hover {
		color: var(--text-primary);
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
