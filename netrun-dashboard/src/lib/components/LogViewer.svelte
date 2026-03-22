<script lang="ts">
	import type { LogEntry, EpochInfo } from '../types.js';
	import { formatTimeMs, formatFieldValue } from '../format.js';

	interface Props {
		logs: LogEntry[];
		epochs: EpochInfo[];
		onNodeHighlight?: (nodeName: string | null) => void;
	}

	let { logs, epochs, onNodeHighlight }: Props = $props();

	let filterNode = $state('');
	let autoScroll = $state(true);
	let listEl: HTMLDivElement | undefined = $state();
	let expandedIndex = $state<number | null>(null);

	interface UnifiedLog {
		timestamp: string;
		node_name: string | null;
		epoch_id: string | null;
		message: string;
		level?: string;
		fields?: Record<string, unknown>;
		isStructured: boolean;
	}

	let unified = $derived.by((): UnifiedLog[] => {
		const result: UnifiedLog[] = [];

		for (const log of logs) {
			result.push({
				timestamp: log.timestamp,
				node_name: log.node_name,
				epoch_id: log.epoch_id,
				message: log.message,
				isStructured: false,
			});
		}

		for (const epoch of epochs) {
			for (const entry of epoch.node_log_entries) {
				result.push({
					timestamp: entry.timestamp,
					node_name: epoch.node_name,
					epoch_id: epoch.epoch_id,
					message: entry.message ?? '',
					level: entry.level,
					fields: entry.fields,
					isStructured: true,
				});
			}
		}

		// Deduplicate: ctx.log() produces both a print buffer entry and a structured
		// entry with the same timestamp+node+epoch. Keep only the structured one.
		const structuredKeys = new Set(
			result
				.filter((r) => r.isStructured)
				.map((r) => `${r.timestamp}::${r.node_name}::${r.epoch_id}`),
		);
		const deduped = result.filter(
			(r) => r.isStructured || !structuredKeys.has(`${r.timestamp}::${r.node_name}::${r.epoch_id}`),
		);

		deduped.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
		return deduped;
	});

	let filtered = $derived(
		filterNode
			? unified.filter((l) => l.node_name?.toLowerCase().includes(filterNode.toLowerCase()))
			: unified,
	);

	function handleClick(log: UnifiedLog, index: number) {
		if (log.isStructured && log.fields && Object.keys(log.fields).length > 0) {
			expandedIndex = expandedIndex === index ? null : index;
		}
		if (log.node_name) {
			onNodeHighlight?.(log.node_name);
		}
	}

	// Auto-scroll to bottom when new logs arrive
	$effect(() => {
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
		<label class="toggle">
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
				<div
					class="log-entry"
					class:structured={log.isStructured}
					class:error={log.level === 'error'}
					class:expandable={log.isStructured && log.fields && Object.keys(log.fields).length > 0}
					onclick={() => handleClick(log, i)}
				>
					<span class="log-time">{formatTimeMs(log.timestamp)}</span>
					{#if log.node_name}
						<span class="log-node">{log.node_name}</span>
					{/if}
					<span class="log-message">{log.message}</span>
					{#if log.fields && Object.keys(log.fields).length > 0}
						<span class="field-indicator">&#9656; {Object.keys(log.fields).length} fields</span>
					{/if}
				</div>
				{#if expandedIndex === i && log.fields}
					<div class="field-detail">
						{#each Object.entries(log.fields) as [k, v]}
							<div class="field-row">
								<span class="field-key">{k}</span>
								<span class="field-value">{formatFieldValue(v)}</span>
							</div>
						{/each}
					</div>
				{/if}
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

	.toggle {
		display: flex;
		align-items: center;
		gap: 4px;
		font-size: 11px;
		color: var(--text-secondary);
		cursor: pointer;
		user-select: none;
	}

	.toggle input {
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
		align-items: baseline;
	}

	.log-entry:hover {
		background: var(--bg-tertiary);
	}

	.log-entry.expandable {
		cursor: pointer;
	}

	.log-entry.error {
		color: var(--error-color);
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

	.field-indicator {
		color: var(--text-secondary);
		font-size: 10px;
		flex-shrink: 0;
	}

	.field-detail {
		padding: 4px 8px 4px 100px;
		background: var(--bg-primary);
		border-bottom: 1px solid var(--border-color);
	}

	.field-row {
		display: flex;
		gap: 8px;
		font-size: 11px;
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
