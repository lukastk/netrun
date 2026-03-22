<script lang="ts">
	import type { ObserveState } from '../types.js';

	interface Props {
		name: string;
		state: ObserveState | null;
		connected: boolean;
	}

	let { name, state, connected }: Props = $props();

	const statusLabel = $derived.by(() => {
		if (!connected || !state) return 'disconnected';
		if (state.status.paused) return 'paused';
		if (state.status.is_blocked) return 'blocked';
		if (state.status.started) return 'running';
		return 'stopped';
	});

	const statusClass = $derived.by(() => {
		if (!connected || !state) return 'disconnected';
		if (state.status.paused) return 'paused';
		if (state.status.is_blocked) return 'blocked';
		if (state.status.started) return 'running';
		return 'stopped';
	});
</script>

<div class="status-bar">
	<span class="net-name">{name}</span>
	<span class="badge {statusClass}">{statusLabel}</span>
	{#if state}
		<span class="stat">{state.status.node_names.length} nodes</span>
		<span class="stat">{state.status.total_epochs} epochs</span>
		<span class="stat">{state.status.busy_nodes.length} busy</span>
		{#if state.status.dead_letter_count > 0}
			<span class="stat stat-error">{state.status.dead_letter_count} dead letters</span>
		{/if}
		{#if state.status.exception_count > 0}
			<span class="stat stat-error">{state.status.exception_count} exceptions</span>
		{/if}
	{/if}
</div>

<style>
	.status-bar {
		display: flex;
		align-items: center;
		gap: 12px;
		padding: 8px 16px;
		background: var(--bg-secondary);
		border-bottom: 1px solid var(--border-color);
		font-size: 13px;
	}

	.net-name {
		font-weight: 600;
	}

	.badge {
		padding: 2px 8px;
		border-radius: 3px;
		font-size: 11px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.05em;
	}

	.badge.running {
		background: rgba(34, 197, 94, 0.15);
		color: var(--success-color);
	}

	.badge.paused {
		background: rgba(245, 158, 11, 0.15);
		color: var(--warning-color);
	}

	.badge.blocked {
		background: rgba(245, 158, 11, 0.15);
		color: var(--warning-color);
	}

	.badge.stopped {
		background: rgba(160, 160, 160, 0.15);
		color: var(--text-secondary);
	}

	.badge.disconnected {
		background: rgba(239, 68, 68, 0.15);
		color: var(--error-color);
	}

	.stat {
		color: var(--text-secondary);
		font-size: 12px;
	}

	.stat-error {
		color: var(--error-color);
	}
</style>
