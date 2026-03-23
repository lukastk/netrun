<script lang="ts">
	import type { ObserveState } from '../types.js';

	interface Props {
		config: Record<string, unknown>;
		liveState: ObserveState | null;
	}

	let { config, liveState }: Props = $props();

	interface PoolInfo {
		name: string;
		type: string;
		workers: number | string;
		detail: string;
		nodes: string[];
	}

	let pools = $derived.by((): PoolInfo[] => {
		const result: PoolInfo[] = [];
		const poolConfigs = (config as any)?.pools as Record<string, any> | undefined;
		if (!poolConfigs) return result;

		// Build node→pool mapping
		const nodePoolMap = new Map<string, string[]>();
		const graphNodes = ((config as any)?.graph?.nodes ?? []) as any[];
		for (const node of graphNodes) {
			const pools = node?.execution_config?.pools ?? ['main'];
			for (const pool of pools) {
				if (!nodePoolMap.has(pool)) nodePoolMap.set(pool, []);
				nodePoolMap.get(pool)!.push(node.name);
			}
		}

		for (const [name, poolConfig] of Object.entries(poolConfigs)) {
			const spec = (poolConfig as any)?.spec ?? {};
			const type = spec.type ?? 'unknown';
			let workers: number | string = 1;
			let detail = '';

			if (type === 'thread') {
				workers = spec.num_workers ?? 1;
				detail = `${workers} worker threads`;
			} else if (type === 'multiprocess') {
				const procs = spec.num_processes ?? 1;
				const threads = spec.threads_per_process ?? 1;
				workers = procs * threads;
				detail = `${procs} processes × ${threads} threads`;
			} else if (type === 'remote') {
				workers = '?';
				detail = spec.url ?? 'remote';
			} else if (type === 'main') {
				workers = 1;
				detail = 'async event loop';
			}

			result.push({
				name,
				type,
				workers,
				detail,
				nodes: nodePoolMap.get(name) ?? [],
			});
		}
		return result;
	});

	let busyNodes = $derived(new Set(liveState?.status.busy_nodes ?? []));
</script>

<div class="pool-topology">
	{#if pools.length === 0}
		<div class="empty">No pools configured</div>
	{:else}
		{#each pools as pool}
			<div class="pool-card">
				<div class="pool-header">
					<span class="pool-name">{pool.name}</span>
					<span class="pool-type">{pool.type}</span>
					<span class="pool-workers">{pool.workers} workers</span>
				</div>
				<div class="pool-detail">{pool.detail}</div>
				<div class="pool-nodes">
					{#each pool.nodes as nodeName}
						<span class="node-tag" class:busy={busyNodes.has(nodeName)}>{nodeName}</span>
					{/each}
					{#if pool.nodes.length === 0}
						<span class="no-nodes">no assigned nodes</span>
					{/if}
				</div>
			</div>
		{/each}
	{/if}
</div>

<style>
	.pool-topology {
		height: 100%;
		overflow: auto;
		padding: 8px;
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.empty {
		padding: 16px;
		text-align: center;
		color: var(--text-secondary);
		font-size: 12px;
	}

	.pool-card {
		padding: 10px 12px;
		background: var(--bg-primary);
		border: 1px solid var(--border-color);
		border-radius: 6px;
	}

	.pool-header {
		display: flex;
		align-items: center;
		gap: 8px;
		margin-bottom: 4px;
	}

	.pool-name {
		font-weight: 700;
		font-size: 13px;
	}

	.pool-type {
		font-size: 10px;
		padding: 1px 5px;
		border-radius: 3px;
		background: rgba(59, 130, 246, 0.15);
		color: var(--accent-color);
		font-weight: 600;
		text-transform: uppercase;
	}

	.pool-workers {
		font-size: 11px;
		color: var(--text-secondary);
		margin-left: auto;
	}

	.pool-detail {
		font-size: 11px;
		color: var(--text-secondary);
		margin-bottom: 6px;
	}

	.pool-nodes {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
	}

	.node-tag {
		font-size: 10px;
		padding: 1px 6px;
		border-radius: 3px;
		background: var(--bg-tertiary);
		color: var(--text-primary);
	}

	.node-tag.busy {
		background: rgba(34, 197, 94, 0.15);
		color: var(--success-color);
	}

	.no-nodes {
		font-size: 10px;
		color: var(--text-secondary);
		font-style: italic;
	}
</style>
