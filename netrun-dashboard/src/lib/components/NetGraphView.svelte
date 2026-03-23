<script lang="ts">
	import { SvelteFlowProvider } from '@xyflow/svelte';
	import { NetrunFlowViewer, configToGraph, computeLayout } from 'netrun-ui-vis';
	import { NetrunNode, SubgraphNode, DecorationNode } from 'netrun-ui-vis/components';
	import type { NetrunGraph, FlowNode, NetrunEdge } from 'netrun-ui-vis';
	import type { NodeTypes } from '@xyflow/svelte';
	import type { ObserveState } from '../types.js';

	interface Props {
		config: Record<string, unknown>;
		liveState: ObserveState | null;
		highlightedNode: string | null;
		onNodeClick?: (nodeName: string) => void;
		onPaneClick?: (event: { event: MouseEvent }) => void;
	}

	let { config, liveState, highlightedNode, onNodeClick, onPaneClick }: Props = $props();

	const nodeTypes: NodeTypes = {
		netrunNode: NetrunNode,
		subgraphNode: SubgraphNode,
		decorationNode: DecorationNode,
	};

	// Convert config to graph (once, stable reference)
	let baseGraph = $derived(configToGraph(config));

	// Nodes/edges that we pass to SvelteFlow — set once, not replaced on WS ticks
	let initialNodes = $state<FlowNode[]>([]);
	let initialEdges = $state<NetrunEdge[]>([]);
	let graphSettings = $state<NetrunGraph['settings']>(undefined);
	let initialized = $state(false);

	// One-time init: compute layout if needed, then set initial nodes
	$effect(() => {
		const g = baseGraph;
		if (initialized) return;

		const needsLayout = g.nodes.length > 0 && g.nodes.every((n) => n.position.x === 0 && n.position.y === 0);
		if (needsLayout) {
			computeLayout(g.nodes, g.edges, 'layered-lr').then((result) => {
				const posMap = new Map(result.positions.map((p) => [p.id, p.position]));
				initialNodes = g.nodes.map((n) => {
					const pos = posMap.get(n.id);
					return pos ? { ...n, position: pos } : n;
				});
				initialEdges = g.edges;
				graphSettings = g.settings;
				initialized = true;
			});
		} else {
			initialNodes = g.nodes;
			initialEdges = g.edges;
			graphSettings = g.settings;
			initialized = true;
		}
	});

	// Tooltip element
	let tooltipEl: HTMLDivElement | undefined = $state();
	let nodeStatusCache = new Map<string, typeof liveState extends null ? never : NonNullable<typeof liveState>['nodes'][0]>();

	// One-time setup: attach hover listeners for tooltips
	$effect(() => {
		if (!initialized || !tooltipEl) return;

		const listeners: Array<[Element, string, EventListener]> = [];

		for (const node of initialNodes) {
			const el = document.querySelector(`[data-id="${node.id}"]`);
			if (!el) continue;

			const onEnter = (e: Event) => {
				const status = nodeStatusCache.get(node.id);
				if (!status || !tooltipEl) return;
				const stateLabel = !status.enabled ? 'disabled' : status.is_busy ? 'running' : 'idle';
				const running = status.running_epoch_ids.length;
				const startable = status.startable_epoch_ids.length;
				tooltipEl.innerHTML = `<strong>${status.name}</strong><br>` +
					`Status: ${stateLabel}<br>` +
					`Epochs: ${status.epoch_count}` +
					(running > 0 ? ` (${running} running)` : '') +
					(startable > 0 ? ` (${startable} startable)` : '') +
					(status.pools.length > 0 ? `<br>Pool: ${status.pools.join(', ')}` : '');
				const rect = (el as HTMLElement).getBoundingClientRect();
				tooltipEl.style.left = `${rect.left}px`;
				tooltipEl.style.top = `${rect.bottom + 6}px`;
				tooltipEl.style.display = 'block';
			};

			const onLeave = () => {
				if (tooltipEl) tooltipEl.style.display = 'none';
			};

			el.addEventListener('mouseenter', onEnter);
			el.addEventListener('mouseleave', onLeave);
			listeners.push([el, 'mouseenter', onEnter], [el, 'mouseleave', onLeave]);
		}

		return () => {
			for (const [el, event, fn] of listeners) {
				el.removeEventListener(event, fn);
			}
		};
	});

	// Apply live status + highlight + badges via DOM
	$effect(() => {
		if (!initialized) return;

		const nodeStatusMap = liveState
			? new Map(liveState.nodes.map((n) => [n.name, n]))
			: null;

		// Update cache for tooltips
		if (nodeStatusMap) nodeStatusCache = nodeStatusMap;

		for (const node of initialNodes) {
			const el = document.querySelector(`[data-id="${node.id}"]`);
			if (!el) continue;

			// Status classes
			el.classList.remove('node-disabled', 'node-busy', 'node-startable', 'node-idle');
			if (nodeStatusMap) {
				const status = nodeStatusMap.get(node.id);
				if (status) {
					if (!status.enabled) el.classList.add('node-disabled');
					else if (status.is_busy) el.classList.add('node-busy');
					else if (status.startable_epoch_ids.length > 0) el.classList.add('node-startable');
					else el.classList.add('node-idle');

					// Epoch badge
					let badge = el.querySelector('.epoch-badge') as HTMLElement | null;
					if (status.epoch_count > 0) {
						if (!badge) {
							badge = document.createElement('div');
							badge.className = 'epoch-badge';
							el.appendChild(badge);
						}
						badge.textContent = `${status.epoch_count}`;
					} else if (badge) {
						badge.remove();
					}
				}
			}

			// Highlight class
			el.classList.toggle('node-highlighted', node.id === highlightedNode);
		}

		// Update edge labels
		if (liveState) {
			const edgeStatusMap = new Map(
				liveState.edges.map((e) => [`${e.source_node}:${e.source_port}->${e.target_node}:${e.target_port}`, e]),
			);
			for (const edge of initialEdges) {
				const key = `${edge.data?.sourceNode}:${edge.data?.sourcePort}->${edge.data?.targetNode}:${edge.data?.targetPort}`;
				const status = edgeStatusMap.get(key);
				const el = document.querySelector(`[data-id="${edge.id}"] .svelte-flow__edge-text`);
				if (el) {
					el.textContent = status && status.packet_count > 0 ? `${status.packet_count}` : '';
				}
			}
		}
	});

	function handleNodeClick(event: { node: { id: string } }) {
		onNodeClick?.(event.node.id);
	}

	function handlePaneClick(event: { event: MouseEvent }) {
		onPaneClick?.(event);
	}
</script>

<div class="graph-container">
	<div class="dashboard-tooltip" bind:this={tooltipEl}></div>
	{#if initialized}
		<SvelteFlowProvider>
			<NetrunFlowViewer
				nodes={initialNodes}
				edges={initialEdges}
				{nodeTypes}
				settings={graphSettings}
				showMinimap={false}
				onNodeClick={handleNodeClick}
				onPaneClick={handlePaneClick}
				deleteKey={null}
			/>
		</SvelteFlowProvider>
	{/if}
</div>

<style>
	.graph-container {
		flex: 1;
		width: 100%;
		height: 100%;
		position: relative;
	}

	:global(.node-disabled) {
		opacity: 0.4;
	}

	:global(.node-busy) {
		outline: 2px solid var(--success-color, #22c55e);
		outline-offset: 2px;
		border-radius: 6px;
	}

	:global(.node-startable) {
		outline: 2px solid var(--warning-color, #f59e0b);
		outline-offset: 2px;
		border-radius: 6px;
	}

	:global(.node-highlighted) {
		box-shadow: 0 0 0 4px var(--accent-color, #3b82f6);
		border-radius: 6px;
	}

	:global(.epoch-badge) {
		position: absolute;
		top: -8px;
		right: -8px;
		background: var(--accent-color, #3b82f6);
		color: white;
		font-size: 9px;
		font-weight: 700;
		padding: 1px 5px;
		border-radius: 8px;
		z-index: 5;
		pointer-events: none;
		line-height: 14px;
	}

	.dashboard-tooltip {
		position: fixed;
		background: var(--bg-tertiary);
		border: 1px solid var(--border-color);
		padding: 8px 10px;
		border-radius: 4px;
		font-size: 11px;
		line-height: 1.5;
		z-index: 1000;
		pointer-events: none;
		display: none;
		max-width: 280px;
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
		color: var(--text-primary);
	}
</style>
