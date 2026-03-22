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
	}

	let { config, liveState, highlightedNode, onNodeClick }: Props = $props();

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

	// Apply live status + highlight as CSS classes via DOM
	$effect(() => {
		if (!initialized) return;

		const nodeStatusMap = liveState
			? new Map(liveState.nodes.map((n) => [n.name, n]))
			: null;

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
</script>

<div class="graph-container">
	{#if initialized}
		<SvelteFlowProvider>
			<NetrunFlowViewer
				nodes={initialNodes}
				edges={initialEdges}
				{nodeTypes}
				settings={graphSettings}
				showMinimap={false}
				onNodeClick={handleNodeClick}
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
		outline: 2px solid var(--accent-color, #3b82f6);
		outline-offset: 2px;
		border-radius: 6px;
	}
</style>
