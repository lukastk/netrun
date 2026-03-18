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
	}

	let { config, liveState }: Props = $props();

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

	// Apply live status as CSS classes via DOM — no node replacement
	$effect(() => {
		if (!liveState || !initialized) return;

		const nodeStatusMap = new Map(liveState.nodes.map((n) => [n.name, n]));

		// Update node classes directly on DOM
		for (const node of initialNodes) {
			const status = nodeStatusMap.get(node.id);
			if (!status) continue;

			const el = document.querySelector(`[data-id="${node.id}"]`);
			if (!el) continue;

			el.classList.remove('node-disabled', 'node-busy', 'node-idle');
			if (!status.enabled) el.classList.add('node-disabled');
			else if (status.is_busy) el.classList.add('node-busy');
			else el.classList.add('node-idle');
		}

		// Update edge labels — find edge label elements and update text
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
	});
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
</style>
