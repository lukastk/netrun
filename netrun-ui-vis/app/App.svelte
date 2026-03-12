<script lang="ts">
	import { SvelteFlowProvider } from '@xyflow/svelte';
	import NetrunFlowViewer from '../src/components/NetrunFlowViewer.svelte';
	import NetrunNodeComponent from '../src/components/NetrunNode.svelte';
	import SubgraphNodeComponent from '../src/components/SubgraphNode.svelte';
	import DecorationNodeComponent from '../src/components/DecorationNode.svelte';
	import type { NetrunGraph } from '../src/types/graph.js';
	import type { CascadeHighlightState } from '../src/types/events.js';
	import type { Node, Edge, NodeTypes } from '@xyflow/svelte';
	import { analyzeDependencyCascade, analyzeDependencyCascadeFromNode } from '../src/utils/dependencyAnalysis.js';

	interface Props {
		graph: NetrunGraph;
		showMinimap?: boolean;
	}

	let { graph, showMinimap = false }: Props = $props();

	const nodeTypes: NodeTypes = {
		netrunNode: NetrunNodeComponent,
		subgraphNode: SubgraphNodeComponent,
		decorationNode: DecorationNodeComponent,
	};

	let cascadeHighlight = $state<CascadeHighlightState | null>(null);

	function onNodeClick(event: { node: Node; event: MouseEvent | TouchEvent }) {
		const result = analyzeDependencyCascadeFromNode(event.node.id, graph.edges);
		cascadeHighlight = result ?? null;
	}

	function onEdgeClick(event: { edge: Edge; event: MouseEvent }) {
		const edge = event.edge;
		if ((edge.data as Record<string, unknown> | undefined)?.dependency) {
			cascadeHighlight = analyzeDependencyCascade(edge, graph.edges);
		} else {
			cascadeHighlight = null;
		}
	}

	function onSelectionChange() {
		// Allow clearing cascade by clicking empty pane (via NetrunFlowViewer internals)
	}

	function onPaneContextMenu() {
		cascadeHighlight = null;
	}

	function minimapNodeColor(node: Node): string {
		if (cascadeHighlight) {
			if (cascadeHighlight.sourceNodes.has(node.id)) return '#a78bfa';
			if (cascadeHighlight.visitedNodes.has(node.id)) return '#7c3aed';
		}
		if (node.data?.nodeType === 'decoration') return '#6b7280';
		const extra = ((node.data as Record<string, unknown>)?._config as Record<string, unknown> | undefined)?.extra as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		const headerColor = ui?.headerColor as string | undefined;
		if (headerColor) return headerColor;
		if (node.data?.nodeType === 'subgraph') return '#22c55e';
		return '#3d3d3d';
	}
</script>

<SvelteFlowProvider>
	<NetrunFlowViewer
		nodes={graph.nodes}
		edges={graph.edges}
		{nodeTypes}
		settings={graph.settings}
		{cascadeHighlight}
		{showMinimap}
		minimapNodeColor={showMinimap ? minimapNodeColor : undefined}
		{onNodeClick}
		{onEdgeClick}
		{onPaneContextMenu}
	/>
</SvelteFlowProvider>
