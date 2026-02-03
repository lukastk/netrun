<script lang="ts">
	import {
		SvelteFlow,
		Background,
		Controls,
		MiniMap,
		type Edge,
		type Node,
		type Connection,
		type NodeTypes,
		BackgroundVariant,
		ConnectionLineType
	} from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import { derived } from 'svelte/store';

	import NetrunNodeComponent from './NetrunNode.svelte';
	import SubgraphNodeComponent from './SubgraphNode.svelte';
	import {
		nodes,
		edges,
		selectedNodeIds,
		selectedEdgeIds,
		addEdge as addEdgeToStore,
		generateEdgeId,
		deleteNodes,
		deleteEdges,
		pushHistory,
		updateNodePositions,
		type NetrunNodeData,
		type NetrunEdge
	} from '$lib/stores/flowStore';

	// Derive nodes and edges with selection state applied
	// This ensures SvelteFlow sees the correct selection even after data updates
	const nodesWithSelection = derived(
		[nodes, selectedNodeIds],
		([$nodes, $selectedNodeIds]) => $nodes.map(node => ({
			...node,
			selected: $selectedNodeIds.has(node.id)
		}))
	);

	const edgesWithSelection = derived(
		[edges, selectedEdgeIds],
		([$edges, $selectedEdgeIds]) => $edges.map(edge => ({
			...edge,
			selected: $selectedEdgeIds.has(edge.id)
		}))
	);

	// Register custom node types
	const nodeTypes: NodeTypes = {
		netrunNode: NetrunNodeComponent,
		subgraphNode: SubgraphNodeComponent
	};

	// Handle new connections
	function onConnect(connection: Connection) {
		if (connection.source && connection.target) {
			const newEdge: NetrunEdge = {
				id: generateEdgeId(),
				source: connection.source,
				target: connection.target,
				sourceHandle: connection.sourceHandle,
				targetHandle: connection.targetHandle,
				type: 'smoothstep',
				animated: false
			};
			addEdgeToStore(newEdge);
		}
	}

	// Handle deletion
	function onDelete(params: { nodes: Node[]; edges: Edge[] }) {
		if (params.nodes.length > 0) {
			deleteNodes(params.nodes.map(n => n.id));
		}
		if (params.edges.length > 0) {
			deleteEdges(params.edges.map(e => e.id));
		}
	}

	// Handle selection changes
	function onSelectionChange(params: { nodes: Node[]; edges: Edge[] }) {
		selectedNodeIds.set(new Set(params.nodes.map(n => n.id)));
		selectedEdgeIds.set(new Set(params.edges.map(e => e.id)));
	}

	// Handle node drag end - sync positions to store and push history
	function onNodeDragStop(event: { nodes: Node[] }) {
		// Update positions in our store
		const updates = event.nodes.map(node => ({
			id: node.id,
			position: node.position
		}));
		updateNodePositions(updates);
		pushHistory();
	}

	// Handle context menu on pane
	function onPaneContextMenu(event: { event: MouseEvent }) {
		event.event.preventDefault();
		// TODO: Show pane context menu
	}

	// Handle context menu on node
	function onNodeContextMenu(event: { node: Node; event: MouseEvent }) {
		event.event.preventDefault();
		// TODO: Show node context menu
	}
</script>

<div class="flow-editor">
	<SvelteFlow
		nodes={$nodesWithSelection}
		edges={$edgesWithSelection}
		{nodeTypes}
		onconnect={onConnect}
		ondelete={onDelete}
		onselectionchange={onSelectionChange}
		onnodedragstop={onNodeDragStop}
		onpanecontextmenu={onPaneContextMenu}
		onnodecontextmenu={onNodeContextMenu}
		fitView
		snapGrid={[15, 15]}
		defaultEdgeOptions={{
			type: 'smoothstep',
			animated: false
		}}
		connectionLineType={ConnectionLineType.SmoothStep}
		deleteKey="Delete"
		selectionKey="Shift"
		colorMode="dark"
	>
		<Background variant={BackgroundVariant.Dots} gap={20} size={1} />
		<Controls />
		<MiniMap
			nodeColor={(node) => {
				if (node.data?.nodeType === 'subgraph') return '#22c55e';
				if (node.data?.nodeType === 'factory') return '#7c3aed';
				return '#3b82f6';
			}}
			maskColor="rgba(0, 0, 0, 0.7)"
			pannable
			zoomable
		/>
	</SvelteFlow>
</div>

<style>
	.flow-editor {
		width: 100%;
		height: 100%;
	}

	/* Override SvelteFlow styles for dark mode */
	:global(.svelte-flow) {
		background: var(--bg-primary, #1a1a1a);
	}

	:global(.svelte-flow__background) {
		background: var(--bg-primary, #1a1a1a);
	}

	:global(.svelte-flow__edge-path) {
		stroke: var(--border-color, #404040);
		stroke-width: 2;
	}

	:global(.svelte-flow__edge.selected .svelte-flow__edge-path) {
		stroke: var(--accent-color, #3b82f6);
	}

	:global(.svelte-flow__controls) {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
	}

	:global(.svelte-flow__controls-button) {
		background: var(--bg-secondary, #242424);
		border-bottom: 1px solid var(--border-color, #404040);
		fill: var(--text-secondary, #a0a0a0);
	}

	:global(.svelte-flow__controls-button:hover) {
		background: var(--bg-tertiary, #2d2d2d);
	}

	:global(.svelte-flow__minimap) {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
	}
</style>
