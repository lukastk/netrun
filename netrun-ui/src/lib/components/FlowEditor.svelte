<script lang="ts">
	import {
		SvelteFlow,
		Background,
		Controls,
		MiniMap,
		MarkerType,
		useSvelteFlow,
		type Edge,
		type Node,
		type Connection,
		type NodeTypes,
		BackgroundVariant,
		ConnectionLineType
	} from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import { tick, onMount, onDestroy } from 'svelte';
	import { derived, get } from 'svelte/store';
	import { isValidConnection, activeTabId, panOnDrag, selectionOnDrag } from '$lib/stores/flowStore';
	import { svelteFlowRef } from '$lib/stores/svelteFlowStore';

	import NetrunNodeComponent from './NetrunNode.svelte';
	import SubgraphNodeComponent from './SubgraphNode.svelte';
	import DecorationNodeComponent from './DecorationNode.svelte';
	import {
		nodes,
		edges,
		selectedNodeIds,
		selectedEdgeIds,
		addEdge as addEdgeToStore,
		addEdges as addEdgesToStore,
		generateEdgeId,
		deleteNodes,
		deleteEdges,
		pushHistory,
		updateNodePositions,
		graphExtra,
		isExpandedChildNode,
		getParentSubgraphId,
		type NetrunNodeData,
		type NetrunEdge
	} from '$lib/stores/flowStore';
	import {
		expandedView,
		updateExpandedChildPosition,
		deleteExpandedChildren,
		addExpandedChildEdge,
		cleanupExpandedSubgraph,
	} from '$lib/stores/subgraphExpandStore';
	import {
		isGroupHandle,
		parseGroupHandleId,
		getGroupConnectionPairs,
		getPortGroupPath,
		getGroupPortNames,
		ROOT_GROUP_PATH,
	} from '$lib/utils/portGroups';
	import { isPortGroupCollapsed, getPortGroupStates } from '$lib/stores/portGroupStore';

	// Get edge style from graph extra, defaulting to smoothstep
	const edgeStyle = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.edgeStyle as string) ?? 'smoothstep';
	});

	// Get edge markers setting from graph extra
	const edgeMarkers = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.edgeMarkers as string) ?? 'arrow-end';
	});

	// Zoom limits from graph extra
	const minZoom = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.minZoom as number) ?? 0.05;
	});

	const maxZoom = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.maxZoom as number) ?? 4;
	});

	// Node font sizes from graph extra
	const nodeTitleFontSize = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.nodeTitleFontSize as number) ?? 12;
	});

	const nodeDescFontSize = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.nodeDescFontSize as number) ?? 10;
	});

	const nodePortFontSize = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.nodePortFontSize as number) ?? 11;
	});

	// Arrow marker configuration
	const arrowMarker = {
		type: MarkerType.ArrowClosed,
		width: 20,
		height: 20,
		color: 'var(--border-color, #404040)',
	};

	// Get marker config based on setting
	function getMarkers(setting: string): { markerStart?: typeof arrowMarker; markerEnd?: typeof arrowMarker } {
		switch (setting) {
			case 'arrow-start':
				return { markerStart: arrowMarker };
			case 'arrow-both':
				return { markerStart: arrowMarker, markerEnd: arrowMarker };
			case 'none':
				return {};
			case 'arrow-end':
			default:
				return { markerEnd: arrowMarker };
		}
	}

	// Derive nodes and edges with selection state applied
	// Uses expandedView which includes child nodes from expanded subgraphs
	const nodesWithSelection = derived(
		[expandedView, selectedNodeIds],
		([{ allNodes }, $selectedNodeIds]) => allNodes.map(node => ({
			...node,
			selected: $selectedNodeIds.has(node.id)
		}))
	);

	const edgesWithSelection = derived(
		[expandedView, selectedEdgeIds, edgeStyle, edgeMarkers],
		([{ allEdges }, $selectedEdgeIds, $edgeStyle, $edgeMarkers]) => {
			const markers = getMarkers($edgeMarkers);
			return allEdges.map(edge => ({
				...edge,
				type: $edgeStyle,
				markerStart: markers.markerStart,
				markerEnd: markers.markerEnd,
				selected: $selectedEdgeIds.has(edge.id)
			}));
		}
	);

	// Register custom node types
	const nodeTypes: NodeTypes = {
		netrunNode: NetrunNodeComponent,
		subgraphNode: SubgraphNodeComponent,
		decorationNode: DecorationNodeComponent,
	};

	/**
	 * Intercept connections before they are created.
	 * Handles group-to-group connections by expanding into multiple edges.
	 */
	function onBeforeConnect(connection: Connection): Connection | false {
		const srcGroup = parseGroupHandleId(connection.sourceHandle);
		const tgtGroup = parseGroupHandleId(connection.targetHandle);

		// If neither handle is a group, pass through normally
		if (!srcGroup && !tgtGroup) return connection;

		// If only one is a group, reject (group-to-individual not allowed)
		if (!srcGroup || !tgtGroup) return false;

		// Both are groups — expand to individual edges
		if (!connection.source || !connection.target) return false;

		const currentNodes = get(nodes);
		const sourceNode = currentNodes.find(n => n.id === connection.source);
		const targetNode = currentNodes.find(n => n.id === connection.target);
		if (!sourceNode || !targetNode) return false;

		const pairs = getGroupConnectionPairs(
			sourceNode, srcGroup.groupPath,
			targetNode, tgtGroup.groupPath,
		);

		if (pairs.length === 0) return false;

		const markers = getMarkers(get(edgeMarkers));
		const style = get(edgeStyle);
		const newEdges: NetrunEdge[] = pairs.map(([srcPort, tgtPort]) => ({
			id: generateEdgeId(),
			source: connection.source!,
			target: connection.target!,
			sourceHandle: srcPort,
			targetHandle: tgtPort,
			type: style,
			animated: false,
			...markers,
		}));

		addEdgesToStore(newEdges);
		return false; // Suppress default single edge creation
	}

	// Handle new connections (for normal single-port connections)
	function onConnect(connection: Connection) {
		if (!connection.source || !connection.target) return;

		const srcIsChild = isExpandedChildNode(connection.source);
		const tgtIsChild = isExpandedChildNode(connection.target);

		if (srcIsChild && tgtIsChild) {
			// Both are children — must be in same subgraph
			const srcParent = getParentSubgraphId(connection.source);
			const tgtParent = getParentSubgraphId(connection.target);
			if (srcParent !== tgtParent) return; // Cross-subgraph not allowed

			addExpandedChildEdge({
				source: connection.source,
				target: connection.target,
				sourceHandle: connection.sourceHandle,
				targetHandle: connection.targetHandle,
			});
			return;
		}

		if (srcIsChild || tgtIsChild) {
			// Mixed: one child, one parent-level — not allowed (must use subgraph ports)
			return;
		}

		// Normal parent-level connection
		const markers = getMarkers(get(edgeMarkers));
		const newEdge: NetrunEdge = {
			id: generateEdgeId(),
			source: connection.source,
			target: connection.target,
			sourceHandle: connection.sourceHandle,
			targetHandle: connection.targetHandle,
			type: get(edgeStyle),
			animated: false,
			...markers
		};
		addEdgeToStore(newEdge);
	}

	// Handle deletion
	// When deleting an edge whose ports on both sides belong to collapsed groups,
	// delete all sibling edges between those two groups.
	function onDelete(params: { nodes: Node[]; edges: Edge[] }) {
		// Separate child nodes/edges from parent-level ones
		const childNodeIds = params.nodes.filter(n => isExpandedChildNode(n.id)).map(n => n.id);
		const parentNodeIds = params.nodes.filter(n => !isExpandedChildNode(n.id)).map(n => n.id);
		const childEdgeIds = params.edges.filter(e => isExpandedChildNode(e.id)).map(e => e.id);
		const parentEdges = params.edges.filter(e => !isExpandedChildNode(e.id));

		// Handle child deletions via expand store
		if (childNodeIds.length > 0 || childEdgeIds.length > 0) {
			deleteExpandedChildren(childNodeIds, childEdgeIds);
		}

		// Handle parent-level node deletions
		if (parentNodeIds.length > 0) {
			// Clean up expansion state for any deleted subgraph nodes
			for (const id of parentNodeIds) {
				cleanupExpandedSubgraph(id);
			}
			deleteNodes(parentNodeIds);
		}

		if (parentEdges.length > 0) {
			const edgeIdsToDelete = new Set<string>(parentEdges.map(e => e.id));
			const currentNodes = get(nodes);
			const currentEdges = get(edges);

			for (const edge of parentEdges) {
				if (!edge.sourceHandle || !edge.targetHandle) continue;

				const srcNode = currentNodes.find(n => n.id === edge.source);
				const tgtNode = currentNodes.find(n => n.id === edge.target);
				if (!srcNode || !tgtNode) continue;

				// Read port group states from node data
				const srcPortGroupStates = getPortGroupStates(srcNode.data as Record<string, unknown>);
				const tgtPortGroupStates = getPortGroupStates(tgtNode.data as Record<string, unknown>);

				// Check if root group is collapsed on each side first
				const srcRootCollapsed = isPortGroupCollapsed(srcPortGroupStates, 'out', ROOT_GROUP_PATH, srcNode.data.outPorts.length);
				const tgtRootCollapsed = isPortGroupCollapsed(tgtPortGroupStates, 'in', ROOT_GROUP_PATH, tgtNode.data.inPorts.length);

				// Determine effective group path: root if root is collapsed, otherwise the port's sub-group
				const srcGroupPath = srcRootCollapsed
					? ROOT_GROUP_PATH
					: getPortGroupPath(edge.sourceHandle);
				const tgtGroupPath = tgtRootCollapsed
					? ROOT_GROUP_PATH
					: getPortGroupPath(edge.targetHandle);

				if (!srcGroupPath || !tgtGroupPath) continue;

				// For non-root groups, also check if the sub-group itself is collapsed
				const srcCollapsed = srcRootCollapsed || isPortGroupCollapsed(srcPortGroupStates, 'out', srcGroupPath,
					srcNode.data.outPorts.filter(p => p.name.startsWith(srcGroupPath + '.')).length);
				const tgtCollapsed = tgtRootCollapsed || isPortGroupCollapsed(tgtPortGroupStates, 'in', tgtGroupPath,
					tgtNode.data.inPorts.filter(p => p.name.startsWith(tgtGroupPath + '.')).length);

				if (srcCollapsed && tgtCollapsed) {
					// Both groups collapsed — delete all edges between these two groups
					const srcPortNames = new Set(getGroupPortNames(srcGroupPath, srcNode.data.outPorts));
					const tgtPortNames = new Set(getGroupPortNames(tgtGroupPath, tgtNode.data.inPorts));

					for (const e of currentEdges) {
						if (e.source === edge.source && e.target === edge.target &&
							e.sourceHandle && e.targetHandle &&
							srcPortNames.has(e.sourceHandle) && tgtPortNames.has(e.targetHandle)) {
							edgeIdsToDelete.add(e.id);
						}
					}
				}
			}

			deleteEdges([...edgeIdsToDelete]);
		}
	}

	// Handle selection changes
	function onSelectionChange(params: { nodes: Node[]; edges: Edge[] }) {
		selectedNodeIds.set(new Set(params.nodes.map(n => n.id)));
		selectedEdgeIds.set(new Set(params.edges.map(e => e.id)));
	}

	// Handle node drag end - sync positions to store and push history
	function onNodeDragStop(event: { nodes: Node[] }) {
		// Separate child nodes from parent-level nodes
		const childNodes = event.nodes.filter(n => isExpandedChildNode(n.id));
		const parentNodes = event.nodes.filter(n => !isExpandedChildNode(n.id));

		// Update child node positions via expand store
		for (const node of childNodes) {
			updateExpandedChildPosition(node.id, node.position);
		}

		// Update parent-level positions in our store
		if (parentNodes.length > 0) {
			const updates = parentNodes.map(node => ({
				id: node.id,
				position: node.position
			}));
			updateNodePositions(updates);
		}

		pushHistory();
	}

	// Handle context menu on pane
	function onPaneContextMenu(event: { event: MouseEvent }) {
		event.event.preventDefault();
	}

	// Handle context menu on node
	function onNodeContextMenu(event: { node: Node; event: MouseEvent }) {
		event.event.preventDefault();
	}

	// Double-click detection for nodes
	let lastClickTime = 0;
	let lastClickedNodeId: string | null = null;
	const DOUBLE_CLICK_THRESHOLD = 300; // ms

	// Handle click on node (with double-click detection)
	async function onNodeClick(event: { node: Node; event: MouseEvent | TouchEvent }) {
		const now = Date.now();
		const nodeId = event.node.id;

		// Check for double-click
		if (lastClickedNodeId === nodeId && (now - lastClickTime) < DOUBLE_CLICK_THRESHOLD) {
			// Double-click detected - focus name input
			if (event.node.type !== 'subgraphNode') {
				await tick();
				const input = document.getElementById('node-label') as HTMLInputElement | null;
				if (input) {
					input.focus();
					input.select();
				}
			}
			// Reset to prevent triple-click triggering again
			lastClickedNodeId = null;
			lastClickTime = 0;
		} else {
			// Single click - record for potential double-click
			lastClickedNodeId = nodeId;
			lastClickTime = now;
		}
	}

	// Map edge style to connection line type
	function getConnectionLineType(style: string): ConnectionLineType {
		switch (style) {
			case 'straight': return ConnectionLineType.Straight;
			case 'step': return ConnectionLineType.Step;
			case 'default': return ConnectionLineType.Bezier;
			default: return ConnectionLineType.SmoothStep;
		}
	}

	// Get SvelteFlow instance for programmatic control
	const { fitView, getNodes } = useSvelteFlow();

	// Expose SvelteFlow methods outside the provider boundary
	onMount(() => {
		svelteFlowRef.set({ fitView, getNodes });
	});
	onDestroy(() => {
		svelteFlowRef.set(null);
	});

	// Fit view options - ensure entire graph is visible with padding
	const fitViewOptions = {
		padding: 0.2,
		maxZoom: 1.5,
		minZoom: 0.1,
	};

	// Track previous tab ID to detect tab switches
	let previousTabId: string | null = null;

	// Fit view when switching tabs or loading new content
	$: if ($activeTabId && $activeTabId !== previousTabId) {
		previousTabId = $activeTabId;
		// Wait for nodes to render, then fit view
		tick().then(() => {
			fitView(fitViewOptions);
		});
	}
</script>

<div class="flow-editor" style:--node-title-font-size="{$nodeTitleFontSize}px" style:--node-desc-font-size="{$nodeDescFontSize}px" style:--node-port-font-size="{$nodePortFontSize}px">
	<SvelteFlow
		nodes={$nodesWithSelection}
		edges={$edgesWithSelection}
		{nodeTypes}
		onbeforeconnect={onBeforeConnect}
		onconnect={onConnect}
		ondelete={onDelete}
		onselectionchange={onSelectionChange}
		onnodedragstop={onNodeDragStop}
		onpanecontextmenu={onPaneContextMenu}
		onnodecontextmenu={onNodeContextMenu}
		onnodeclick={onNodeClick}
		{isValidConnection}
		fitView
		{fitViewOptions}
		snapGrid={[15, 15]}
		defaultEdgeOptions={{
			type: $edgeStyle,
			animated: false,
			...getMarkers($edgeMarkers),
			style: 'stroke-width: 2px;'
		}}
		connectionLineType={getConnectionLineType($edgeStyle)}
		deleteKey={['Delete', 'Backspace']}
		panOnDrag={$panOnDrag}
		selectionOnDrag={$selectionOnDrag}
		selectionKey="Shift"
		colorMode="dark"
		minZoom={$minZoom}
		maxZoom={$maxZoom}
	>
		<Background variant={BackgroundVariant.Dots} gap={20} size={1} />
		<Controls />
		<MiniMap
			nodeColor={(node) => {
				if (node.data?.nodeType === 'decoration') return '#6b7280';
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

	/* Arrow marker styling */
	:global(.svelte-flow__edge-path + marker path) {
		fill: var(--border-color, #404040);
	}

	:global(.svelte-flow__edge.selected marker path) {
		fill: var(--accent-color, #3b82f6);
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
