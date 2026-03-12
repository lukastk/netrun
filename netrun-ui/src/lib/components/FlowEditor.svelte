<script lang="ts">
	import { NetrunFlowViewer } from 'netrun-ui-vis/components';
	import type { ContextMenuItem, GraphSettings } from 'netrun-ui-vis';
	import { tick } from 'svelte';
	import { derived, get } from 'svelte/store';
	import type { Edge, Node, Connection, NodeTypes, FitViewOptions } from '@xyflow/svelte';
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
		isExposedPortEdge,
		getParentSubgraphId,
		cascadeHighlight,
		toggleEdgeDependency,
		getNodeLocked,
		updateNodeLocked,
		sendNodesToFront,
		sendNodesToBack,
		type NetrunNodeData,
		type NetrunEdgeData,
		type NetrunEdge,
		type AnyNodeData,
	} from '$lib/stores/flowStore';
	import { analyzeDependencyCascade, analyzeDependencyCascadeFromNode } from '$lib/utils/dependencyAnalysis';
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

	// Register custom node types
	const nodeTypes: NodeTypes = {
		netrunNode: NetrunNodeComponent,
		subgraphNode: SubgraphNodeComponent,
		decorationNode: DecorationNodeComponent,
	};

	// Derive graph settings from graphExtra
	const graphSettings = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return {
			edgeStyle: (ui?.edgeStyle as GraphSettings['edgeStyle']) ?? undefined,
			edgeMarkers: (ui?.edgeMarkers as GraphSettings['edgeMarkers']) ?? undefined,
			minZoom: (ui?.minZoom as number) ?? undefined,
			maxZoom: (ui?.maxZoom as number) ?? undefined,
			nodeTitleFontSize: (ui?.nodeTitleFontSize as number) ?? undefined,
			nodeDescFontSize: (ui?.nodeDescFontSize as number) ?? undefined,
			nodePortFontSize: (ui?.nodePortFontSize as number) ?? undefined,
		} satisfies GraphSettings;
	});

	// Edge style/markers for connection creation (used in onBeforeConnect/onConnect)
	const edgeStyle = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.edgeStyle as string) ?? 'smoothstep';
	});
	const edgeMarkers = derived(graphExtra, ($graphExtra) => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return (ui?.edgeMarkers as string) ?? 'arrow-end';
	});

	// Derive nodes with selection state applied
	const nodesWithSelection = derived(
		[expandedView, selectedNodeIds],
		([{ allNodes }, $selectedNodeIds]) => allNodes.map(node => {
			const locked = getNodeLocked(node.data);
			return {
				...node,
				selected: $selectedNodeIds.has(node.id),
				...(locked ? { draggable: false } : {}),
			};
		})
	);

	// Derive edges with selection state
	const edgesWithSelection = derived(
		[expandedView, selectedEdgeIds],
		([{ allEdges }, $selectedEdgeIds]) => allEdges.map(edge => ({
			...edge,
			selected: $selectedEdgeIds.has(edge.id),
		}))
	);

	// Arrow marker config (reused in onBeforeConnect/onConnect for new edges)
	import { MarkerType } from '@xyflow/svelte';
	const arrowMarker = { type: MarkerType.ArrowClosed, width: 20, height: 20, color: 'var(--border-color, #404040)' };
	function getMarkers(setting: string, marker = arrowMarker): { markerStart?: typeof arrowMarker; markerEnd?: typeof arrowMarker } {
		switch (setting) {
			case 'arrow-start': return { markerStart: marker };
			case 'arrow-both': return { markerStart: marker, markerEnd: marker };
			case 'none': return {};
			default: return { markerEnd: marker };
		}
	}

	/**
	 * Intercept connections before they are created.
	 * Handles group-to-group connections by expanding into multiple edges.
	 */
	function onBeforeConnect(connection: Connection): Connection | false {
		const srcGroup = parseGroupHandleId(connection.sourceHandle);
		const tgtGroup = parseGroupHandleId(connection.targetHandle);

		if (!srcGroup && !tgtGroup) return connection;
		if (!srcGroup || !tgtGroup) return false;

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
		return false;
	}

	// Handle new connections (for normal single-port connections)
	function onConnect(connection: Connection) {
		if (!connection.source || !connection.target) return;

		const srcIsChild = isExpandedChildNode(connection.source);
		const tgtIsChild = isExpandedChildNode(connection.target);

		if (srcIsChild && tgtIsChild) {
			const srcParent = getParentSubgraphId(connection.source);
			const tgtParent = getParentSubgraphId(connection.target);
			if (srcParent !== tgtParent) return;

			addExpandedChildEdge({
				source: connection.source,
				target: connection.target,
				sourceHandle: connection.sourceHandle,
				targetHandle: connection.targetHandle,
			});
			return;
		}

		if (srcIsChild || tgtIsChild) return;

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

	// Handle deletion with group edge expansion
	function onDelete(params: { nodes: Node[]; edges: Edge[] }) {
		const childNodeIds = params.nodes.filter(n => isExpandedChildNode(n.id)).map(n => n.id);
		const parentNodeIds = params.nodes.filter(n => !isExpandedChildNode(n.id)).map(n => n.id);
		const childEdgeIds = params.edges.filter(e => isExpandedChildNode(e.id) && !isExposedPortEdge(e.id)).map(e => e.id);
		const parentEdges = params.edges.filter(e => !isExpandedChildNode(e.id));

		if (childNodeIds.length > 0 || childEdgeIds.length > 0) {
			deleteExpandedChildren(childNodeIds, childEdgeIds);
		}

		if (parentNodeIds.length > 0) {
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

				const srcPortGroupStates = getPortGroupStates(srcNode.data as Record<string, unknown>);
				const tgtPortGroupStates = getPortGroupStates(tgtNode.data as Record<string, unknown>);

				const srcRootCollapsed = isPortGroupCollapsed(srcPortGroupStates, 'out', ROOT_GROUP_PATH, srcNode.data.outPorts.length);
				const tgtRootCollapsed = isPortGroupCollapsed(tgtPortGroupStates, 'in', ROOT_GROUP_PATH, tgtNode.data.inPorts.length);

				const srcGroupPath = srcRootCollapsed
					? ROOT_GROUP_PATH
					: getPortGroupPath(edge.sourceHandle);
				const tgtGroupPath = tgtRootCollapsed
					? ROOT_GROUP_PATH
					: getPortGroupPath(edge.targetHandle);

				if (!srcGroupPath || !tgtGroupPath) continue;

				const srcCollapsed = srcRootCollapsed || isPortGroupCollapsed(srcPortGroupStates, 'out', srcGroupPath,
					srcNode.data.outPorts.filter(p => p.name.startsWith(srcGroupPath + '.')).length);
				const tgtCollapsed = tgtRootCollapsed || isPortGroupCollapsed(tgtPortGroupStates, 'in', tgtGroupPath,
					tgtNode.data.inPorts.filter(p => p.name.startsWith(tgtGroupPath + '.')).length);

				if (srcCollapsed && tgtCollapsed) {
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
		if (params.nodes.length === 0 && params.edges.length === 0) {
			cascadeHighlight.set(null);
		}
	}

	// Handle node drag end
	function onNodeDragStop(event: { nodes: Node[] }) {
		const childNodes = event.nodes.filter(n => isExpandedChildNode(n.id));
		const parentNodes = event.nodes.filter(n => !isExpandedChildNode(n.id));

		for (const node of childNodes) {
			updateExpandedChildPosition(node.id, node.position);
		}

		if (parentNodes.length > 0) {
			const updates = parentNodes.map(node => ({
				id: node.id,
				position: node.position
			}));
			updateNodePositions(updates);
		}

		pushHistory();
	}

	// Double-click detection for nodes
	let lastClickTime = 0;
	let lastClickedNodeId: string | null = null;
	const DOUBLE_CLICK_THRESHOLD = 300;

	// Handle click on node (double-click detection + dependency cascade)
	async function onNodeClick(event: { node: Node; event: MouseEvent | TouchEvent }) {
		const now = Date.now();
		const nodeId = event.node.id;

		if (lastClickedNodeId === nodeId && (now - lastClickTime) < DOUBLE_CLICK_THRESHOLD) {
			if (event.node.type !== 'subgraphNode') {
				await tick();
				const input = document.getElementById('node-label') as HTMLInputElement | null;
				if (input) {
					input.focus();
					input.select();
				}
			}
			lastClickedNodeId = null;
			lastClickTime = 0;
		} else {
			lastClickedNodeId = nodeId;
			lastClickTime = now;
		}

		const result = analyzeDependencyCascadeFromNode(nodeId, get(edges));
		cascadeHighlight.set(result ?? null);
	}

	// Handle click on edge (dependency cascade highlighting)
	function onEdgeClick(event: { edge: Edge; event: MouseEvent }) {
		const edge = event.edge;

		selectedEdgeIds.set(new Set([edge.id]));
		selectedNodeIds.set(new Set());

		if ((edge.data as NetrunEdgeData | undefined)?.dependency) {
			const result = analyzeDependencyCascade(edge, get(edges));
			cascadeHighlight.set(result);
		} else {
			cascadeHighlight.set(null);
		}
	}

	// Handle pane context menu
	function onPaneContextMenu(_event: MouseEvent) {
		cascadeHighlight.set(null);
	}

	// Node context menu items
	function nodeContextMenuItems(node: Node): ContextMenuItem[] {
		const items: ContextMenuItem[] = [];
		if (node.data.nodeType !== 'decoration') {
			items.push({ label: 'Open Source', action: 'open-source' });
		}
		const locked = getNodeLocked(node.data as AnyNodeData);
		items.push({
			label: locked ? 'Unlock Position' : 'Lock Position',
			action: 'toggle-lock',
			separator: true,
		});
		items.push({ label: 'Send to Front', action: 'send-to-front', separator: true });
		items.push({ label: 'Send to Back', action: 'send-to-back' });
		items.push({ label: 'Delete Node', action: 'delete', danger: true, separator: true });
		return items;
	}

	// Edge context menu items
	function edgeContextMenuItems(edge: Edge): ContextMenuItem[] {
		const isDep = (edge.data as NetrunEdgeData | undefined)?.dependency;
		return [
			{ label: isDep ? 'Remove Dependency' : 'Make Dependency', action: 'toggle-dependency' },
			{ label: 'Delete Edge', action: 'delete', danger: true, separator: true },
		];
	}

	// Handle node context menu action
	function onNodeContextAction(event: { node: Node; action: string }) {
		const { node, action } = event;
		if (action === 'toggle-lock') {
			const locked = getNodeLocked(node.data as AnyNodeData);
			updateNodeLocked(node.id, !locked);
		} else if (action === 'send-to-front') {
			sendNodesToFront([node.id]);
		} else if (action === 'send-to-back') {
			sendNodesToBack([node.id]);
		} else if (action === 'open-source') {
			selectedNodeIds.set(new Set([node.id]));
			window.dispatchEvent(new CustomEvent('netrun-node-open', {
				detail: { id: node.id, data: node.data },
			}));
		} else if (action === 'delete') {
			deleteNodes([node.id]);
		}
	}

	// Handle edge context menu action
	function onEdgeContextAction(event: { edge: Edge; action: string }) {
		const { edge, action } = event;
		if (action === 'toggle-dependency') {
			toggleEdgeDependency(edge.id);
		} else if (action === 'delete') {
			deleteEdges([edge.id]);
		}
	}

	// Minimap node coloring
	function minimapNodeColor(node: Node): string {
		const cascade = get(cascadeHighlight);
		if (cascade) {
			if (cascade.sourceNodes.has(node.id)) return '#a78bfa';
			if (cascade.visitedNodes.has(node.id)) return '#7c3aed';
		}
		if (node.data?.nodeType === 'decoration') return '#6b7280';
		const extra = ((node.data as Record<string, unknown>)?._config as Record<string, unknown> | undefined)?.extra as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		const headerColor = ui?.headerColor as string | undefined;
		if (headerColor) return headerColor;
		if (node.data?.nodeType === 'subgraph') return '#22c55e';
		return '#3d3d3d';
	}

	// Track previous tab ID to detect tab switches
	let previousTabId: string | null = null;
	let flowApi: { fitView: (options?: FitViewOptions) => void; getNodes: () => Node[] } | null = null;

	const fitViewOptions = { padding: 0.2, maxZoom: 1.5, minZoom: 0.1 };

	function onInit(api: { fitView: (options?: FitViewOptions) => void; getNodes: () => Node[] }) {
		flowApi = api;
		svelteFlowRef.set({ fitView: api.fitView, getNodes: api.getNodes });
	}

	// Fit view when switching tabs
	$effect(() => {
		if ($activeTabId && $activeTabId !== previousTabId) {
			previousTabId = $activeTabId;
			tick().then(() => {
				flowApi?.fitView(fitViewOptions);
			});
		}
	});
</script>

<NetrunFlowViewer
	nodes={$nodesWithSelection}
	edges={$edgesWithSelection}
	{nodeTypes}
	settings={$graphSettings}
	cascadeHighlight={$cascadeHighlight}
	panOnDrag={$panOnDrag}
	selectionOnDrag={$selectionOnDrag}
	{isValidConnection}
	{nodeContextMenuItems}
	{edgeContextMenuItems}
	{minimapNodeColor}
	{onNodeClick}
	{onEdgeClick}
	{onSelectionChange}
	{onNodeDragStop}
	{onConnect}
	{onBeforeConnect}
	{onDelete}
	{onPaneContextMenu}
	{onNodeContextAction}
	{onEdgeContextAction}
	{onInit}
/>
