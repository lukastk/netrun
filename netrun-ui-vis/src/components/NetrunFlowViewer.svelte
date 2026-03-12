<script lang="ts">
	import {
		SvelteFlow,
		Background,
		Controls,
		MiniMap,
		MarkerType,
		type Edge,
		type Node,
		type Connection,
		type NodeTypes,
		type IsValidConnection,
		BackgroundVariant,
		ConnectionLineType,
	} from '@xyflow/svelte';
	import '@xyflow/svelte/dist/style.css';
	import type { GraphSettings } from '../types/graph.js';
	import type { CascadeHighlightState, ContextMenuItem } from '../types/events.js';
	import type { NetrunEdgeData, NetrunEdge } from '../types/edges.js';
	import { PORT_ROW_HEIGHT } from '../constants.js';

	interface Props {
		/** Pre-processed nodes (selection and draggable state already applied). */
		nodes: Node[];
		/** Raw edges (vis component handles styling based on settings/cascade). */
		edges: NetrunEdge[];
		/** Custom node type components. */
		nodeTypes: NodeTypes;
		/** Visual settings (edge style, markers, zoom, font sizes). */
		settings?: GraphSettings;
		/** Cascade highlight state for dependency visualization. */
		cascadeHighlight?: CascadeHighlightState | null;
		/** Grid snap size. Defaults to [PORT_ROW_HEIGHT, PORT_ROW_HEIGHT]. */
		snapGrid?: [number, number];
		/** Pan on drag. Defaults to true. */
		panOnDrag?: boolean | number[];
		/** Selection on drag. Defaults to false. */
		selectionOnDrag?: boolean;
		/** Connection validation function. */
		isValidConnection?: IsValidConnection;

		/** Returns context menu items for a node (return empty array to suppress menu). */
		nodeContextMenuItems?: (node: Node) => ContextMenuItem[];
		/** Returns context menu items for an edge (return empty array to suppress menu). */
		edgeContextMenuItems?: (edge: Edge) => ContextMenuItem[];
		/** Whether to show the minimap. Defaults to true. */
		showMinimap?: boolean;
		/** Custom minimap node coloring. */
		minimapNodeColor?: (node: Node) => string;

		// Event callbacks
		onNodeClick?: (event: { node: Node; event: MouseEvent | TouchEvent }) => void;
		onEdgeClick?: (event: { edge: Edge; event: MouseEvent }) => void;
		onSelectionChange?: (event: { nodes: Node[]; edges: Edge[] }) => void;
		onNodeDragStop?: (event: { nodes: Node[] }) => void;
		onConnect?: (connection: Connection) => void;
		onBeforeConnect?: (connection: Connection) => Connection | false;
		onDelete?: (params: { nodes: Node[]; edges: Edge[] }) => void;
		onPaneContextMenu?: (event: MouseEvent) => void;
		onNodeContextAction?: (event: { node: Node; action: string }) => void;
		onEdgeContextAction?: (event: { edge: Edge; action: string }) => void;
	}

	let {
		nodes,
		edges,
		nodeTypes,
		settings = {},
		cascadeHighlight = null,
		snapGrid = [PORT_ROW_HEIGHT, PORT_ROW_HEIGHT] as [number, number],
		panOnDrag = true,
		selectionOnDrag = false,
		isValidConnection,
		nodeContextMenuItems,
		edgeContextMenuItems,
		showMinimap = true,
		minimapNodeColor,
		onNodeClick,
		onEdgeClick,
		onSelectionChange,
		onNodeDragStop,
		onConnect,
		onBeforeConnect,
		onDelete,
		onPaneContextMenu,
		onNodeContextAction,
		onEdgeContextAction,
	}: Props = $props();

	// Derive settings with defaults
	let edgeStyle = $derived(settings.edgeStyle ?? 'smoothstep');
	let edgeMarkersMode = $derived(settings.edgeMarkers ?? 'arrow-end');
	let minZoom = $derived(settings.minZoom ?? 0.05);
	let maxZoom = $derived(settings.maxZoom ?? 4);
	let nodeTitleFontSize = $derived(settings.nodeTitleFontSize ?? 12);
	let nodeDescFontSize = $derived(settings.nodeDescFontSize ?? 10);
	let nodePortFontSize = $derived(settings.nodePortFontSize ?? 11);

	// Arrow marker configuration
	const arrowMarker = {
		type: MarkerType.ArrowClosed,
		width: 20,
		height: 20,
		color: 'var(--netrun-border-color, #404040)',
	};

	const dependencyArrowMarker = {
		type: MarkerType.ArrowClosed,
		width: 20,
		height: 20,
		color: 'var(--netrun-dependency-color, #a78bfa)',
	};

	// Get marker config based on setting
	function getMarkers(setting: string, marker = arrowMarker): { markerStart?: typeof arrowMarker; markerEnd?: typeof arrowMarker } {
		switch (setting) {
			case 'arrow-start':
				return { markerStart: marker };
			case 'arrow-both':
				return { markerStart: marker, markerEnd: marker };
			case 'none':
				return {};
			case 'arrow-end':
			default:
				return { markerEnd: marker };
		}
	}

	// Derive styled edges (apply type, markers, CSS classes)
	let styledEdges = $derived.by(() => {
		const markers = getMarkers(edgeMarkersMode);
		const depMarkers = getMarkers(edgeMarkersMode, dependencyArrowMarker);
		return edges.map(edge => {
			const isDep = (edge.data as NetrunEdgeData | undefined)?.dependency === true;
			const isCascadeEdge = cascadeHighlight?.visitedEdges.has(edge.id) ?? false;
			const classes: string[] = [];
			if (isDep) classes.push('dependency-edge');
			if (isCascadeEdge && !isDep) classes.push('cascade-edge');
			const m = isDep ? depMarkers : markers;
			return {
				...edge,
				type: edgeStyle,
				markerStart: m.markerStart,
				markerEnd: m.markerEnd,
				class: classes.join(' ') || undefined,
				...(isDep ? { interactionWidth: 30 } : {}),
			};
		});
	});

	// Map edge style to connection line type
	function getConnectionLineType(style: string): ConnectionLineType {
		switch (style) {
			case 'straight': return ConnectionLineType.Straight;
			case 'step': return ConnectionLineType.Step;
			case 'bezier': return ConnectionLineType.Bezier;
			case 'simplebezier': return ConnectionLineType.SimpleBezier;
			default: return ConnectionLineType.SmoothStep;
		}
	}

	// Context menu state
	let nodeContextMenu = $state<{ x: number; y: number; node: Node } | null>(null);
	let edgeContextMenu = $state<{ x: number; y: number; edge: Edge } | null>(null);

	function closeNodeContextMenu() { nodeContextMenu = null; }
	function closeEdgeContextMenu() { edgeContextMenu = null; }

	function handleNodeContextMenuEvent(event: { node: Node; event: MouseEvent }) {
		event.event.preventDefault();
		closeEdgeContextMenu();
		if (nodeContextMenuItems) {
			const items = nodeContextMenuItems(event.node);
			if (items.length > 0) {
				nodeContextMenu = { x: event.event.clientX, y: event.event.clientY, node: event.node };
			}
		}
	}

	function handleEdgeContextMenuEvent(event: { edge: Edge; event: MouseEvent }) {
		event.event.preventDefault();
		closeNodeContextMenu();
		if (edgeContextMenuItems) {
			const items = edgeContextMenuItems(event.edge);
			if (items.length > 0) {
				edgeContextMenu = { x: event.event.clientX, y: event.event.clientY, edge: event.edge };
			}
		}
	}

	function handlePaneContextMenu(event: { event: MouseEvent }) {
		event.event.preventDefault();
		closeNodeContextMenu();
		closeEdgeContextMenu();
		onPaneContextMenu?.(event.event);
	}

	function handleNodeAction(action: string) {
		if (!nodeContextMenu) return;
		const node = nodeContextMenu.node;
		closeNodeContextMenu();
		onNodeContextAction?.({ node, action });
	}

	function handleEdgeAction(action: string) {
		if (!edgeContextMenu) return;
		const edge = edgeContextMenu.edge;
		closeEdgeContextMenu();
		onEdgeContextAction?.({ edge, action });
	}

	// Fit view options
	const fitViewOptions = {
		padding: 0.2,
		maxZoom: 1.5,
		minZoom: 0.1,
	};

</script>

<div class="netrun-flow-viewer" style:--netrun-node-title-font-size="{nodeTitleFontSize}px" style:--netrun-node-desc-font-size="{nodeDescFontSize}px" style:--netrun-node-port-font-size="{nodePortFontSize}px">
	<SvelteFlow
		{nodes}
		edges={styledEdges}
		{nodeTypes}
		onbeforeconnect={onBeforeConnect}
		onconnect={onConnect}
		ondelete={onDelete}
		onselectionchange={onSelectionChange}
		onnodedragstop={onNodeDragStop}
		onpanecontextmenu={handlePaneContextMenu}
		onnodecontextmenu={handleNodeContextMenuEvent}
		onnodeclick={onNodeClick}
		onedgeclick={onEdgeClick}
		onedgecontextmenu={handleEdgeContextMenuEvent}
		{isValidConnection}
		fitView
		{fitViewOptions}
		{snapGrid}
		defaultEdgeOptions={{
			type: edgeStyle,
			animated: false,
			...getMarkers(edgeMarkersMode),
		}}
		connectionLineType={getConnectionLineType(edgeStyle)}
		deleteKey={['Delete', 'Backspace']}
		{panOnDrag}
		{selectionOnDrag}
		selectionKey="Shift"
		colorMode="dark"
		{minZoom}
		{maxZoom}
	>
		<Background variant={BackgroundVariant.Dots} gap={20} size={1} />
		<Controls />
		{#if showMinimap}
			<MiniMap
				nodeColor={minimapNodeColor ?? (() => '#3d3d3d')}
				maskColor="rgba(0, 0, 0, 0.7)"
				pannable
				zoomable
			/>
		{/if}
	</SvelteFlow>

	{#if edgeContextMenu && edgeContextMenuItems}
		{@const items = edgeContextMenuItems(edgeContextMenu.edge)}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="context-overlay" onclick={closeEdgeContextMenu} oncontextmenu={(e) => { e.preventDefault(); closeEdgeContextMenu(); }}>
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div
				class="context-menu"
				style="left: {edgeContextMenu.x}px; top: {edgeContextMenu.y}px;"
				onclick={(e) => e.stopPropagation()}
			>
				{#each items as item}
					{#if item.separator}
						<div class="context-separator"></div>
					{/if}
					<button class="context-item" class:danger={item.danger} onclick={() => handleEdgeAction(item.action)}>
						{item.label}
					</button>
				{/each}
			</div>
		</div>
	{/if}

	{#if nodeContextMenu && nodeContextMenuItems}
		{@const items = nodeContextMenuItems(nodeContextMenu.node)}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="context-overlay" onclick={closeNodeContextMenu} oncontextmenu={(e) => { e.preventDefault(); closeNodeContextMenu(); }}>
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div
				class="context-menu"
				style="left: {nodeContextMenu.x}px; top: {nodeContextMenu.y}px;"
				onclick={(e) => e.stopPropagation()}
			>
				{#each items as item}
					{#if item.separator}
						<div class="context-separator"></div>
					{/if}
					<button class="context-item" class:danger={item.danger} onclick={() => handleNodeAction(item.action)}>
						{item.label}
					</button>
				{/each}
			</div>
		</div>
	{/if}
</div>

<style>
	.netrun-flow-viewer {
		width: 100%;
		height: 100%;
	}

	/* Override SvelteFlow styles for dark mode */
	:global(.svelte-flow) {
		background: var(--netrun-bg-primary, #1a1a1a);
	}

	:global(.svelte-flow__background) {
		background: var(--netrun-bg-primary, #1a1a1a);
	}

	:global(.svelte-flow__edge-path) {
		stroke: var(--netrun-border-color, #404040);
		stroke-width: 2;
	}

	:global(.svelte-flow__edge.selected .svelte-flow__edge-path) {
		stroke: var(--netrun-accent-color, #3b82f6);
	}

	/* Dependency edge styling */
	:global(.svelte-flow__edge.dependency-edge .svelte-flow__edge-path) {
		stroke: var(--netrun-dependency-color, #a78bfa);
		stroke-width: 2;
	}

	:global(.svelte-flow__edge.dependency-edge.selected .svelte-flow__edge-path) {
		stroke: var(--netrun-dependency-selected, #c4b5fd);
	}

	:global(.svelte-flow__edge.dependency-edge marker path) {
		fill: var(--netrun-dependency-color, #a78bfa);
	}

	:global(.svelte-flow__edge.dependency-edge.selected marker path) {
		fill: var(--netrun-dependency-selected, #c4b5fd);
	}

	/* Cascade-visited (non-dependency) edge styling */
	:global(.svelte-flow__edge.cascade-edge .svelte-flow__edge-path) {
		stroke: var(--netrun-dependency-color, #a78bfa);
		opacity: 0.5;
	}

	/* Arrow marker styling */
	:global(.svelte-flow__edge-path + marker path) {
		fill: var(--netrun-border-color, #404040);
	}

	:global(.svelte-flow__edge.selected marker path) {
		fill: var(--netrun-accent-color, #3b82f6);
	}

	:global(.svelte-flow__controls) {
		background: var(--netrun-bg-secondary, #242424);
		border: 1px solid var(--netrun-border-color, #404040);
		border-radius: 4px;
	}

	:global(.svelte-flow__controls-button) {
		background: var(--netrun-bg-secondary, #242424);
		border-bottom: 1px solid var(--netrun-border-color, #404040);
		fill: var(--netrun-text-secondary, #a0a0a0);
	}

	:global(.svelte-flow__controls-button:hover) {
		background: var(--netrun-bg-tertiary, #2d2d2d);
	}

	:global(.svelte-flow__minimap) {
		background: var(--netrun-bg-secondary, #242424);
		border: 1px solid var(--netrun-border-color, #404040);
		border-radius: 4px;
	}

	/* Context menu */
	.context-overlay {
		position: fixed;
		inset: 0;
		z-index: 1000;
	}

	.context-menu {
		position: fixed;
		background: var(--netrun-bg-secondary, #242424);
		border: 1px solid var(--netrun-border-color, #404040);
		border-radius: 6px;
		padding: 4px 0;
		min-width: 160px;
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
		z-index: 1001;
	}

	.context-item {
		display: block;
		width: 100%;
		padding: 6px 12px;
		background: none;
		border: none;
		color: var(--netrun-text-primary, #fff);
		font-size: 12px;
		text-align: left;
		cursor: pointer;
	}

	.context-item:hover {
		background: var(--netrun-bg-tertiary, #2d2d2d);
	}

	.context-item.danger {
		color: var(--netrun-error-color, #ef4444);
	}

	.context-separator {
		height: 1px;
		background: var(--netrun-border-color, #404040);
		margin: 4px 0;
	}
</style>
