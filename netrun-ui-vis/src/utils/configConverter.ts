/**
 * Convert a raw .netrun.json config to a NetrunGraph suitable for rendering.
 *
 * This is a pure JS converter that works without a Python backend. It handles:
 * - Nodes with explicit in_ports / out_ports
 * - Factory nodes (infers ports from connected edges if ports aren't declared)
 * - Subgraph nodes (exposed ports)
 * - Decoration nodes
 * - Graph-level settings (edge style, markers, zoom, fonts)
 */
import type { FlowNode, AnyNodeData, NetrunNodeData, SubgraphNodeData, DecorationNodeData, PortConfig } from '../types/nodes.js';
import type { NetrunEdge, NetrunEdgeData } from '../types/edges.js';
import type { NetrunGraph, GraphSettings } from '../types/graph.js';

// --- Raw config types (what .netrun.json looks like) ---

interface RawConfig {
	graph?: RawGraphConfig;
	// Also supports top-level graph format (nodes/edges at root)
	nodes?: RawNodeConfig[];
	edges?: RawEdgeConfig[];
	extra?: Record<string, unknown>;
}

interface RawGraphConfig {
	nodes?: RawNodeConfig[];
	edges?: RawEdgeConfig[];
	extra?: Record<string, unknown>;
}

interface RawNodeConfig {
	name: string;
	type?: 'subgraph';
	factory?: string;
	factory_args?: Record<string, unknown>;
	in_ports?: Record<string, { port_type?: string }>;
	out_ports?: Record<string, { port_type?: string }>;
	description?: string;
	extra?: {
		ui?: {
			position?: { x: number; y: number };
			dimensions?: { width: number; height: number };
			zIndex?: number;
			headerColor?: string;
			portGroups?: Record<string, boolean>;
			[key: string]: unknown;
		};
		[key: string]: unknown;
	};
	// Subgraph fields
	exposed_in_ports?: Record<string, unknown>;
	exposed_out_ports?: Record<string, unknown>;
	path?: string;
	nodes?: RawNodeConfig[];
	edges?: RawEdgeConfig[];
}

interface RawEdgeConfig {
	source_node: string;
	source_port: string;
	target_node: string;
	target_port: string;
	dependency?: boolean;
}

/**
 * Convert a parsed .netrun.json config into a NetrunGraph.
 *
 * @param config - Parsed JSON from a .netrun.json or .netrun.toml file
 * @returns A NetrunGraph ready for NetrunFlowViewer
 */
export function configToGraph(config: Record<string, unknown>): NetrunGraph {
	const raw = config as unknown as RawConfig;

	// Extract graph data (may be nested under "graph" or at top level)
	const graphData = raw.graph ?? { nodes: raw.nodes, edges: raw.edges, extra: raw.extra };
	const rawNodes = graphData.nodes ?? [];
	const rawEdges = graphData.edges ?? [];
	const graphExtra = graphData.extra ?? {};

	// Extract settings from graph.extra.ui
	const uiSettings = (graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
	const settings: GraphSettings = {};
	if (uiSettings) {
		if (uiSettings.edgeStyle) settings.edgeStyle = uiSettings.edgeStyle as GraphSettings['edgeStyle'];
		if (uiSettings.edgeMarkers) settings.edgeMarkers = uiSettings.edgeMarkers as GraphSettings['edgeMarkers'];
		if (uiSettings.minZoom != null) settings.minZoom = uiSettings.minZoom as number;
		if (uiSettings.maxZoom != null) settings.maxZoom = uiSettings.maxZoom as number;
		if (uiSettings.nodeTitleFontSize != null) settings.nodeTitleFontSize = uiSettings.nodeTitleFontSize as number;
		if (uiSettings.nodeDescFontSize != null) settings.nodeDescFontSize = uiSettings.nodeDescFontSize as number;
		if (uiSettings.nodePortFontSize != null) settings.nodePortFontSize = uiSettings.nodePortFontSize as number;
	}

	// Build port inference map from edges (for factory nodes without explicit ports)
	const inferredInPorts = new Map<string, Set<string>>();
	const inferredOutPorts = new Map<string, Set<string>>();
	for (const edge of rawEdges) {
		if (!inferredOutPorts.has(edge.source_node)) inferredOutPorts.set(edge.source_node, new Set());
		inferredOutPorts.get(edge.source_node)!.add(edge.source_port);
		if (!inferredInPorts.has(edge.target_node)) inferredInPorts.set(edge.target_node, new Set());
		inferredInPorts.get(edge.target_node)!.add(edge.target_port);
	}

	// Convert nodes
	const nodes: FlowNode[] = rawNodes.map((raw, i) => {
		const uiExtra = raw.extra?.ui ?? {};
		const position = uiExtra.position ?? { x: i * 200, y: 100 };

		if (raw.type === 'subgraph') {
			return convertSubgraphNode(raw, position, uiExtra, inferredInPorts, inferredOutPorts);
		}
		return convertRegularNode(raw, position, uiExtra, i, inferredInPorts, inferredOutPorts);
	});

	// Convert edges
	const edges: NetrunEdge[] = rawEdges.map((raw, i) => {
		const edge: NetrunEdge = {
			id: `edge-${i}`,
			source: raw.source_node,
			target: raw.target_node,
			sourceHandle: raw.source_port,
			targetHandle: raw.target_port,
		};
		if (raw.dependency) {
			edge.data = { dependency: true } as NetrunEdgeData;
		}
		return edge;
	});

	return { nodes, edges, settings };
}

function convertPorts(
	configPorts: Record<string, { port_type?: string }> | undefined,
	inferredPorts: Set<string> | undefined,
): PortConfig[] {
	if (configPorts && Object.keys(configPorts).length > 0) {
		return Object.entries(configPorts).map(([name, info]) => ({
			name,
			type: info.port_type ?? undefined,
		}));
	}
	// Fall back to inferred ports from edges
	if (inferredPorts && inferredPorts.size > 0) {
		return [...inferredPorts].sort().map(name => ({ name }));
	}
	return [];
}

function convertRegularNode(
	raw: RawNodeConfig,
	position: { x: number; y: number },
	uiExtra: Record<string, unknown>,
	index: number,
	inferredInPorts: Map<string, Set<string>>,
	inferredOutPorts: Map<string, Set<string>>,
): FlowNode {
	const isFactory = raw.factory != null;
	const inPorts = convertPorts(raw.in_ports, inferredInPorts.get(raw.name));
	const outPorts = convertPorts(raw.out_ports, inferredOutPorts.get(raw.name));

	const data: NetrunNodeData = {
		label: raw.name,
		nodeType: isFactory ? 'factory' : 'regular',
		inPorts: inPorts,
		outPorts: outPorts,
		isValid: true,
	};

	if (isFactory) {
		data.factory = raw.factory;
		data.factoryArgs = raw.factory_args;
	}
	if (raw.description) data.description = raw.description;

	// Store original extra in _config for round-trip and header color access
	if (raw.extra) {
		data._config = { extra: raw.extra };
	}

	const node: FlowNode = {
		id: raw.name,
		type: 'netrunNode',
		position,
		data: data as AnyNodeData,
	};

	const dims = uiExtra.dimensions as { width: number; height: number } | undefined;
	if (dims) {
		node.width = dims.width;
		node.height = dims.height;
	}
	const zIndex = uiExtra.zIndex as number | undefined;
	if (zIndex != null) node.zIndex = zIndex;

	return node;
}

function convertSubgraphNode(
	raw: RawNodeConfig,
	position: { x: number; y: number },
	uiExtra: Record<string, unknown>,
	inferredInPorts: Map<string, Set<string>>,
	inferredOutPorts: Map<string, Set<string>>,
): FlowNode {
	const exposedIn = raw.exposed_in_ports ?? {};
	const exposedOut = raw.exposed_out_ports ?? {};

	// Use exposed ports, fall back to inferred
	const inPorts: PortConfig[] = Object.keys(exposedIn).length > 0
		? Object.keys(exposedIn).map(name => ({ name }))
		: [...(inferredInPorts.get(raw.name) ?? [])].sort().map(name => ({ name }));

	const outPorts: PortConfig[] = Object.keys(exposedOut).length > 0
		? Object.keys(exposedOut).map(name => ({ name }))
		: [...(inferredOutPorts.get(raw.name) ?? [])].sort().map(name => ({ name }));

	const isFileRef = raw.path != null;
	const nodeCount = !isFileRef && raw.nodes ? raw.nodes.length : undefined;

	const data: SubgraphNodeData = {
		label: raw.name,
		nodeType: 'subgraph',
		inPorts,
		outPorts,
		isValid: true,
		source: isFileRef ? raw.path : 'Inline',
		nodeCount,
	};

	if (raw.description) data.description = raw.description;
	if (raw.extra) data._config = { extra: raw.extra };

	const node: FlowNode = {
		id: raw.name,
		type: 'subgraphNode',
		position,
		data: data as AnyNodeData,
	};

	const dims = uiExtra.dimensions as { width: number; height: number } | undefined;
	if (dims) {
		node.width = dims.width;
		node.height = dims.height;
	}
	const zIndex = uiExtra.zIndex as number | undefined;
	if (zIndex != null) node.zIndex = zIndex;

	return node;
}
