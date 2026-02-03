/**
 * Flow state management for netrun-ui
 */
import { writable, derived, get } from 'svelte/store';
import type { Node, Edge } from '@xyflow/svelte';
import { api, type UINode, type UIEdge } from '$lib/api';

// Types for netrun node data
export interface PortConfig {
	name: string;
	type?: string;
	[key: string]: unknown; // Allow additional properties
}

export interface NetrunNodeData extends Record<string, unknown> {
	label: string;
	nodeType: 'regular' | 'factory';
	inPorts: PortConfig[];
	outPorts: PortConfig[];
	// For factory nodes
	factory?: string;
	factoryArgs?: Record<string, unknown>;
	// Validation state
	isValid?: boolean;
	validationErrors?: string[];
}

export type NetrunNode = Node<NetrunNodeData, 'netrunNode'>;
export type NetrunEdge = Edge;

// Flow state
export const nodes = writable<NetrunNode[]>([]);
export const edges = writable<NetrunEdge[]>([]);

// Selection state
export const selectedNodeIds = writable<Set<string>>(new Set());
export const selectedEdgeIds = writable<Set<string>>(new Set());

// Derived: selected nodes
export const selectedNodes = derived(
	[nodes, selectedNodeIds],
	([$nodes, $selectedIds]) => $nodes.filter(n => $selectedIds.has(n.id))
);

// Derived: selected node (single selection for sidebar)
export const selectedNode = derived(
	selectedNodes,
	($selectedNodes) => $selectedNodes.length === 1 ? $selectedNodes[0] : null
);

// Current file state
export const currentFilePath = writable<string | null>(null);
export const isDirty = writable(false);

// Undo/Redo history
interface HistoryState {
	nodes: NetrunNode[];
	edges: NetrunEdge[];
}

interface History {
	past: HistoryState[];
	future: HistoryState[];
}

export const history = writable<History>({ past: [], future: [] });

const MAX_HISTORY = 50;

export function pushHistory() {
	const currentNodes = get(nodes);
	const currentEdges = get(edges);

	history.update(h => ({
		past: [...h.past.slice(-MAX_HISTORY + 1), { nodes: currentNodes, edges: currentEdges }],
		future: []
	}));

	isDirty.set(true);
}

export function undo() {
	const h = get(history);
	if (h.past.length === 0) return;

	const currentNodes = get(nodes);
	const currentEdges = get(edges);
	const previous = h.past[h.past.length - 1];

	history.set({
		past: h.past.slice(0, -1),
		future: [{ nodes: currentNodes, edges: currentEdges }, ...h.future]
	});

	nodes.set(previous.nodes);
	edges.set(previous.edges);
}

export function redo() {
	const h = get(history);
	if (h.future.length === 0) return;

	const currentNodes = get(nodes);
	const currentEdges = get(edges);
	const next = h.future[0];

	history.set({
		past: [...h.past, { nodes: currentNodes, edges: currentEdges }],
		future: h.future.slice(1)
	});

	nodes.set(next.nodes);
	edges.set(next.edges);
}

// Helper functions
export function addNode(node: NetrunNode) {
	pushHistory();
	nodes.update(n => [...n, node]);
}

export function updateNode(id: string, updates: Partial<NetrunNode>) {
	pushHistory();
	nodes.update(n => n.map(node =>
		node.id === id ? { ...node, ...updates } : node
	));
}

export function updateNodeData(id: string, dataUpdates: Partial<NetrunNodeData>) {
	pushHistory();
	nodes.update(n => n.map(node =>
		node.id === id ? { ...node, data: { ...node.data, ...dataUpdates } } : node
	));
}

// Update node data without pushing history (for live editing like typing)
export function updateNodeDataLive(id: string, dataUpdates: Partial<NetrunNodeData>) {
	nodes.update(n => n.map(node =>
		node.id === id ? { ...node, data: { ...node.data, ...dataUpdates } } : node
	));
	isDirty.set(true);
}

// Update node positions (called when nodes are dragged)
export function updateNodePositions(updates: Array<{ id: string; position: { x: number; y: number } }>) {
	nodes.update(n => n.map(node => {
		const update = updates.find(u => u.id === node.id);
		if (update) {
			return { ...node, position: update.position };
		}
		return node;
	}));
	isDirty.set(true);
}

export function deleteNodes(ids: string[]) {
	pushHistory();
	const idSet = new Set(ids);
	nodes.update(n => n.filter(node => !idSet.has(node.id)));
	// Also remove connected edges
	edges.update(e => e.filter(edge => !idSet.has(edge.source) && !idSet.has(edge.target)));
}

export function addEdge(edge: NetrunEdge) {
	pushHistory();
	edges.update(e => [...e, edge]);
}

export function deleteEdges(ids: string[]) {
	pushHistory();
	const idSet = new Set(ids);
	edges.update(e => e.filter(edge => !idSet.has(edge.id)));
}

// Generate unique IDs
let nodeCounter = 0;
export function generateNodeId(): string {
	return `node-${Date.now()}-${nodeCounter++}`;
}

let edgeCounter = 0;
export function generateEdgeId(): string {
	return `edge-${Date.now()}-${edgeCounter++}`;
}

// Create a new regular node
export function createRegularNode(position: { x: number; y: number }, name?: string): NetrunNode {
	const id = generateNodeId();
	return {
		id,
		type: 'netrunNode',
		position,
		data: {
			label: name || `Node ${id.slice(-4)}`,
			nodeType: 'regular',
			inPorts: [{ name: 'in', type: 'any' }],
			outPorts: [{ name: 'out', type: 'any' }],
			isValid: true,
		}
	};
}

// Create a new factory node
export function createFactoryNode(
	position: { x: number; y: number },
	factory: string,
	factoryArgs: Record<string, unknown> = {}
): NetrunNode {
	const id = generateNodeId();
	return {
		id,
		type: 'netrunNode',
		position,
		data: {
			label: factory.split('.').pop() || 'Factory Node',
			nodeType: 'factory',
			factory,
			factoryArgs,
			inPorts: [], // Will be populated by factory
			outPorts: [],
			isValid: true,
		}
	};
}

// File format store
export const fileFormat = writable<'json' | 'toml'>('json');

// Helper to convert API port info to our PortConfig
function apiPortToPortConfig(port: { name: string; type?: string | null }): PortConfig {
	return {
		name: port.name,
		type: port.type ?? undefined,
	};
}

// Load from file via API
export async function loadFromFile(path: string): Promise<void> {
	const response = await api.readFile(path);

	// Convert API response to our node/edge types
	const loadedNodes: NetrunNode[] = response.nodes.map(node => ({
		id: node.id,
		type: node.type as 'netrunNode',
		position: node.position,
		data: {
			label: node.data.label,
			nodeType: node.data.nodeType,
			inPorts: node.data.inPorts.map(apiPortToPortConfig),
			outPorts: node.data.outPorts.map(apiPortToPortConfig),
			factory: node.data.factory,
			factoryArgs: node.data.factoryArgs,
			isValid: node.data.isValid ?? true,
			validationErrors: node.data.validationErrors,
			_config: node.data._config as Record<string, unknown> | undefined,
		}
	}));

	const loadedEdges: NetrunEdge[] = response.edges.map(edge => ({
		id: edge.id,
		source: edge.source,
		target: edge.target,
		sourceHandle: edge.sourceHandle,
		targetHandle: edge.targetHandle,
		type: edge.type || 'smoothstep',
	}));

	// Update stores
	nodes.set(loadedNodes);
	edges.set(loadedEdges);
	currentFilePath.set(path);
	fileFormat.set(response.format);
	isDirty.set(false);
	history.set({ past: [], future: [] });
}

// Save to file via API
export async function saveToFile(path?: string): Promise<void> {
	const savePath = path || get(currentFilePath);
	if (!savePath) {
		throw new Error('No file path specified');
	}

	// Determine format from path extension or use current format
	let format = get(fileFormat);
	if (savePath.endsWith('.json')) {
		format = 'json';
	} else if (savePath.endsWith('.toml')) {
		format = 'toml';
	}

	const currentNodes = get(nodes);
	const currentEdges = get(edges);

	// Convert to API format
	const apiNodes: UINode[] = currentNodes.map(node => ({
		id: node.id,
		type: node.type || 'netrunNode',
		position: node.position,
		data: {
			label: node.data.label,
			nodeType: node.data.nodeType,
			inPorts: node.data.inPorts.map(p => ({ name: p.name, type: p.type })),
			outPorts: node.data.outPorts.map(p => ({ name: p.name, type: p.type })),
			factory: node.data.factory,
			factoryArgs: node.data.factoryArgs,
			isValid: node.data.isValid,
			validationErrors: node.data.validationErrors,
			_config: node.data._config as Record<string, unknown> | undefined,
		}
	}));

	const apiEdges: UIEdge[] = currentEdges.map(edge => ({
		id: edge.id,
		source: edge.source,
		target: edge.target,
		sourceHandle: edge.sourceHandle ?? undefined,
		targetHandle: edge.targetHandle ?? undefined,
		type: edge.type,
	}));

	await api.saveFile(savePath, format, apiNodes, apiEdges);

	currentFilePath.set(savePath);
	fileFormat.set(format);
	isDirty.set(false);
}

// Clear the current flow
export function clearFlow(): void {
	nodes.set([]);
	edges.set([]);
	currentFilePath.set(null);
	isDirty.set(false);
	history.set({ past: [], future: [] });
}

// Update factory node with preview from API
export async function updateFactoryNodePreview(nodeId: string): Promise<void> {
	const currentNodes = get(nodes);
	const node = currentNodes.find(n => n.id === nodeId);

	if (!node || node.data.nodeType !== 'factory' || !node.data.factory) {
		return;
	}

	try {
		const preview = await api.previewFactory(
			node.data.factory,
			node.data.factoryArgs || {}
		);

		if (preview.error) {
			// Update node with error state
			updateNodeData(nodeId, {
				isValid: false,
				validationErrors: [preview.error],
			});
			return;
		}

		// Update node with preview data
		updateNodeData(nodeId, {
			label: preview.name || node.data.label,
			inPorts: preview.in_ports.map(p => ({
				name: p.name,
				type: p.port_type || undefined,
			})),
			outPorts: preview.out_ports.map(p => ({
				name: p.name,
				type: p.port_type || undefined,
			})),
			isValid: true,
			validationErrors: [],
		});
	} catch (error) {
		updateNodeData(nodeId, {
			isValid: false,
			validationErrors: [(error as Error).message],
		});
	}
}
