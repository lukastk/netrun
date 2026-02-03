/**
 * Flow state management for netrun-ui
 *
 * This store now integrates with tabsStore for multi-tab support.
 * All state is stored per-tab, and this module provides the interface
 * for working with the active tab's state.
 */
import { writable, derived, get } from 'svelte/store';
import type { Node, Edge } from '@xyflow/svelte';
import { api, type UINode, type UIEdge } from '$lib/api';
import {
	tabs,
	activeTab,
	activeTabId,
	updateActiveTab,
	updateTab,
	createTab,
	switchTab,
	getTabByFilePath,
	createEmptyTabState,
	type TabState,
} from './tabsStore';

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

// Re-export tab stores for convenience
export { tabs, activeTab, activeTabId } from './tabsStore';
export { createTab, switchTab, closeTab, closeActiveTab, switchToTabIndex, switchToNextTab, switchToPreviousTab, hasUnsavedChanges } from './tabsStore';

// Derived stores from active tab
export const nodes = derived(activeTab, ($activeTab) => $activeTab?.nodes || []);
export const edges = derived(activeTab, ($activeTab) => $activeTab?.edges || []);
export const isDirty = derived(activeTab, ($activeTab) => $activeTab?.isDirty || false);
export const currentFilePath = derived(activeTab, ($activeTab) => $activeTab?.filePath || null);
export const extraData = derived(activeTab, ($activeTab) => $activeTab?.extraData || null);
export const graphMeta = derived(activeTab, ($activeTab) => $activeTab?.graphMeta || null);
export const fileFormat = derived(activeTab, ($activeTab) => $activeTab?.fileFormat || 'json');
export const history = derived(activeTab, ($activeTab) => $activeTab?.history || { past: [], future: [] });

// Selection state (not per-tab, applies to current view)
export const selectedNodeIds = writable<Set<string>>(new Set());
export const selectedEdgeIds = writable<Set<string>>(new Set());

// Track current tab to detect real tab switches vs. data updates
let previousTabId: string | null = null;

// Clear selection only when actually switching tabs (not on data updates)
activeTabId.subscribe((newTabId) => {
	if (previousTabId !== null && newTabId !== previousTabId) {
		selectedNodeIds.set(new Set());
		selectedEdgeIds.set(new Set());
	}
	previousTabId = newTabId;
});

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

const MAX_HISTORY = 50;

export function pushHistory() {
	const tab = get(activeTab);
	if (!tab) return;

	const currentNodes = tab.nodes;
	const currentEdges = tab.edges;

	updateActiveTab({
		history: {
			past: [...tab.history.past.slice(-MAX_HISTORY + 1), { nodes: currentNodes, edges: currentEdges }],
			future: []
		},
		isDirty: true,
	});
}

export function undo() {
	const tab = get(activeTab);
	if (!tab || tab.history.past.length === 0) return;

	const currentNodes = tab.nodes;
	const currentEdges = tab.edges;
	const previous = tab.history.past[tab.history.past.length - 1];

	updateActiveTab({
		history: {
			past: tab.history.past.slice(0, -1),
			future: [{ nodes: currentNodes, edges: currentEdges }, ...tab.history.future]
		},
		nodes: previous.nodes,
		edges: previous.edges,
		isDirty: true,
	});
}

export function redo() {
	const tab = get(activeTab);
	if (!tab || tab.history.future.length === 0) return;

	const currentNodes = tab.nodes;
	const currentEdges = tab.edges;
	const next = tab.history.future[0];

	updateActiveTab({
		history: {
			past: [...tab.history.past, { nodes: currentNodes, edges: currentEdges }],
			future: tab.history.future.slice(1)
		},
		nodes: next.nodes,
		edges: next.edges,
		isDirty: true,
	});
}

// Helper functions
export function addNode(node: NetrunNode) {
	pushHistory();
	const tab = get(activeTab);
	if (!tab) return;
	updateActiveTab({ nodes: [...tab.nodes, node] });
}

export function updateNode(id: string, updates: Partial<NetrunNode>) {
	pushHistory();
	const tab = get(activeTab);
	if (!tab) return;
	updateActiveTab({
		nodes: tab.nodes.map(node =>
			node.id === id ? { ...node, ...updates } : node
		)
	});
}

export function updateNodeData(id: string, dataUpdates: Partial<NetrunNodeData>) {
	pushHistory();
	const tab = get(activeTab);
	if (!tab) return;
	updateActiveTab({
		nodes: tab.nodes.map(node =>
			node.id === id ? { ...node, data: { ...node.data, ...dataUpdates } } : node
		)
	});
}

// Update node data without pushing history (for live editing like typing)
export function updateNodeDataLive(id: string, dataUpdates: Partial<NetrunNodeData>) {
	const tab = get(activeTab);
	if (!tab) return;
	updateActiveTab({
		nodes: tab.nodes.map(node =>
			node.id === id ? { ...node, data: { ...node.data, ...dataUpdates } } : node
		),
		isDirty: true,
	});
}

// Update node positions (called when nodes are dragged)
export function updateNodePositions(updates: Array<{ id: string; position: { x: number; y: number } }>) {
	const tab = get(activeTab);
	if (!tab) return;
	updateActiveTab({
		nodes: tab.nodes.map(node => {
			const update = updates.find(u => u.id === node.id);
			if (update) {
				return { ...node, position: update.position };
			}
			return node;
		}),
		isDirty: true,
	});
}

export function deleteNodes(ids: string[]) {
	pushHistory();
	const tab = get(activeTab);
	if (!tab) return;
	const idSet = new Set(ids);
	updateActiveTab({
		nodes: tab.nodes.filter(node => !idSet.has(node.id)),
		edges: tab.edges.filter(edge => !idSet.has(edge.source) && !idSet.has(edge.target)),
	});
}

export function addEdge(edge: NetrunEdge) {
	pushHistory();
	const tab = get(activeTab);
	if (!tab) return;
	updateActiveTab({ edges: [...tab.edges, edge] });
}

export function deleteEdges(ids: string[]) {
	pushHistory();
	const tab = get(activeTab);
	if (!tab) return;
	const idSet = new Set(ids);
	updateActiveTab({
		edges: tab.edges.filter(edge => !idSet.has(edge.id))
	});
}

// Copy/Paste functionality
import { copyToClipboard, getClipboardNodes, hasClipboardContent } from './clipboardStore';
export { hasClipboardContent } from './clipboardStore';

// Recent files
import { addRecentFile } from './recentFilesStore';
export { recentFiles, removeRecentFile, clearRecentFiles } from './recentFilesStore';

/**
 * Update extraData (pools, etc.) for the active tab
 */
export function updateExtraData(updates: Record<string, unknown>): void {
	const tab = get(activeTab);
	if (!tab) return;

	pushHistory();
	updateActiveTab({
		extraData: { ...(tab.extraData || {}), ...updates },
	});
}

/**
 * Update graphMeta for the active tab
 */
export function updateGraphMeta(updates: Record<string, unknown>): void {
	const tab = get(activeTab);
	if (!tab) return;

	pushHistory();
	updateActiveTab({
		graphMeta: { ...(tab.graphMeta || {}), ...updates },
	});
}

/**
 * Update extraData without pushing history (for live editing)
 */
export function updateExtraDataLive(updates: Record<string, unknown>): void {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		extraData: { ...(tab.extraData || {}), ...updates },
		isDirty: true,
	});
}

/**
 * Update graphMeta without pushing history (for live editing)
 */
export function updateGraphMetaLive(updates: Record<string, unknown>): void {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		graphMeta: { ...(tab.graphMeta || {}), ...updates },
		isDirty: true,
	});
}

/**
 * Validate a single node and return validation errors
 */
function validateNode(node: NetrunNode): string[] {
	const errors: string[] = [];

	// Check label
	if (!node.data.label || node.data.label.trim() === '') {
		errors.push('Node must have a name');
	}

	// Check factory nodes have factory path
	if (node.data.nodeType === 'factory') {
		if (!node.data.factory || node.data.factory.trim() === '') {
			errors.push('Factory node must have a factory path');
		}
	}

	// Check ports have names
	for (const port of node.data.inPorts) {
		if (!port.name || port.name.trim() === '') {
			errors.push('Input port missing name');
			break;
		}
	}
	for (const port of node.data.outPorts) {
		if (!port.name || port.name.trim() === '') {
			errors.push('Output port missing name');
			break;
		}
	}

	return errors;
}

/**
 * Validate all nodes in the active tab and update their validation state
 */
export function validateAllNodes(): { valid: boolean; errorCount: number } {
	const tab = get(activeTab);
	if (!tab) return { valid: true, errorCount: 0 };

	let errorCount = 0;
	const updatedNodes = tab.nodes.map(node => {
		const errors = validateNode(node);
		const isValid = errors.length === 0;
		if (!isValid) errorCount++;

		return {
			...node,
			data: {
				...node.data,
				isValid,
				validationErrors: isValid ? undefined : errors,
			},
		};
	});

	updateActiveTab({ nodes: updatedNodes });

	return { valid: errorCount === 0, errorCount };
}

/**
 * Clear validation state on all nodes
 */
export function clearValidation(): void {
	const tab = get(activeTab);
	if (!tab) return;

	const updatedNodes = tab.nodes.map(node => ({
		...node,
		data: {
			...node.data,
			isValid: true,
			validationErrors: undefined,
		},
	}));

	updateActiveTab({ nodes: updatedNodes });
}

/**
 * Copy selected nodes to clipboard
 */
export function copySelectedNodes(): void {
	const selectedIds = get(selectedNodeIds);
	if (selectedIds.size === 0) return;

	const tab = get(activeTab);
	if (!tab) return;

	const nodesToCopy = tab.nodes.filter(node => selectedIds.has(node.id));
	copyToClipboard(nodesToCopy, get(activeTabId));
}

/**
 * Paste nodes from clipboard at the given position
 * If no position given, offsets from original positions
 */
export function pasteNodes(position?: { x: number; y: number }): NetrunNode[] {
	if (!hasClipboardContent()) return [];

	const clipboardNodes = getClipboardNodes();
	const tab = get(activeTab);
	if (!tab || clipboardNodes.length === 0) return [];

	pushHistory();

	// Calculate offset for pasting
	// If position given, center the pasted nodes there
	// Otherwise, offset by 50px from original positions
	let offsetX = 50;
	let offsetY = 50;

	if (position && clipboardNodes.length > 0) {
		// Find bounding box of clipboard nodes
		const minX = Math.min(...clipboardNodes.map(n => n.position.x));
		const minY = Math.min(...clipboardNodes.map(n => n.position.y));
		const maxX = Math.max(...clipboardNodes.map(n => n.position.x));
		const maxY = Math.max(...clipboardNodes.map(n => n.position.y));

		// Center of clipboard nodes
		const centerX = (minX + maxX) / 2;
		const centerY = (minY + maxY) / 2;

		// Offset to move center to target position
		offsetX = position.x - centerX;
		offsetY = position.y - centerY;
	}

	// Create new nodes with new IDs and offset positions
	const newNodes: NetrunNode[] = clipboardNodes.map(node => ({
		...node,
		id: generateNodeId(),
		position: {
			x: node.position.x + offsetX,
			y: node.position.y + offsetY,
		},
		data: { ...node.data },
		selected: false,
	}));

	// Add new nodes
	updateActiveTab({
		nodes: [...tab.nodes, ...newNodes],
	});

	// Select the pasted nodes
	selectedNodeIds.set(new Set(newNodes.map(n => n.id)));

	return newNodes;
}

/**
 * Cut selected nodes (copy + delete)
 */
export function cutSelectedNodes(): void {
	copySelectedNodes();
	const selectedIds = get(selectedNodeIds);
	if (selectedIds.size > 0) {
		deleteNodes(Array.from(selectedIds));
	}
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

// Helper to convert API port info to our PortConfig
function apiPortToPortConfig(port: { name: string; type?: string | null }): PortConfig {
	return {
		name: port.name,
		type: port.type ?? undefined,
	};
}

// Load from file via API
// Creates a new tab if file not already open, or switches to existing tab
export async function loadFromFile(path: string): Promise<void> {
	// Check if file is already open in a tab
	const existingTab = getTabByFilePath(path);
	if (existingTab) {
		switchTab(existingTab.id);
		return;
	}

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

	// Check if current tab is empty and untitled - reuse it
	const currentTab = get(activeTab);
	const currentTabList = get(tabs);

	if (currentTab && !currentTab.filePath && currentTab.nodes.length === 0 && !currentTab.isDirty) {
		// Reuse current empty tab
		updateActiveTab({
			filePath: path,
			fileName: path.split('/').pop() || 'Untitled',
			nodes: loadedNodes,
			edges: loadedEdges,
			isDirty: false,
			history: { past: [], future: [] },
			extraData: response.extra_data || null,
			graphMeta: response.meta || null,
			fileFormat: response.format,
		});
	} else {
		// Create a new tab with the loaded content
		const tabId = createTab(path, true);
		updateTab(tabId, {
			nodes: loadedNodes,
			edges: loadedEdges,
			isDirty: false,
			history: { past: [], future: [] },
			extraData: response.extra_data || null,
			graphMeta: response.meta || null,
			fileFormat: response.format,
		});
	}

	// Track in recent files
	addRecentFile(path);
}

// Save to file via API
export async function saveToFile(path?: string): Promise<void> {
	const tab = get(activeTab);
	if (!tab) throw new Error('No active tab');

	const savePath = path || tab.filePath;
	if (!savePath) {
		throw new Error('No file path specified');
	}

	// Determine format from path extension or use current format
	let format = tab.fileFormat;
	if (savePath.endsWith('.json')) {
		format = 'json';
	} else if (savePath.endsWith('.toml')) {
		format = 'toml';
	}

	// Convert to API format
	const apiNodes: UINode[] = tab.nodes.map(node => ({
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

	const apiEdges: UIEdge[] = tab.edges.map(edge => ({
		id: edge.id,
		source: edge.source,
		target: edge.target,
		sourceHandle: edge.sourceHandle ?? undefined,
		targetHandle: edge.targetHandle ?? undefined,
		type: edge.type,
	}));

	await api.saveFile(
		savePath,
		format,
		apiNodes,
		apiEdges,
		tab.graphMeta ?? undefined,
		tab.extraData ?? undefined
	);

	updateActiveTab({
		filePath: savePath,
		fileName: savePath.split('/').pop() || 'Untitled',
		fileFormat: format,
		isDirty: false,
	});
}

// Clear the current flow (reset active tab)
export function clearFlow(): void {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		nodes: [],
		edges: [],
		filePath: null,
		fileName: 'Untitled',
		isDirty: false,
		history: { past: [], future: [] },
		extraData: null,
		graphMeta: null,
	});
}

// Update factory node with preview from API
export async function updateFactoryNodePreview(nodeId: string): Promise<void> {
	const tab = get(activeTab);
	if (!tab) return;

	const node = tab.nodes.find(n => n.id === nodeId);

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
