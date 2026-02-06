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
import { triggerFileExplorerRefresh } from './fileExplorerStore';
import { updateUrlWithFile } from './urlStore';

// Types for netrun node data
export interface PortConfig {
	name: string;
	type?: string;
	[key: string]: unknown; // Allow additional properties
}

// Base data interface shared by all node types
export interface BaseNodeData extends Record<string, unknown> {
	label: string;
	nodeType: 'regular' | 'factory' | 'subgraph';
	inPorts: PortConfig[];
	outPorts: PortConfig[];
	// Validation state
	isValid?: boolean;
	validationErrors?: string[];
}

// Extended data for regular/factory nodes
export interface NetrunNodeData extends BaseNodeData {
	nodeType: 'regular' | 'factory';
	// For factory nodes
	factory?: string;
	factoryArgs?: Record<string, unknown>;
	// Extra config data
	_config?: Record<string, unknown>;
}

// Extended data for subgraph nodes
export interface SubgraphNodeData extends BaseNodeData {
	nodeType: 'subgraph';
	// Subgraph-specific
	source?: string; // "Inline" or file path
	nodeCount?: number; // Number of nodes inside (for inline)
	// Store full subgraph config for round-trip serialization
	_subgraphConfig?: Record<string, unknown>;
}

// Combined type for any flow node data
export type AnyNodeData = NetrunNodeData | SubgraphNodeData;

// Use generic Node with AnyNodeData to avoid union type issues
export type FlowNode = Node<AnyNodeData>;
export type NetrunNode = Node<NetrunNodeData, 'netrunNode'>;
export type SubgraphNode = Node<SubgraphNodeData, 'subgraphNode'>;
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
export const isInlineSubgraph = derived(activeTab, ($activeTab) => $activeTab?.subgraphContext?.isInline || false);
export const isNewFile = derived(activeTab, ($activeTab) => $activeTab?.isNewFile || false);

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

/**
 * Select a node by its label/name
 * Used for deep linking via URL parameters
 */
export function selectNodeByName(name: string): boolean {
	const tab = get(activeTab);
	if (!tab) return false;

	// Find node by label (case-insensitive)
	const node = tab.nodes.find(n => n.data.label.toLowerCase() === name.toLowerCase());
	if (!node) {
		console.warn(`Node not found: ${name}`);
		return false;
	}

	// Select the node
	selectedNodeIds.set(new Set([node.id]));
	selectedEdgeIds.set(new Set());

	return true;
}

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

/**
 * Rename a node: updates its id, data.label, and all edge references.
 * Returns true if renamed successfully, false if newName is invalid/duplicate.
 */
export function renameNode(oldName: string, newName: string): boolean {
	if (oldName === newName) return true;

	const tab = get(activeTab);
	if (!tab) return false;

	const trimmed = newName.trim();
	if (!trimmed) return false;

	// Check for duplicates
	if (tab.nodes.some(n => n.id !== oldName && n.data.label === trimmed)) {
		return false;
	}

	// Update nodes: change id and label for the renamed node
	const updatedNodes = tab.nodes.map(node => {
		if (node.id === oldName) {
			return { ...node, id: trimmed, data: { ...node.data, label: trimmed } };
		}
		return node;
	});

	// Update edges: source/target references
	const updatedEdges = tab.edges.map(edge => {
		let changed = false;
		let newEdge = { ...edge };
		if (edge.source === oldName) {
			newEdge.source = trimmed;
			changed = true;
		}
		if (edge.target === oldName) {
			newEdge.target = trimmed;
			changed = true;
		}
		return changed ? newEdge : edge;
	});

	// Update selectedNodeIds if needed
	const currentSelected = get(selectedNodeIds);
	if (currentSelected.has(oldName)) {
		const newSelected = new Set(currentSelected);
		newSelected.delete(oldName);
		newSelected.add(trimmed);
		selectedNodeIds.set(newSelected);
	}

	updateActiveTab({
		nodes: updatedNodes,
		edges: updatedEdges,
		isDirty: true,
	});

	return true;
}

// Update node-level environment variable overrides
export function updateNodeEnv(id: string, env: Record<string, string> | undefined) {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		nodes: tab.nodes.map(node => {
			if (node.id !== id) return node;

			const config = (node.data._config || {}) as Record<string, unknown>;
			const meta = (config.meta || {}) as Record<string, unknown>;
			const ui = (meta.ui || {}) as Record<string, unknown>;

			const newUi = env && Object.keys(env).length > 0
				? { ...ui, env }
				: (() => { const { env: _env, ...rest } = ui; return rest; })();

			const newMeta = Object.keys(newUi).length > 0
				? { ...meta, ui: newUi }
				: (() => { const { ui: _ui, ...rest } = meta; return rest; })();

			const newConfig = Object.keys(newMeta).length > 0
				? { ...config, meta: newMeta }
				: (() => { const { meta: _meta, ...rest } = config; return rest; })();

			return {
				...node,
				data: {
					...node.data,
					_config: Object.keys(newConfig).length > 0 ? newConfig : undefined,
				}
			};
		}),
		isDirty: true,
	});
}

// Update node-level actions
export function updateNodeActions(id: string, actions: unknown[] | undefined) {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		nodes: tab.nodes.map(node => {
			if (node.id !== id) return node;

			const config = (node.data._config || {}) as Record<string, unknown>;
			const meta = (config.meta || {}) as Record<string, unknown>;
			const ui = (meta.ui || {}) as Record<string, unknown>;

			const newUi = actions && actions.length > 0
				? { ...ui, actions }
				: (() => { const { actions: _actions, ...rest } = ui; return rest; })();

			const newMeta = Object.keys(newUi).length > 0
				? { ...meta, ui: newUi }
				: (() => { const { ui: _ui, ...rest } = meta; return rest; })();

			const newConfig = Object.keys(newMeta).length > 0
				? { ...config, meta: newMeta }
				: (() => { const { meta: _meta, ...rest } = config; return rest; })();

			return {
				...node,
				data: {
					...node.data,
					_config: Object.keys(newConfig).length > 0 ? newConfig : undefined,
				}
			};
		}),
		isDirty: true,
	});
}

// Update node-level salvo conditions
export function updateNodeSalvoConditions(
	id: string,
	type: 'in' | 'out',
	conditions: Record<string, unknown> | null
) {
	const tab = get(activeTab);
	if (!tab) return;

	pushHistory();
	updateActiveTab({
		nodes: tab.nodes.map(node => {
			if (node.id !== id) return node;

			const config = (node.data._config || {}) as Record<string, unknown>;
			const configKey = type === 'in' ? 'in_salvo_conditions' : 'out_salvo_conditions';

			let newConfig: Record<string, unknown>;
			if (conditions === null) {
				// Remove the key to use defaults
				const { [configKey]: _removed, ...rest } = config;
				newConfig = rest;
			} else {
				// Set the conditions
				newConfig = { ...config, [configKey]: conditions };
			}

			return {
				...node,
				data: {
					...node.data,
					_config: Object.keys(newConfig).length > 0 ? newConfig : undefined,
				}
			};
		}),
		isDirty: true,
	});
}

// Get salvo conditions from a node's _config
export function getNodeSalvoConditions(
	node: FlowNode,
	type: 'in' | 'out'
): Record<string, unknown> | null {
	const config = (node.data._config || {}) as Record<string, unknown>;
	const configKey = type === 'in' ? 'in_salvo_conditions' : 'out_salvo_conditions';
	const conditions = config[configKey];

	if (conditions === undefined || conditions === null) {
		return null; // Use defaults
	}

	return conditions as Record<string, unknown>;
}

// Update node-level execution config
export function updateNodeExecutionConfig(
	id: string,
	executionConfig: Record<string, unknown> | null
) {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		nodes: tab.nodes.map(node => {
			if (node.id !== id) return node;

			const config = (node.data._config || {}) as Record<string, unknown>;

			let newConfig: Record<string, unknown>;
			if (executionConfig === null || Object.keys(executionConfig).length === 0) {
				// Remove execution_config to use defaults
				const { execution_config: _removed, ...rest } = config;
				newConfig = rest;
			} else {
				// Set the execution config
				newConfig = { ...config, execution_config: executionConfig };
			}

			return {
				...node,
				data: {
					...node.data,
					_config: Object.keys(newConfig).length > 0 ? newConfig : undefined,
				}
			};
		}),
		isDirty: true,
	});
}

// Get execution config from a node's _config
export function getNodeExecutionConfig(
	node: FlowNode
): Record<string, unknown> | null {
	const config = (node.data._config || {}) as Record<string, unknown>;
	const executionConfig = config.execution_config;

	if (executionConfig === undefined || executionConfig === null) {
		return null; // Use defaults
	}

	return executionConfig as Record<string, unknown>;
}

/**
 * Rename a pool across all nodes' execution configs
 * Updates any node that references the old pool name in its pools array
 */
export function renamePoolInAllNodes(oldName: string, newName: string): void {
	if (oldName === newName) return;

	const tab = get(activeTab);
	if (!tab) return;

	let hasChanges = false;
	const updatedNodes = tab.nodes.map(node => {
		const config = (node.data._config || {}) as Record<string, unknown>;
		const executionConfig = config.execution_config as Record<string, unknown> | undefined;

		if (!executionConfig) return node;

		const pools = executionConfig.pools as string[] | undefined;
		if (!pools || !Array.isArray(pools)) return node;

		// Check if this node uses the old pool name
		const poolIndex = pools.indexOf(oldName);
		if (poolIndex === -1) return node;

		// Replace the old pool name with the new one
		hasChanges = true;
		const newPools = [...pools];
		newPools[poolIndex] = newName;

		return {
			...node,
			data: {
				...node.data,
				_config: {
					...config,
					execution_config: {
						...executionConfig,
						pools: newPools,
					},
				},
			},
		};
	});

	if (hasChanges) {
		updateActiveTab({
			nodes: updatedNodes,
			isDirty: true,
		});
	}
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

/**
 * Check if a connection is valid.
 * Prevents fan-out: multiple edges from the same output port are not allowed.
 * Allows fan-in: multiple edges to the same input port are allowed.
 */
export function isValidConnection(connection: { source?: string | null; sourceHandle?: string | null }): boolean {
	if (!connection.source || !connection.sourceHandle) return false;

	const tab = get(activeTab);
	if (!tab) return false;

	// Prevent multiple edges from same output port (fan-out not allowed)
	const existingFromSource = tab.edges.some(
		e => e.source === connection.source && e.sourceHandle === connection.sourceHandle
	);

	return !existingFromSource;
}

export function addEdge(edge: NetrunEdge) {
	const tab = get(activeTab);
	if (!tab) return;

	// Validate: no multiple edges from same output port (fan-out not allowed)
	const existingFromSource = tab.edges.some(
		e => e.source === edge.source && e.sourceHandle === edge.sourceHandle
	);

	if (existingFromSource) {
		console.warn('Cannot add edge: output port already has an outgoing edge');
		return;
	}

	pushHistory();
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

// Toast notifications
import { toasts } from './toastStore';

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
function validateNode(node: FlowNode, allNodes: FlowNode[]): string[] {
	const errors: string[] = [];

	// Check label
	if (!node.data.label || node.data.label.trim() === '') {
		errors.push('Node must have a name');
	} else {
		// Check for duplicate names
		const duplicates = allNodes.filter(
			n => n.id !== node.id && n.data.label.trim() === node.data.label.trim()
		);
		if (duplicates.length > 0) {
			errors.push('Duplicate node name');
		}
	}

	// Check factory nodes have factory path
	if (node.data.nodeType === 'factory') {
		const data = node.data as NetrunNodeData;
		if (!data.factory || data.factory.trim() === '') {
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

	// Check for duplicate port names within the node
	const inPortNames = node.data.inPorts.map(p => p.name.trim()).filter(n => n);
	const outPortNames = node.data.outPorts.map(p => p.name.trim()).filter(n => n);

	if (new Set(inPortNames).size !== inPortNames.length) {
		errors.push('Duplicate input port names');
	}
	if (new Set(outPortNames).size !== outPortNames.length) {
		errors.push('Duplicate output port names');
	}

	return errors;
}

/**
 * Validate all nodes and return updated nodes with validation state
 * This is a pure function that doesn't update state directly
 */
function computeValidatedNodes(nodes: FlowNode[]): { nodes: FlowNode[]; errorCount: number } {
	let errorCount = 0;
	const updatedNodes = nodes.map(node => {
		const errors = validateNode(node, nodes);
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

	return { nodes: updatedNodes, errorCount };
}

/**
 * Validate all nodes in the active tab and update their validation state
 */
export function validateAllNodes(): { valid: boolean; errorCount: number } {
	const tab = get(activeTab);
	if (!tab) return { valid: true, errorCount: 0 };

	const { nodes: updatedNodes, errorCount } = computeValidatedNodes(tab.nodes);
	updateActiveTab({ nodes: updatedNodes });

	// Also trigger backend validation
	triggerBackendValidation();

	return { valid: errorCount === 0, errorCount };
}

// Backend validation state
export const backendValidationErrors = writable<{ loc: (string | number)[]; msg: string }[]>([]);
export const backendValidationAvailable = writable<boolean>(true);

// Debounce timer for backend validation
let backendValidationTimer: ReturnType<typeof setTimeout> | null = null;

/**
 * Trigger backend validation (debounced)
 */
function triggerBackendValidation() {
	if (backendValidationTimer) {
		clearTimeout(backendValidationTimer);
	}

	backendValidationTimer = setTimeout(async () => {
		const tab = get(activeTab);
		if (!tab) return;

		try {
			// Convert nodes to UINode format for API
			const apiNodes: UINode[] = tab.nodes.map(n => ({
				id: n.id,
				type: n.type || 'netrunNode',
				position: n.position,
				data: {
					label: n.data.label,
					nodeType: n.data.nodeType,
					inPorts: n.data.inPorts,
					outPorts: n.data.outPorts,
					factory: (n.data as NetrunNodeData).factory,
					factoryArgs: (n.data as NetrunNodeData).factoryArgs,
					_config: (n.data as NetrunNodeData)._config,
					_subgraphConfig: (n.data as SubgraphNodeData)._subgraphConfig,
				},
			}));

			const apiEdges: UIEdge[] = tab.edges.map(e => ({
				id: e.id,
				source: e.source,
				target: e.target,
				sourceHandle: e.sourceHandle ?? undefined,
				targetHandle: e.targetHandle ?? undefined,
				type: e.type,
			}));

			const response = await api.validateConfig(
				apiNodes,
				apiEdges,
				tab.graphMeta ?? undefined,
				tab.extraData ?? undefined
			);

			backendValidationAvailable.set(response.netrun_available);
			backendValidationErrors.set(response.errors);

			// Validate factory import paths
			const factoryNodes = tab.nodes.filter(
				n => n.data.nodeType === 'factory' && (n.data as NetrunNodeData).factory?.trim()
			);

			const factoryErrorMap = new Map<string, string[]>();

			if (factoryNodes.length > 0) {
				const projectRoot = (tab.extraData as Record<string, unknown>)?.project_root as string | undefined;
				const results = await Promise.all(
					factoryNodes.map(async (node) => {
						const factory = (node.data as NetrunNodeData).factory!;
						try {
							const result = await api.validateImport(factory, projectRoot);
							if (!result.valid) {
								return { nodeId: node.id, error: `Factory import failed: ${result.error}` };
							}
							if (!result.is_factory) {
								return { nodeId: node.id, error: `Module '${factory}' has no get_node_config function` };
							}
							return null;
						} catch {
							return null; // Backend unreachable, skip
						}
					})
				);

				for (const result of results) {
					if (result) {
						const existing = factoryErrorMap.get(result.nodeId) || [];
						existing.push(result.error);
						factoryErrorMap.set(result.nodeId, existing);
					}
				}
			}

			// Merge factory import errors into nodes
			if (factoryErrorMap.size > 0) {
				const currentTab = get(activeTab);
				if (!currentTab) return;

				// Re-compute client-side validation as a clean base, then add factory errors
				const { nodes: validatedNodes } = computeValidatedNodes(currentTab.nodes);
				const mergedNodes = validatedNodes.map(node => {
					const fErrors = factoryErrorMap.get(node.id);
					if (fErrors) {
						const existingErrors = node.data.validationErrors || [];
						return {
							...node,
							data: {
								...node.data,
								isValid: false,
								validationErrors: [...existingErrors, ...fErrors],
							},
						};
					}
					return node;
				});

				updateActiveTab({ nodes: mergedNodes });
			}
		} catch (e) {
			// Backend not available - clear errors but note it's not available
			backendValidationAvailable.set(false);
			backendValidationErrors.set([]);
		}
	}, 500); // 500ms debounce
}

// Auto-validation: Subscribe to tab changes and validate nodes
let lastValidatedNodesJSON = '';
activeTab.subscribe((tab) => {
	if (!tab) return;

	// Create a simple hash of nodes to detect changes
	// Only include fields that affect validation
	const nodesForValidation = tab.nodes.map(n => ({
		id: n.id,
		label: n.data.label,
		nodeType: n.data.nodeType,
		factory: (n.data as NetrunNodeData).factory,
		inPorts: n.data.inPorts.map(p => p.name),
		outPorts: n.data.outPorts.map(p => p.name),
	}));
	const nodesJSON = JSON.stringify(nodesForValidation);

	// Only revalidate if relevant data has changed
	if (nodesJSON === lastValidatedNodesJSON) return;
	lastValidatedNodesJSON = nodesJSON;

	// Compute validation but check if it would change anything
	const { nodes: validatedNodes, errorCount } = computeValidatedNodes(tab.nodes);

	// Only update if validation state actually changed
	const validationChanged = validatedNodes.some((vNode, i) => {
		const origNode = tab.nodes[i];
		return vNode.data.isValid !== origNode.data.isValid ||
			JSON.stringify(vNode.data.validationErrors) !== JSON.stringify(origNode.data.validationErrors);
	});

	if (validationChanged) {
		updateActiveTab({ nodes: validatedNodes });
	}

	// Trigger backend validation (debounced)
	triggerBackendValidation();
});

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
export function pasteNodes(position?: { x: number; y: number }): FlowNode[] {
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

	// Create new nodes with unique names as IDs
	// We need to accumulate generated names to avoid collisions between pasted nodes
	const allExisting = [...tab.nodes];
	const newNodes: FlowNode[] = clipboardNodes.map(node => {
		const uniqueName = generateUniqueNodeName(allExisting, node.data.label);
		const newNode: FlowNode = {
			...node,
			id: uniqueName,
			position: {
				x: node.position.x + offsetX,
				y: node.position.y + offsetY,
			},
			data: { ...node.data, label: uniqueName },
			selected: false,
		};
		// Add to allExisting so subsequent pasted nodes see this name as taken
		allExisting.push(newNode);
		return newNode;
	});

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
let edgeCounter = 0;
export function generateEdgeId(): string {
	return `edge-${Date.now()}-${edgeCounter++}`;
}

/**
 * Generate a unique node name given existing nodes.
 * If `base` is not taken, returns it as-is.
 * Otherwise appends _1, _2, etc.
 */
export function generateUniqueNodeName(existingNodes: FlowNode[], base: string): string {
	const names = new Set(existingNodes.map(n => n.data.label));
	if (!names.has(base)) return base;
	let i = 1;
	while (names.has(`${base}_${i}`)) i++;
	return `${base}_${i}`;
}

// Create a new regular node (id = name)
export function createRegularNode(position: { x: number; y: number }, name?: string): NetrunNode {
	const tab = get(activeTab);
	const existingNodes = tab?.nodes || [];
	const nodeName = generateUniqueNodeName(existingNodes, name || 'Node');
	return {
		id: nodeName,
		type: 'netrunNode',
		position,
		data: {
			label: nodeName,
			nodeType: 'regular',
			inPorts: [{ name: 'in', type: 'any' }],
			outPorts: [{ name: 'out', type: 'any' }],
			isValid: true,
		}
	};
}

// Create a new factory node (id = name)
export function createFactoryNode(
	position: { x: number; y: number },
	factory: string,
	factoryArgs: Record<string, unknown> = {}
): NetrunNode {
	const tab = get(activeTab);
	const existingNodes = tab?.nodes || [];
	const isFilePath = factory.includes('/') || factory.includes('\\') || factory.startsWith('.');
	const shortName = isFilePath
		? (factory.split('/').pop()?.replace('.py', '') || 'Factory_Node')
		: (factory.split('.').pop() || 'Factory_Node');
	const nodeName = generateUniqueNodeName(existingNodes, shortName);
	return {
		id: nodeName,
		type: 'netrunNode',
		position,
		data: {
			label: nodeName,
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
	const loadedNodes: FlowNode[] = response.nodes.map(node => {
		if (node.data.nodeType === 'subgraph') {
			// Subgraph node
			return {
				id: node.id,
				type: node.type as 'subgraphNode',
				position: node.position,
				data: {
					label: node.data.label,
					nodeType: 'subgraph' as const,
					inPorts: node.data.inPorts.map(apiPortToPortConfig),
					outPorts: node.data.outPorts.map(apiPortToPortConfig),
					isValid: node.data.isValid ?? true,
					validationErrors: node.data.validationErrors,
					source: node.data.source,
					nodeCount: node.data.nodeCount,
					_subgraphConfig: node.data._subgraphConfig,
				}
			} as SubgraphNode;
		} else {
			// Regular or factory node
			return {
				id: node.id,
				type: node.type as 'netrunNode',
				position: node.position,
				data: {
					label: node.data.label,
					nodeType: node.data.nodeType as 'regular' | 'factory',
					inPorts: node.data.inPorts.map(apiPortToPortConfig),
					outPorts: node.data.outPorts.map(apiPortToPortConfig),
					factory: node.data.factory,
					factoryArgs: node.data.factoryArgs,
					isValid: node.data.isValid ?? true,
					validationErrors: node.data.validationErrors,
					_config: node.data._config as Record<string, unknown> | undefined,
				}
			} as NetrunNode;
		}
	});

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

	// Update browser URL to reflect opened file
	updateUrlWithFile(path);
}

// Save inline subgraph changes back to parent tab
export function saveInlineSubgraphToParent(): boolean {
	const tab = get(activeTab);
	if (!tab || !tab.subgraphContext?.isInline) return false;

	const parentTab = get(tabs).find(t => t.id === tab.subgraphContext!.parentTabId);
	if (!parentTab) return false;

	// Find the subgraph node in the parent
	const nodeId = tab.subgraphContext.nodeId;
	const parentNode = parentTab.nodes.find(n => n.id === nodeId);
	if (!parentNode || parentNode.data.nodeType !== 'subgraph') return false;

	// Build the updated subgraph config from current tab's nodes/edges
	const updatedConfig = {
		...(parentNode.data as SubgraphNodeData)._subgraphConfig,
		nodes: tab.nodes.map(n => {
			// Convert UI node back to config format
			const nodeData = n.data;

			if (nodeData.nodeType === 'subgraph') {
				// For subgraph nodes, preserve the full _subgraphConfig to maintain nested content
				const subgraphData = nodeData as SubgraphNodeData;
				return {
					type: 'subgraph',
					name: nodeData.label,
					// Spread the stored subgraph config (includes nodes, edges, exposed_ports, etc.)
					...(subgraphData._subgraphConfig || {}),
					// Update meta with current UI state (only position)
					meta: {
						ui: {
							position: n.position,
						}
					}
				};
			} else {
				// Regular node
				return {
					type: 'node',
					name: nodeData.label,
					in_ports: Object.fromEntries(
						nodeData.inPorts.map(p => [p.name, { port_type: p.type || null }])
					),
					out_ports: Object.fromEntries(
						nodeData.outPorts.map(p => [p.name, { port_type: p.type || null }])
					),
					meta: {
						ui: {
							position: n.position,
						}
					}
				};
			}
		}),
		edges: tab.edges.map(e => ({
			source_str: `${e.source}.${e.sourceHandle || 'out'}`,
			target_str: `${e.target}.${e.targetHandle || 'in'}`,
		})),
	};

	// Update the parent node with the new config
	const updatedNodes = parentTab.nodes.map(n => {
		if (n.id === nodeId) {
			return {
				...n,
				data: {
					...n.data,
					nodeCount: tab.nodes.length,
					_subgraphConfig: updatedConfig,
				}
			};
		}
		return n;
	});

	// Update parent tab
	updateTab(parentTab.id, {
		nodes: updatedNodes as FlowNode[],
		isDirty: true,
	});

	// Mark current tab as clean
	updateActiveTab({ isDirty: false });

	return true;
}

// Save to file via API
export async function saveToFile(path?: string): Promise<void> {
	const tab = get(activeTab);
	if (!tab) throw new Error('No active tab');

	// Handle inline subgraphs - save to parent instead
	if (tab.subgraphContext?.isInline) {
		const saved = saveInlineSubgraphToParent();
		if (saved) {
			return; // Successfully saved to parent
		}
		throw new Error('Failed to save inline subgraph to parent');
	}

	// Check for duplicate node names before saving
	const nameCount = new Map<string, number>();
	for (const node of tab.nodes) {
		const name = node.data.label.trim();
		nameCount.set(name, (nameCount.get(name) || 0) + 1);
	}
	const duplicates = [...nameCount.entries()].filter(([, count]) => count > 1).map(([name]) => name);
	if (duplicates.length > 0) {
		toasts.error(`Cannot save: duplicate node names: ${duplicates.join(', ')}`);
		throw new Error(`Duplicate node names: ${duplicates.join(', ')}`);
	}

	let savePath = path || tab.filePath;
	if (!savePath) {
		throw new Error('No file path specified');
	}

	// Ensure file has a valid netrun extension
	if (!savePath.endsWith('.netrun.json') && !savePath.endsWith('.netrun.toml')) {
		savePath = savePath + '.netrun.json';
	}

	// Determine format from path extension or use current format
	let format = tab.fileFormat;
	if (savePath.endsWith('.json')) {
		format = 'json';
	} else if (savePath.endsWith('.toml')) {
		format = 'toml';
	}

	// Convert to API format
	const apiNodes: UINode[] = tab.nodes.map(node => {
		const data = node.data as NetrunNodeData | SubgraphNodeData;
		const baseData = {
			label: data.label,
			nodeType: data.nodeType as 'regular' | 'factory' | 'subgraph',
			inPorts: data.inPorts.map(p => ({ name: p.name, type: p.type })),
			outPorts: data.outPorts.map(p => ({ name: p.name, type: p.type })),
			isValid: data.isValid,
			validationErrors: data.validationErrors,
		};

		// Add type-specific properties
		if (data.nodeType === 'factory') {
			const factoryData = data as NetrunNodeData;
			return {
				id: node.id,
				type: node.type || 'netrunNode',
				position: node.position,
				data: {
					...baseData,
					factory: factoryData.factory,
					factoryArgs: factoryData.factoryArgs,
					_config: factoryData._config as Record<string, unknown> | undefined,
				}
			};
		} else if (data.nodeType === 'subgraph') {
			const subgraphData = data as SubgraphNodeData;
			return {
				id: node.id,
				type: node.type || 'subgraphNode',
				position: node.position,
				data: {
					...baseData,
					source: subgraphData.source,
					nodeCount: subgraphData.nodeCount,
					_subgraphConfig: subgraphData._subgraphConfig,
				}
			};
		} else {
			const regularData = data as NetrunNodeData;
			return {
				id: node.id,
				type: node.type || 'netrunNode',
				position: node.position,
				data: {
					...baseData,
					_config: regularData._config as Record<string, unknown> | undefined,
				}
			};
		}
	});

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

	// Refresh file explorer to show the new/updated file
	triggerFileExplorerRefresh();

	// Update URL to reflect the saved file path
	updateUrlWithFile(savePath);
}

// Clear the current flow (reset active tab) and create a new file
export function clearFlow(format: 'json' | 'toml' = 'json', fileName?: string): void {
	const tab = get(activeTab);
	if (!tab) return;

	const extension = format === 'toml' ? '.netrun.toml' : '.netrun.json';
	const name = fileName || `Untitled${extension}`;
	// Ensure filename has correct extension
	const finalName = name.endsWith(extension) ? name : `${name}${extension}`;

	updateActiveTab({
		nodes: [],
		edges: [],
		filePath: null,
		fileName: finalName,
		isDirty: false,
		history: { past: [], future: [] },
		extraData: null,
		graphMeta: null,
		fileFormat: format,
		isNewFile: true,
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
		const projectRoot = (tab.extraData as Record<string, unknown>)?.project_root as string | undefined;
		const preview = await api.previewFactory(
			node.data.factory,
			node.data.factoryArgs || {},
			projectRoot
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
		// Note: We deliberately don't update the label - the user controls the node name,
		// not the factory. Factories only provide ports and other structural data.
		updateNodeData(nodeId, {
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

/**
 * Create a subgraph from selected nodes
 */
export async function createSubgraphFromSelection(subgraphName: string): Promise<boolean> {
	const tab = get(activeTab);
	if (!tab) return false;

	const selectedIds = get(selectedNodeIds);
	if (selectedIds.size < 2) {
		console.warn('Need at least 2 nodes selected to create a subgraph');
		return false;
	}

	// Get selected nodes and all edges
	const selectedNodes = tab.nodes.filter(n => selectedIds.has(n.id));

	// Convert nodes to API format
	const apiNodes: UINode[] = selectedNodes.map(node => {
		const baseData = {
			label: node.data.label,
			nodeType: node.data.nodeType,
			inPorts: node.data.inPorts,
			outPorts: node.data.outPorts,
			isValid: node.data.isValid,
			validationErrors: node.data.validationErrors,
		};

		if (node.data.nodeType === 'subgraph') {
			const subgraphData = node.data as SubgraphNodeData;
			return {
				id: node.id,
				type: node.type || 'subgraphNode',
				position: node.position,
				data: {
					...baseData,
					source: subgraphData.source,
					nodeCount: subgraphData.nodeCount,
					_subgraphConfig: subgraphData._subgraphConfig,
				}
			};
		} else {
			const regularData = node.data as NetrunNodeData;
			return {
				id: node.id,
				type: node.type || 'netrunNode',
				position: node.position,
				data: {
					...baseData,
					factory: regularData.factory,
					factoryArgs: regularData.factoryArgs,
					_config: regularData._config,
				}
			};
		}
	});

	// Convert all nodes to API format
	const allApiNodes: UINode[] = tab.nodes.map(node => {
		const baseData = {
			label: node.data.label,
			nodeType: node.data.nodeType,
			inPorts: node.data.inPorts,
			outPorts: node.data.outPorts,
			isValid: node.data.isValid,
			validationErrors: node.data.validationErrors,
		};

		if (node.data.nodeType === 'subgraph') {
			const subgraphData = node.data as SubgraphNodeData;
			return {
				id: node.id,
				type: node.type || 'subgraphNode',
				position: node.position,
				data: {
					...baseData,
					source: subgraphData.source,
					nodeCount: subgraphData.nodeCount,
					_subgraphConfig: subgraphData._subgraphConfig,
				}
			};
		} else {
			const regularData = node.data as NetrunNodeData;
			return {
				id: node.id,
				type: node.type || 'netrunNode',
				position: node.position,
				data: {
					...baseData,
					factory: regularData.factory,
					factoryArgs: regularData.factoryArgs,
					_config: regularData._config,
				}
			};
		}
	});

	const apiEdges: UIEdge[] = tab.edges.map(edge => ({
		id: edge.id,
		source: edge.source,
		target: edge.target,
		sourceHandle: edge.sourceHandle ?? undefined,
		targetHandle: edge.targetHandle ?? undefined,
		type: edge.type,
	}));

	try {
		const response = await api.createSubgraph(
			subgraphName,
			Array.from(selectedIds),
			allApiNodes,
			apiEdges
		);

		// Convert response to our types
		const subgraphNode: SubgraphNode = {
			id: response.subgraph_node.id,
			type: 'subgraphNode',
			position: response.subgraph_node.position,
			data: {
				label: response.subgraph_node.data.label,
				nodeType: 'subgraph',
				inPorts: response.subgraph_node.data.inPorts.map(apiPortToPortConfig),
				outPorts: response.subgraph_node.data.outPorts.map(apiPortToPortConfig),
				isValid: response.subgraph_node.data.isValid ?? true,
				validationErrors: response.subgraph_node.data.validationErrors,
				source: response.subgraph_node.data.source,
				nodeCount: response.subgraph_node.data.nodeCount,
				_subgraphConfig: response.subgraph_node.data._subgraphConfig,
			}
		};

		// Convert remaining nodes back to our types
		const remainingNodes: FlowNode[] = response.remaining_nodes.map(node => {
			if (node.data.nodeType === 'subgraph') {
				return {
					id: node.id,
					type: 'subgraphNode' as const,
					position: node.position,
					data: {
						label: node.data.label,
						nodeType: 'subgraph' as const,
						inPorts: node.data.inPorts.map(apiPortToPortConfig),
						outPorts: node.data.outPorts.map(apiPortToPortConfig),
						isValid: node.data.isValid ?? true,
						validationErrors: node.data.validationErrors,
						source: node.data.source,
						nodeCount: node.data.nodeCount,
						_subgraphConfig: node.data._subgraphConfig,
					}
				} as SubgraphNode;
			} else {
				return {
					id: node.id,
					type: 'netrunNode' as const,
					position: node.position,
					data: {
						label: node.data.label,
						nodeType: node.data.nodeType as 'regular' | 'factory',
						inPorts: node.data.inPorts.map(apiPortToPortConfig),
						outPorts: node.data.outPorts.map(apiPortToPortConfig),
						factory: node.data.factory,
						factoryArgs: node.data.factoryArgs,
						isValid: node.data.isValid ?? true,
						validationErrors: node.data.validationErrors,
						_config: node.data._config as Record<string, unknown> | undefined,
					}
				} as NetrunNode;
			}
		});

		const remainingEdges: NetrunEdge[] = response.remaining_edges.map(edge => ({
			id: edge.id,
			source: edge.source,
			target: edge.target,
			sourceHandle: edge.sourceHandle,
			targetHandle: edge.targetHandle,
			type: edge.type || 'smoothstep',
		}));

		// Update the tab with new nodes and edges
		pushHistory();
		updateActiveTab({
			nodes: [...remainingNodes, subgraphNode],
			edges: remainingEdges,
			isDirty: true,
		});

		// Select the new subgraph node
		selectedNodeIds.set(new Set([subgraphNode.id]));

		return true;
	} catch (error) {
		console.error('Failed to create subgraph:', error);
		return false;
	}
}

/**
 * Get the current config as a serializable object for recipes.
 * This captures the full state that a recipe might want to transform.
 */
export function getCurrentConfig(): Record<string, unknown> {
	const tab = get(activeTab);
	if (!tab) return { nodes: [], edges: [], meta: {}, extraData: {} };

	return {
		nodes: tab.nodes.map(n => ({
			id: n.id,
			type: n.type,
			position: n.position,
			data: n.data
		})),
		edges: tab.edges.map(e => ({
			id: e.id,
			source: e.source,
			target: e.target,
			sourceHandle: e.sourceHandle,
			targetHandle: e.targetHandle
		})),
		meta: tab.graphMeta || {},
		extraData: tab.extraData || {}
	};
}

/**
 * Apply a new config from a recipe transformation.
 * This replaces nodes and edges with the recipe's output.
 */
export function applyConfig(config: Record<string, unknown>): void {
	const tab = get(activeTab);
	if (!tab) return;

	pushHistory();

	const configNodes = (config.nodes as unknown[]) ?? [];
	const configEdges = (config.edges as unknown[]) ?? [];

	// Convert to internal node format
	const newNodes: FlowNode[] = configNodes.map((n: unknown) => {
		const node = n as Record<string, unknown>;
		const data = (node.data ?? {}) as Record<string, unknown>;
		const nodeType = (data.nodeType as string) || 'regular';

		return {
			id: node.id as string,
			type: (node.type as string) ?? (nodeType === 'subgraph' ? 'subgraphNode' : 'netrunNode'),
			position: (node.position as { x: number; y: number }) ?? { x: 0, y: 0 },
			data: {
				label: (data.label as string) ?? (node.id as string),
				nodeType: nodeType as 'regular' | 'factory' | 'subgraph',
				inPorts: (data.inPorts as PortConfig[]) ?? [],
				outPorts: (data.outPorts as PortConfig[]) ?? [],
				factory: data.factory as string | undefined,
				factoryArgs: data.factoryArgs as Record<string, unknown> | undefined,
				isValid: true,
				_config: data._config as Record<string, unknown> | undefined,
				_subgraphConfig: data._subgraphConfig as Record<string, unknown> | undefined,
			}
		} as FlowNode;
	});

	// Convert to internal edge format
	const newEdges: NetrunEdge[] = configEdges.map((e: unknown) => {
		const edge = e as Record<string, unknown>;
		return {
			id: edge.id as string,
			source: edge.source as string,
			target: edge.target as string,
			sourceHandle: edge.sourceHandle as string | undefined,
			targetHandle: edge.targetHandle as string | undefined,
			type: (edge.type as string) || 'smoothstep',
		};
	});

	updateActiveTab({
		nodes: newNodes,
		edges: newEdges,
		isDirty: true,
	});

	// Update meta and extraData if provided
	if (config.meta) {
		updateActiveTab({
			graphMeta: config.meta as Record<string, unknown>,
		});
	}
	if (config.extraData) {
		updateActiveTab({
			extraData: config.extraData as Record<string, unknown>,
		});
	}
}
