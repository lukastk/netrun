/**
 * Subgraph expansion store for netrun-ui
 *
 * Manages inline expansion of subgraph nodes within the parent flow.
 * Expanded subgraphs show their internal nodes as children of the subgraph node
 * using SvelteFlow's built-in parentId mechanism.
 */
import { writable, derived, get } from 'svelte/store';
import { api } from '$lib/api';
import {
	nodes,
	edges,
	activeTab,
	activeTabId,
	updateNodeDataLive,
	registerChildNodeHandlers,
	pushHistory,
	isExpandedChildNode,
	getParentSubgraphId,
	getOriginalChildId,
	makeChildNodeId,
	allVisibleNodes,
	allVisibleEdges,
	type FlowNode,
	type NetrunEdge,
	type SubgraphNodeData,
	type AnyNodeData,
} from './flowStore';
import { tabs, updateActiveTab } from './tabsStore';

// ── Types ──────────────────────────────────────────────────────

interface ExpandedContent {
	nodes: FlowNode[];
	edges: NetrunEdge[];
	originalWidth?: number;
	originalHeight?: number;
}

// ── State ──────────────────────────────────────────────────────

/** Set of currently expanded subgraph node IDs (per-tab, keyed by tab ID) */
const expandedByTab = writable<Map<string, Set<string>>>(new Map());

/** Cache of loaded subgraph content (per-tab, keyed by tab::nodeId) */
const contentCache = new Map<string, ExpandedContent>();

/** Currently expanded node IDs for the active tab */
export const expandedSubgraphIds = derived(
	[expandedByTab, activeTabId],
	([$expandedByTab, $activeTabId]) => {
		if (!$activeTabId) return new Set<string>();
		return $expandedByTab.get($activeTabId) || new Set<string>();
	}
);

// ── Helpers ────────────────────────────────────────────────────

function getTabExpandedSet(tabId: string): Set<string> {
	const map = get(expandedByTab);
	return map.get(tabId) || new Set();
}

function setTabExpandedSet(tabId: string, ids: Set<string>): void {
	expandedByTab.update(map => {
		const newMap = new Map(map);
		newMap.set(tabId, ids);
		return newMap;
	});
}

function cacheKey(tabId: string, nodeId: string): string {
	return `${tabId}::${nodeId}`;
}

function getUiConfig(data: AnyNodeData): Record<string, unknown> {
	const config = ((data as Record<string, unknown>)._config || {}) as Record<string, unknown>;
	const extra = (config.extra || {}) as Record<string, unknown>;
	return (extra.ui || {}) as Record<string, unknown>;
}

// ── Load subgraph content ──────────────────────────────────────

async function loadSubgraphContent(
	nodeId: string,
	data: SubgraphNodeData
): Promise<ExpandedContent> {
	const tab = get(activeTab);
	if (!tab) throw new Error('No active tab');

	const subgraphConfig = data._subgraphConfig;

	// Find root tab for resolving file paths
	let rootTab = tab;
	const tabList = get(tabs);
	while (rootTab.subgraphContext) {
		const parent = tabList.find(t => t.id === rootTab.subgraphContext!.parentTabId);
		if (parent) rootTab = parent;
		else break;
	}
	const basePath = rootTab.filePath || undefined;
	const projectRoot = (rootTab.extraData as Record<string, unknown> | null)?.project_root as string | undefined;

	const configPath = subgraphConfig?.path as string | undefined;
	const configNodes = subgraphConfig?.nodes as unknown[] | undefined;
	const isFileReference = configPath || (data.source && data.source !== 'Inline');

	let loadedNodes: FlowNode[] = [];
	let loadedEdges: NetrunEdge[] = [];

	if (isFileReference) {
		const filePath = configPath || data.source;
		const response = await api.loadSubgraph(filePath, undefined, basePath, projectRoot);
		loadedNodes = response.nodes.map(node => ({
			id: node.id,
			type: node.type as 'netrunNode' | 'subgraphNode',
			position: node.position,
			data: node.data as AnyNodeData,
		}));
		loadedEdges = response.edges.map(e => ({
			id: e.id,
			source: e.source,
			target: e.target,
			sourceHandle: e.sourceHandle,
			targetHandle: e.targetHandle,
			type: e.type || 'smoothstep',
		}));
	} else if (subgraphConfig && (configNodes || subgraphConfig.edges)) {
		const response = await api.loadSubgraph(undefined, subgraphConfig, basePath, projectRoot);
		loadedNodes = response.nodes.map(node => ({
			id: node.id,
			type: node.type as 'netrunNode' | 'subgraphNode',
			position: node.position,
			data: node.data as AnyNodeData,
		}));
		loadedEdges = response.edges.map(e => ({
			id: e.id,
			source: e.source,
			target: e.target,
			sourceHandle: e.sourceHandle,
			targetHandle: e.targetHandle,
			type: e.type || 'smoothstep',
		}));
	}

	return { nodes: loadedNodes, edges: loadedEdges };
}

// ── Expand / Collapse ──────────────────────────────────────────

const HEADER_OFFSET_Y = 50; // Space for the subgraph header/ports
const PADDING = 40;
const MIN_EXPANDED_WIDTH = 400;
const MIN_EXPANDED_HEIGHT = 300;

export async function expandSubgraph(nodeId: string): Promise<void> {
	const tab = get(activeTab);
	if (!tab) return;

	const node = tab.nodes.find(n => n.id === nodeId);
	if (!node || node.data.nodeType !== 'subgraph') return;

	const tabId = get(activeTabId);
	if (!tabId) return;

	const ids = getTabExpandedSet(tabId);
	if (ids.has(nodeId)) return; // Already expanded

	try {
		const content = await loadSubgraphContent(nodeId, node.data as SubgraphNodeData);

		// Store original dimensions
		content.originalWidth = node.width as number | undefined;
		content.originalHeight = node.height as number | undefined;

		// Cache the content
		contentCache.set(cacheKey(tabId, nodeId), content);

		// Compute bounding box of child nodes to size the parent
		if (content.nodes.length > 0) {
			let maxX = 0;
			let maxY = 0;
			for (const child of content.nodes) {
				const w = (child.width as number | undefined) || 160;
				const h = (child.height as number | undefined) || 80;
				maxX = Math.max(maxX, child.position.x + w);
				maxY = Math.max(maxY, child.position.y + h);
			}

			const newWidth = Math.max(MIN_EXPANDED_WIDTH, maxX + PADDING * 2);
			const newHeight = Math.max(MIN_EXPANDED_HEIGHT, maxY + HEADER_OFFSET_Y + PADDING * 2);

			// Resize the subgraph node to fit children
			updateActiveTab({
				nodes: tab.nodes.map(n => {
					if (n.id !== nodeId) return n;
					return { ...n, width: newWidth, height: newHeight };
				}),
				isDirty: true,
			});
		}

		// Set expanded flag in node data (persisted)
		setExpandedFlag(nodeId, true);

		// Add to expanded set
		const newIds = new Set(ids);
		newIds.add(nodeId);
		setTabExpandedSet(tabId, newIds);
	} catch (error) {
		console.error('Failed to expand subgraph:', error);
	}
}

export function collapseSubgraph(nodeId: string): void {
	const tab = get(activeTab);
	if (!tab) return;

	const tabId = get(activeTabId);
	if (!tabId) return;

	const ids = getTabExpandedSet(tabId);
	if (!ids.has(nodeId)) return;

	const key = cacheKey(tabId, nodeId);
	const cached = contentCache.get(key);

	// Restore original dimensions
	if (cached) {
		updateActiveTab({
			nodes: tab.nodes.map(n => {
				if (n.id !== nodeId) return n;
				const restored: FlowNode = { ...n };
				if (cached.originalWidth != null) {
					restored.width = cached.originalWidth;
				} else {
					delete (restored as Record<string, unknown>).width;
				}
				if (cached.originalHeight != null) {
					restored.height = cached.originalHeight;
				} else {
					delete (restored as Record<string, unknown>).height;
				}
				return restored;
			}),
			isDirty: true,
		});
	}

	// Clear expanded flag
	setExpandedFlag(nodeId, false);

	// Remove from expanded set
	const newIds = new Set(ids);
	newIds.delete(nodeId);
	setTabExpandedSet(tabId, newIds);

	// Clean up cache
	contentCache.delete(key);
}

export async function toggleSubgraphExpansion(nodeId: string): Promise<void> {
	const tabId = get(activeTabId);
	if (!tabId) return;

	const ids = getTabExpandedSet(tabId);
	if (ids.has(nodeId)) {
		collapseSubgraph(nodeId);
	} else {
		await expandSubgraph(nodeId);
	}
}

function setExpandedFlag(nodeId: string, expanded: boolean): void {
	const tab = get(activeTab);
	if (!tab) return;

	const node = tab.nodes.find(n => n.id === nodeId);
	if (!node) return;

	const config = ((node.data as Record<string, unknown>)._config || {}) as Record<string, unknown>;
	const extra = (config.extra || {}) as Record<string, unknown>;
	const ui = (extra.ui || {}) as Record<string, unknown>;

	const newUi = expanded
		? { ...ui, expanded: true }
		: (() => { const { expanded: _e, ...rest } = ui; return rest; })();

	updateNodeDataLive(nodeId, {
		_config: {
			...config,
			extra: {
				...extra,
				ui: newUi,
			},
		},
	} as Partial<SubgraphNodeData>);
}

// ── Expand All / Collapse All ──────────────────────────────────

export async function expandAllSubgraphs(): Promise<void> {
	const tab = get(activeTab);
	if (!tab) return;

	const subgraphNodes = tab.nodes.filter(n => n.data.nodeType === 'subgraph');
	for (const node of subgraphNodes) {
		await expandSubgraph(node.id);
	}
}

export function collapseAllSubgraphs(): void {
	const tabId = get(activeTabId);
	if (!tabId) return;

	const ids = getTabExpandedSet(tabId);
	for (const nodeId of ids) {
		collapseSubgraph(nodeId);
	}
}

// ── Compute expanded view ──────────────────────────────────────

/**
 * Derived store that computes the full visible nodes/edges by injecting
 * expanded subgraph children into the flat node list.
 */
export const expandedView = derived(
	[nodes, edges, expandedSubgraphIds, activeTabId],
	([$nodes, $edges, $expandedIds, $tabId]) => {
		if ($expandedIds.size === 0) {
			return { allNodes: $nodes, allEdges: $edges };
		}

		const allNodes: FlowNode[] = [...$nodes];
		const allEdges: NetrunEdge[] = [...$edges];

		for (const subgraphId of $expandedIds) {
			const key = cacheKey($tabId!, subgraphId);
			const cached = contentCache.get(key);
			if (!cached) continue;

			// Add child nodes with parentId and namespaced IDs
			for (const child of cached.nodes) {
				const childId = makeChildNodeId(subgraphId, child.id);
				allNodes.push({
					...child,
					id: childId,
					parentId: subgraphId,
					extent: 'parent',
					position: {
						x: child.position.x + PADDING,
						y: child.position.y + HEADER_OFFSET_Y + PADDING,
					},
					data: {
						...child.data,
						// Tag the data so we can identify this is a child node
					},
				} as FlowNode);
			}

			// Add internal edges with namespaced IDs
			for (const edge of cached.edges) {
				allEdges.push({
					...edge,
					id: makeChildNodeId(subgraphId, edge.id),
					source: makeChildNodeId(subgraphId, edge.source),
					target: makeChildNodeId(subgraphId, edge.target),
				});
			}

			// Add exposed port edges (connecting subgraph ports to internal nodes)
			const parentNode = $nodes.find(n => n.id === subgraphId);
			if (parentNode && parentNode.data.nodeType === 'subgraph') {
				const subgraphData = parentNode.data as SubgraphNodeData;
				const sgConfig = subgraphData._subgraphConfig;
				if (sgConfig) {
					// Exposed input ports: subgraph → internal node
					const exposedIn = sgConfig.exposed_in_ports as Record<string, { internal_node: string; internal_port: string }> | undefined;
					if (exposedIn) {
						for (const [exposedPortName, mapping] of Object.entries(exposedIn)) {
							allEdges.push({
								id: `${subgraphId}::exposed-in::${exposedPortName}`,
								source: subgraphId,
								sourceHandle: exposedPortName,
								target: makeChildNodeId(subgraphId, mapping.internal_node),
								targetHandle: mapping.internal_port,
								type: 'smoothstep',
								animated: true,
							});
						}
					}

					// Exposed output ports: internal node → subgraph
					const exposedOut = sgConfig.exposed_out_ports as Record<string, { internal_node: string; internal_port: string }> | undefined;
					if (exposedOut) {
						for (const [exposedPortName, mapping] of Object.entries(exposedOut)) {
							allEdges.push({
								id: `${subgraphId}::exposed-out::${exposedPortName}`,
								source: makeChildNodeId(subgraphId, mapping.internal_node),
								sourceHandle: mapping.internal_port,
								target: subgraphId,
								targetHandle: exposedPortName,
								type: 'smoothstep',
								animated: true,
							});
						}
					}
				}
			}
		}

		return { allNodes, allEdges };
	}
);

// Keep allVisibleNodes and allVisibleEdges in sync with expanded view
expandedView.subscribe(({ allNodes, allEdges }) => {
	allVisibleNodes.set(allNodes);
	allVisibleEdges.set(allEdges);
});

// ── Child node operations (route to _subgraphConfig) ───────────

/**
 * Update a child node's position in the parent's _subgraphConfig.
 * Called by FlowEditor.onNodeDragStop for child nodes.
 */
export function updateExpandedChildPosition(
	childNodeId: string,
	newPosition: { x: number; y: number }
): void {
	const parentId = getParentSubgraphId(childNodeId);
	const originalId = getOriginalChildId(childNodeId);

	const tab = get(activeTab);
	if (!tab) return;

	const tabId = get(activeTabId);
	if (!tabId) return;

	// Update cache
	const key = cacheKey(tabId, parentId);
	const cached = contentCache.get(key);
	if (cached) {
		const childNode = cached.nodes.find(n => n.id === originalId);
		if (childNode) {
			// Store position without the offset (raw internal position)
			childNode.position = {
				x: newPosition.x - PADDING,
				y: newPosition.y - HEADER_OFFSET_Y - PADDING,
			};
		}
	}

	// Update _subgraphConfig on the parent node
	const parentNode = tab.nodes.find(n => n.id === parentId);
	if (!parentNode || parentNode.data.nodeType !== 'subgraph') return;

	const subgraphData = parentNode.data as SubgraphNodeData;
	const config = subgraphData._subgraphConfig;
	if (!config) return;

	const configNodes = config.nodes as Array<Record<string, unknown>> | undefined;
	if (!configNodes) return;

	const updatedNodes = configNodes.map(cn => {
		if (cn.name !== originalId) return cn;
		const existingExtra = (cn.extra || {}) as Record<string, unknown>;
		const existingUi = (existingExtra.ui || {}) as Record<string, unknown>;
		return {
			...cn,
			extra: {
				...existingExtra,
				ui: {
					...existingUi,
					position: {
						x: newPosition.x - PADDING,
						y: newPosition.y - HEADER_OFFSET_Y - PADDING,
					},
				},
			},
		};
	});

	updateNodeDataLive(parentId, {
		_subgraphConfig: {
			...config,
			nodes: updatedNodes,
		},
	} as Partial<SubgraphNodeData>);
}

/**
 * Delete child nodes/edges from parent's _subgraphConfig.
 * Called by FlowEditor.onDelete for child items.
 */
export function deleteExpandedChildren(
	childNodeIds: string[],
	childEdgeIds: string[]
): void {
	// Group by parent subgraph
	const byParent = new Map<string, { nodeIds: Set<string>; edgeIds: Set<string> }>();

	for (const id of childNodeIds) {
		const parentId = getParentSubgraphId(id);
		const originalId = getOriginalChildId(id);
		if (!byParent.has(parentId)) {
			byParent.set(parentId, { nodeIds: new Set(), edgeIds: new Set() });
		}
		byParent.get(parentId)!.nodeIds.add(originalId);
	}

	for (const id of childEdgeIds) {
		const parentId = getParentSubgraphId(id);
		const originalId = getOriginalChildId(id);
		if (!byParent.has(parentId)) {
			byParent.set(parentId, { nodeIds: new Set(), edgeIds: new Set() });
		}
		byParent.get(parentId)!.edgeIds.add(originalId);
	}

	const tab = get(activeTab);
	if (!tab) return;

	const tabId = get(activeTabId);
	if (!tabId) return;

	pushHistory();

	for (const [parentId, { nodeIds, edgeIds }] of byParent) {
		// Update cache
		const key = cacheKey(tabId, parentId);
		const cached = contentCache.get(key);
		if (cached) {
			cached.nodes = cached.nodes.filter(n => !nodeIds.has(n.id));
			cached.edges = cached.edges.filter(e => !edgeIds.has(e.id));
			// Also remove edges connected to deleted nodes
			cached.edges = cached.edges.filter(e =>
				!nodeIds.has(e.source) && !nodeIds.has(e.target)
			);
		}

		// Update _subgraphConfig
		const parentNode = tab.nodes.find(n => n.id === parentId);
		if (!parentNode || parentNode.data.nodeType !== 'subgraph') continue;

		const subgraphData = parentNode.data as SubgraphNodeData;
		const config = subgraphData._subgraphConfig;
		if (!config) continue;

		const configNodes = (config.nodes as Array<Record<string, unknown>> | undefined) || [];
		const configEdges = (config.edges as Array<Record<string, unknown>> | undefined) || [];

		const updatedNodes = configNodes.filter(cn => !nodeIds.has(cn.name as string));
		const updatedEdges = configEdges.filter(ce => {
			const srcStr = ce.source_str as string | undefined;
			const tgtStr = ce.target_str as string | undefined;
			if (!srcStr || !tgtStr) return true;
			// source_str format: "nodeName.portName"
			const srcNode = srcStr.split('.')[0];
			const tgtNode = tgtStr.split('.')[0];
			// Remove if edge references deleted node or is in the edgeIds
			if (nodeIds.has(srcNode) || nodeIds.has(tgtNode)) return false;
			// Check if edge itself is targeted for deletion
			// Edge IDs in config don't have explicit IDs, so we skip this check
			return true;
		});

		updateNodeDataLive(parentId, {
			_subgraphConfig: {
				...config,
				nodes: updatedNodes,
				edges: updatedEdges,
			},
			nodeCount: updatedNodes.length,
		} as Partial<SubgraphNodeData>);
	}
}

/**
 * Add an edge between two child nodes in the same expanded subgraph.
 * Called by FlowEditor.onConnect for internal connections.
 */
export function addExpandedChildEdge(connection: {
	source: string;
	target: string;
	sourceHandle?: string | null;
	targetHandle?: string | null;
}): void {
	const sourceParent = getParentSubgraphId(connection.source);
	const targetParent = getParentSubgraphId(connection.target);

	// Must be in the same subgraph
	if (sourceParent !== targetParent) return;

	const parentId = sourceParent;
	const originalSource = getOriginalChildId(connection.source);
	const originalTarget = getOriginalChildId(connection.target);

	const tab = get(activeTab);
	if (!tab) return;

	const tabId = get(activeTabId);
	if (!tabId) return;

	// Update cache
	const key = cacheKey(tabId, parentId);
	const cached = contentCache.get(key);
	if (cached) {
		cached.edges.push({
			id: `edge-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
			source: originalSource,
			target: originalTarget,
			sourceHandle: connection.sourceHandle ?? undefined,
			targetHandle: connection.targetHandle ?? undefined,
			type: 'smoothstep',
		});
	}

	// Update _subgraphConfig
	const parentNode = tab.nodes.find(n => n.id === parentId);
	if (!parentNode || parentNode.data.nodeType !== 'subgraph') return;

	const subgraphData = parentNode.data as SubgraphNodeData;
	const config = subgraphData._subgraphConfig;
	if (!config) return;

	const configEdges = (config.edges as Array<Record<string, unknown>> | undefined) || [];

	pushHistory();
	updateNodeDataLive(parentId, {
		_subgraphConfig: {
			...config,
			edges: [
				...configEdges,
				{
					source_str: `${originalSource}.${connection.sourceHandle || 'out'}`,
					target_str: `${originalTarget}.${connection.targetHandle || 'in'}`,
				},
			],
		},
	} as Partial<SubgraphNodeData>);
}

// ── Restore expansion state on load ────────────────────────────

/**
 * Scan nodes for expanded flag and restore expansion state.
 * Called after loading a file or switching tabs.
 */
export async function restoreExpansionState(): Promise<void> {
	const tab = get(activeTab);
	if (!tab) return;

	for (const node of tab.nodes) {
		if (node.data.nodeType !== 'subgraph') continue;
		const ui = getUiConfig(node.data);
		if (ui.expanded === true) {
			await expandSubgraph(node.id);
		}
	}
}

// ── Cleanup on tab close ───────────────────────────────────────

/**
 * Clean up expansion state when a subgraph node is deleted.
 */
export function cleanupExpandedSubgraph(nodeId: string): void {
	const tabId = get(activeTabId);
	if (!tabId) return;

	const ids = getTabExpandedSet(tabId);
	if (ids.has(nodeId)) {
		const newIds = new Set(ids);
		newIds.delete(nodeId);
		setTabExpandedSet(tabId, newIds);
		contentCache.delete(cacheKey(tabId, nodeId));
	}
}

/**
 * Check if a subgraph node is currently expanded.
 */
export function isSubgraphExpanded(nodeId: string): boolean {
	const tabId = get(activeTabId);
	if (!tabId) return false;
	return getTabExpandedSet(tabId).has(nodeId);
}

// ── Child node handlers (registered with flowStore) ────────────

/**
 * Persist child node UI state to the parent's _subgraphConfig.
 */
function persistChildUiToConfig(
	parentId: string,
	originalId: string,
	uiUpdates: Record<string, unknown>
): void {
	const tab = get(activeTab);
	if (!tab) return;

	const parentNode = tab.nodes.find(n => n.id === parentId);
	if (!parentNode || parentNode.data.nodeType !== 'subgraph') return;

	const subgraphData = parentNode.data as SubgraphNodeData;
	const config = subgraphData._subgraphConfig;
	if (!config) return;

	const configNodes = (config.nodes as Array<Record<string, unknown>>) || [];
	const updatedNodes = configNodes.map(cn => {
		if (cn.name !== originalId) return cn;
		const existingExtra = (cn.extra || {}) as Record<string, unknown>;
		const existingUi = (existingExtra.ui || {}) as Record<string, unknown>;
		return {
			...cn,
			extra: {
				...existingExtra,
				ui: {
					...existingUi,
					...uiUpdates,
				},
			},
		};
	});

	// updateNodeDataLive on the parent (not a child node) goes through the normal path
	updateNodeDataLive(parentId, {
		_subgraphConfig: {
			...config,
			nodes: updatedNodes,
		},
	} as Partial<SubgraphNodeData>);
}

/**
 * Handle data updates for expanded child nodes.
 * Updates the content cache and persists UI state to parent's _subgraphConfig.
 */
function handleChildNodeDataUpdate(childNodeId: string, dataUpdates: Partial<AnyNodeData>): void {
	const parentId = getParentSubgraphId(childNodeId);
	const originalId = getOriginalChildId(childNodeId);

	const tabId = get(activeTabId);
	if (!tabId) return;

	// 1. Update content cache
	const key = cacheKey(tabId, parentId);
	const cached = contentCache.get(key);
	if (cached) {
		cached.nodes = cached.nodes.map(n => {
			if (n.id !== originalId) return n;
			return { ...n, data: { ...n.data, ...dataUpdates } };
		});
	}

	// 2. Persist UI state from _config to parent's _subgraphConfig
	let persisted = false;
	const updatedConfigRaw = (dataUpdates as Record<string, unknown>)._config;
	if (updatedConfigRaw) {
		const updatedConfig = updatedConfigRaw as Record<string, unknown>;
		const updatedExtra = (updatedConfig.extra || {}) as Record<string, unknown>;
		const updatedUi = (updatedExtra.ui || {}) as Record<string, unknown>;

		if (Object.keys(updatedUi).length > 0) {
			// This calls updateNodeDataLive on parent, which triggers nodes → expandedView recompute
			persistChildUiToConfig(parentId, originalId, updatedUi);
			persisted = true;
		}
	}

	// 3. If nothing was persisted to parent, force expandedView recompute manually
	if (!persisted) {
		expandedByTab.update(map => new Map(map));
	}
}

/**
 * Handle dimension updates for expanded child nodes.
 * Updates the content cache and persists dimensions to parent's _subgraphConfig.
 */
function handleChildNodeDimensionUpdate(
	updates: Array<{ id: string; width: number; height: number; position: { x: number; y: number } }>
): void {
	const tabId = get(activeTabId);
	if (!tabId) return;

	// Group by parent subgraph
	const byParent = new Map<string, Array<{ originalId: string; width: number; height: number; position: { x: number; y: number } }>>();

	for (const u of updates) {
		const parentId = getParentSubgraphId(u.id);
		const originalId = getOriginalChildId(u.id);
		if (!byParent.has(parentId)) byParent.set(parentId, []);
		byParent.get(parentId)!.push({ originalId, width: u.width, height: u.height, position: u.position });
	}

	for (const [parentId, childUpdates] of byParent) {
		// Update cache
		const key = cacheKey(tabId, parentId);
		const cached = contentCache.get(key);
		if (cached) {
			for (const cu of childUpdates) {
				const child = cached.nodes.find(n => n.id === cu.originalId);
				if (child) {
					child.width = cu.width;
					child.height = cu.height;
					// Store raw position (without offset)
					child.position = {
						x: cu.position.x - PADDING,
						y: cu.position.y - HEADER_OFFSET_Y - PADDING,
					};
				}
			}
		}

		// Persist to _subgraphConfig
		const tab = get(activeTab);
		if (!tab) continue;

		const parentNode = tab.nodes.find(n => n.id === parentId);
		if (!parentNode || parentNode.data.nodeType !== 'subgraph') continue;

		const subgraphData = parentNode.data as SubgraphNodeData;
		const config = subgraphData._subgraphConfig;
		if (!config) continue;

		const configNodes = (config.nodes as Array<Record<string, unknown>>) || [];
		const updatedConfigNodes = configNodes.map(cn => {
			const update = childUpdates.find(u => u.originalId === (cn.name as string));
			if (!update) return cn;
			const existingExtra = (cn.extra || {}) as Record<string, unknown>;
			const existingUi = (existingExtra.ui || {}) as Record<string, unknown>;
			return {
				...cn,
				extra: {
					...existingExtra,
					ui: {
						...existingUi,
						dimensions: { width: update.width, height: update.height },
						position: {
							x: update.position.x - PADDING,
							y: update.position.y - HEADER_OFFSET_Y - PADDING,
						},
					},
				},
			};
		});

		updateNodeDataLive(parentId, {
			_subgraphConfig: {
				...config,
				nodes: updatedConfigNodes,
			},
		} as Partial<SubgraphNodeData>);
	}
}

// ── Register handlers with flowStore ───────────────────────────
registerChildNodeHandlers(handleChildNodeDataUpdate, handleChildNodeDimensionUpdate);
