/**
 * Tab management for netrun-ui
 *
 * Manages multiple open files with per-tab state isolation.
 */
import { writable, derived, get } from 'svelte/store';
import type { NetrunEdge, SubgraphNodeData, FlowNode, AnyNodeData } from './flowStore';
import { api } from '$lib/api';
import { showConfirm } from '$lib/stores/modalStore';
import { updateUrlWithFile } from '$lib/stores/urlStore';

// History state for undo/redo
interface HistoryState {
	nodes: FlowNode[];
	edges: NetrunEdge[];
}

interface History {
	past: HistoryState[];
	future: HistoryState[];
}

// Context for subgraph tabs
export interface SubgraphContext {
	parentTabId: string;
	nodeId: string;
	path: string[]; // Breadcrumb: ["Root", "Subgraph1", "Nested"]
	isInline: boolean; // True for inline subgraphs, false for file-referenced
}

// Complete state for a single tab
export interface TabState {
	id: string;
	filePath: string | null;
	fileName: string;
	isDirty: boolean;
	nodes: FlowNode[];
	edges: NetrunEdge[];
	history: History;
	extraData: Record<string, unknown> | null;
	graphMeta: Record<string, unknown> | null;
	fileFormat: 'json' | 'toml';
	// Subgraph context (null for root tabs)
	subgraphContext: SubgraphContext | null;
	// True when user has explicitly created a new file (vs initial empty state)
	isNewFile: boolean;
}

// Generate unique tab ID
let tabCounter = 0;
function generateTabId(): string {
	return `tab-${Date.now()}-${tabCounter++}`;
}

// Extract filename from path
function getFileName(filePath: string | null): string {
	if (!filePath) return 'Untitled';
	return filePath.split('/').pop() || 'Untitled';
}

// Create a new empty tab state
export function createEmptyTabState(filePath?: string | null): TabState {
	return {
		id: generateTabId(),
		filePath: filePath || null,
		fileName: getFileName(filePath || null),
		isDirty: false,
		nodes: [],
		edges: [],
		history: { past: [], future: [] },
		extraData: null,
		graphMeta: null,
		fileFormat: 'json',
		subgraphContext: null,
		isNewFile: false,
	};
}

// Main stores
export const tabs = writable<TabState[]>([createEmptyTabState()]);
export const activeTabId = writable<string | null>(null);

// Initialize activeTabId to first tab
tabs.subscribe(tabList => {
	const currentActiveId = get(activeTabId);
	if (tabList.length > 0 && (!currentActiveId || !tabList.find(t => t.id === currentActiveId))) {
		activeTabId.set(tabList[0].id);
	}
});

// Derived: active tab state
export const activeTab = derived(
	[tabs, activeTabId],
	([$tabs, $activeTabId]) => $tabs.find(t => t.id === $activeTabId) || null
);

// Derived: active tab index
export const activeTabIndex = derived(
	[tabs, activeTabId],
	([$tabs, $activeTabId]) => $tabs.findIndex(t => t.id === $activeTabId)
);

// Create a new tab and optionally switch to it
export function createTab(filePath?: string | null, switchTo: boolean = true): string {
	const newTab = createEmptyTabState(filePath);

	tabs.update(t => [...t, newTab]);

	if (switchTo) {
		activeTabId.set(newTab.id);
	}

	return newTab.id;
}

// Switch to a tab by ID
export function switchTab(tabId: string): void {
	const tabList = get(tabs);
	const targetTab = tabList.find(t => t.id === tabId);
	if (targetTab) {
		activeTabId.set(tabId);
		// Update URL to reflect the active file
		updateUrlWithFile(targetTab.filePath);
	}
}

// Switch to tab by index (0-based)
export function switchToTabIndex(index: number): void {
	const tabList = get(tabs);
	if (index >= 0 && index < tabList.length) {
		const targetTab = tabList[index];
		activeTabId.set(targetTab.id);
		updateUrlWithFile(targetTab.filePath);
	}
}

// Switch to next/previous tab
export function switchToNextTab(): void {
	const tabList = get(tabs);
	const currentIndex = get(activeTabIndex);
	let targetTab: TabState;
	if (currentIndex < tabList.length - 1) {
		targetTab = tabList[currentIndex + 1];
	} else if (tabList.length > 0) {
		// Wrap around to first tab
		targetTab = tabList[0];
	} else {
		return;
	}
	activeTabId.set(targetTab.id);
	updateUrlWithFile(targetTab.filePath);
}

export function switchToPreviousTab(): void {
	const tabList = get(tabs);
	const currentIndex = get(activeTabIndex);
	let targetTab: TabState;
	if (currentIndex > 0) {
		targetTab = tabList[currentIndex - 1];
	} else if (tabList.length > 0) {
		// Wrap around to last tab
		targetTab = tabList[tabList.length - 1];
	} else {
		return;
	}
	activeTabId.set(targetTab.id);
	updateUrlWithFile(targetTab.filePath);
}

// Close a tab by ID
// Returns true if tab was closed, false if user cancelled
export async function closeTab(tabId: string, confirmUnsaved: boolean = true): Promise<boolean> {
	const tabList = get(tabs);
	const tabToClose = tabList.find(t => t.id === tabId);

	if (!tabToClose) return false;

	// Check for unsaved changes
	if (confirmUnsaved && tabToClose.isDirty) {
		const confirmed = await showConfirm({
			title: 'Unsaved Changes',
			message: `"${tabToClose.fileName}" has unsaved changes. Close anyway?`,
			confirmText: 'Close',
			cancelText: 'Cancel',
		});
		if (!confirmed) {
			return false;
		}
	}

	// If this is the last tab, create a new empty one
	if (tabList.length === 1) {
		const newTab = createEmptyTabState();
		tabs.set([newTab]);
		activeTabId.set(newTab.id);
		// Clear URL since there's no file open
		updateUrlWithFile(null);
		return true;
	}

	// Find index of tab being closed
	const closingIndex = tabList.findIndex(t => t.id === tabId);
	const currentActiveId = get(activeTabId);

	// Remove the tab
	tabs.update(t => t.filter(tab => tab.id !== tabId));

	// If we closed the active tab, switch to adjacent one
	if (currentActiveId === tabId) {
		const newTabList = get(tabs);
		// Prefer the tab to the left, or the first one if we closed the leftmost
		const newIndex = Math.min(closingIndex, newTabList.length - 1);
		const newActiveTab = newTabList[newIndex];
		activeTabId.set(newActiveTab.id);
		// Update URL to reflect new active file
		updateUrlWithFile(newActiveTab.filePath);
	}

	return true;
}

// Close the active tab
export async function closeActiveTab(confirmUnsaved: boolean = true): Promise<boolean> {
	const currentActiveId = get(activeTabId);
	if (currentActiveId) {
		return await closeTab(currentActiveId, confirmUnsaved);
	}
	return false;
}

// Find tab by file path
export function getTabByFilePath(filePath: string): TabState | undefined {
	return get(tabs).find(t => t.filePath === filePath);
}

// Update the active tab's state
export function updateActiveTab(updates: Partial<TabState>): void {
	const currentActiveId = get(activeTabId);
	if (!currentActiveId) return;

	tabs.update(tabList =>
		tabList.map(tab =>
			tab.id === currentActiveId
				? { ...tab, ...updates }
				: tab
		)
	);
}

// Update a specific tab's state by ID
export function updateTab(tabId: string, updates: Partial<TabState>): void {
	tabs.update(tabList =>
		tabList.map(tab =>
			tab.id === tabId
				? { ...tab, ...updates }
				: tab
		)
	);
}

// Set active tab's nodes
export function setActiveTabNodes(nodes: FlowNode[]): void {
	updateActiveTab({ nodes });
}

// Set active tab's edges
export function setActiveTabEdges(edges: NetrunEdge[]): void {
	updateActiveTab({ edges });
}

// Mark active tab as dirty
export function markActiveTabDirty(): void {
	updateActiveTab({ isDirty: true });
}

// Mark active tab as clean
export function markActiveTabClean(): void {
	updateActiveTab({ isDirty: false });
}

// Check if any tab has unsaved changes
export function hasUnsavedChanges(): boolean {
	return get(tabs).some(t => t.isDirty);
}

// Open a subgraph in a new tab
export async function openSubgraphTab(nodeId: string, data: SubgraphNodeData): Promise<string | null> {
	const parentTab = get(activeTab);
	if (!parentTab) return null;

	// Check if this subgraph is already open in a tab
	const existingTab = get(tabs).find(
		t => t.subgraphContext?.parentTabId === parentTab.id && t.subgraphContext?.nodeId === nodeId
	);

	if (existingTab) {
		// Switch to existing tab
		switchTab(existingTab.id);
		return existingTab.id;
	}

	try {
		// Load subgraph content
		const subgraphConfig = data._subgraphConfig;
		let nodes: FlowNode[] = [];
		let edges: NetrunEdge[] = [];

		// Get the root file path for resolving relative subgraph paths
		// We need to find the root parent tab (the one with the actual file)
		let rootTab = parentTab;
		while (rootTab.subgraphContext) {
			const parent = get(tabs).find(t => t.id === rootTab.subgraphContext!.parentTabId);
			if (parent) {
				rootTab = parent;
			} else {
				break;
			}
		}
		const basePath = rootTab.filePath || undefined;

		// Determine if this is an inline or file-referenced subgraph
		// subgraphConfig may have 'path' (file ref) or 'nodes' (inline)
		const configPath = subgraphConfig?.path as string | undefined;
		const configNodes = subgraphConfig?.nodes as unknown[] | undefined;
		const isFileReference = configPath || (data.source && data.source !== 'Inline');
		// An inline subgraph has nodes array (even if empty) or no path specified
		const isInline = !isFileReference;

		let resolvedFilePath: string | null = null;

		if (isFileReference) {
			// Load from file - use path from config or source
			const filePath = configPath || data.source;
			const response = await api.loadSubgraph(filePath, undefined, basePath);
			nodes = response.nodes.map(node => ({
				id: node.id,
				type: node.type as 'netrunNode' | 'subgraphNode',
				position: node.position,
				data: node.data
			})) as FlowNode[];
			edges = response.edges;
			// The API returns the resolved absolute path in response.source
			resolvedFilePath = response.source;
		} else if (isInline) {
			// Load from inline config - may be empty if subgraph has no nodes yet
			if (subgraphConfig && (configNodes || subgraphConfig.edges)) {
				const response = await api.loadSubgraph(undefined, subgraphConfig, basePath);
				nodes = response.nodes.map(node => ({
					id: node.id,
					type: node.type as 'netrunNode' | 'subgraphNode',
					position: node.position,
					data: node.data
				})) as FlowNode[];
				edges = response.edges;
			}
			// If no subgraphConfig or empty, nodes/edges stay as empty arrays
			// which is correct for an empty inline subgraph
			resolvedFilePath = null;
		}

		// Build breadcrumb path
		const parentPath = parentTab.subgraphContext?.path || [parentTab.fileName];
		const path = [...parentPath, data.label];

		// Create new tab
		const newTabId = generateTabId();
		const newTab: TabState = {
			id: newTabId,
			filePath: resolvedFilePath, // File-ref subgraphs have their own file path
			fileName: data.label,
			isDirty: false,
			nodes,
			edges,
			history: { past: [], future: [] },
			extraData: null,
			graphMeta: null,
			fileFormat: parentTab.fileFormat,
			subgraphContext: {
				parentTabId: parentTab.id,
				nodeId,
				path,
				isInline,
			},
			isNewFile: false,
		};

		tabs.update(t => [...t, newTab]);
		activeTabId.set(newTabId);

		return newTabId;
	} catch (error) {
		console.error('Failed to open subgraph:', error);
		return null;
	}
}

// Get the breadcrumb path for the active tab
export function getActiveBreadcrumb(): string[] {
	const tab = get(activeTab);
	if (!tab) return [];
	if (tab.subgraphContext) {
		return tab.subgraphContext.path;
	}
	return [tab.fileName];
}

// Navigate to a specific level in the breadcrumb
export function navigateToBreadcrumb(index: number): void {
	const tab = get(activeTab);
	if (!tab || !tab.subgraphContext) return;

	const path = tab.subgraphContext.path;
	if (index >= path.length - 1) return; // Already at this level

	// Find the tab at the target level
	let targetTabId: string | null = null;

	if (index === 0) {
		// Navigate to root
		// Find the root tab by following parentTabId chain
		let currentTab: TabState | null = tab;
		while (currentTab && currentTab.subgraphContext) {
			targetTabId = currentTab.subgraphContext.parentTabId;
			currentTab = get(tabs).find(t => t.id === targetTabId) || null;
		}
	} else {
		// Navigate to intermediate level
		// We need to find the tab with path matching up to index
		const targetPath = path.slice(0, index + 1);
		const tabList = get(tabs);

		for (const t of tabList) {
			if (!t.subgraphContext) continue;
			const tPath = t.subgraphContext.path;
			if (tPath.length === targetPath.length && tPath.every((p, i) => p === targetPath[i])) {
				targetTabId = t.id;
				break;
			}
		}
	}

	if (targetTabId) {
		switchTab(targetTabId);
	}
}
