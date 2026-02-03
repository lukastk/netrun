/**
 * Tab management for netrun-ui
 *
 * Manages multiple open files with per-tab state isolation.
 */
import { writable, derived, get } from 'svelte/store';
import type { NetrunNode, NetrunEdge } from './flowStore';

// History state for undo/redo
interface HistoryState {
	nodes: NetrunNode[];
	edges: NetrunEdge[];
}

interface History {
	past: HistoryState[];
	future: HistoryState[];
}

// Complete state for a single tab
export interface TabState {
	id: string;
	filePath: string | null;
	fileName: string;
	isDirty: boolean;
	nodes: NetrunNode[];
	edges: NetrunEdge[];
	history: History;
	extraData: Record<string, unknown> | null;
	graphMeta: Record<string, unknown> | null;
	fileFormat: 'json' | 'toml';
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
	if (tabList.find(t => t.id === tabId)) {
		activeTabId.set(tabId);
	}
}

// Switch to tab by index (0-based)
export function switchToTabIndex(index: number): void {
	const tabList = get(tabs);
	if (index >= 0 && index < tabList.length) {
		activeTabId.set(tabList[index].id);
	}
}

// Switch to next/previous tab
export function switchToNextTab(): void {
	const tabList = get(tabs);
	const currentIndex = get(activeTabIndex);
	if (currentIndex < tabList.length - 1) {
		activeTabId.set(tabList[currentIndex + 1].id);
	} else if (tabList.length > 0) {
		// Wrap around to first tab
		activeTabId.set(tabList[0].id);
	}
}

export function switchToPreviousTab(): void {
	const tabList = get(tabs);
	const currentIndex = get(activeTabIndex);
	if (currentIndex > 0) {
		activeTabId.set(tabList[currentIndex - 1].id);
	} else if (tabList.length > 0) {
		// Wrap around to last tab
		activeTabId.set(tabList[tabList.length - 1].id);
	}
}

// Close a tab by ID
// Returns true if tab was closed, false if user cancelled
export function closeTab(tabId: string, confirmUnsaved: boolean = true): boolean {
	const tabList = get(tabs);
	const tabToClose = tabList.find(t => t.id === tabId);

	if (!tabToClose) return false;

	// Check for unsaved changes
	if (confirmUnsaved && tabToClose.isDirty) {
		if (!confirm(`"${tabToClose.fileName}" has unsaved changes. Close anyway?`)) {
			return false;
		}
	}

	// If this is the last tab, create a new empty one
	if (tabList.length === 1) {
		const newTab = createEmptyTabState();
		tabs.set([newTab]);
		activeTabId.set(newTab.id);
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
		activeTabId.set(newTabList[newIndex].id);
	}

	return true;
}

// Close the active tab
export function closeActiveTab(confirmUnsaved: boolean = true): boolean {
	const currentActiveId = get(activeTabId);
	if (currentActiveId) {
		return closeTab(currentActiveId, confirmUnsaved);
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
export function setActiveTabNodes(nodes: NetrunNode[]): void {
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
