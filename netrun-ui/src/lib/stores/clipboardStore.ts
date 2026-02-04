/**
 * Clipboard store for copy/paste operations
 *
 * Maintains a clipboard of copied nodes that works across tabs.
 */
import { writable, get } from 'svelte/store';
import type { FlowNode } from './flowStore';

interface ClipboardState {
	nodes: FlowNode[];
	sourceTabId: string | null;
}

// Clipboard store - persists copied nodes
export const clipboard = writable<ClipboardState>({
	nodes: [],
	sourceTabId: null,
});

/**
 * Copy nodes to clipboard
 */
export function copyToClipboard(nodes: FlowNode[], sourceTabId: string | null): void {
	// Deep clone nodes to avoid reference issues
	const clonedNodes = nodes.map(node => ({
		...node,
		data: { ...node.data },
		position: { ...node.position },
	}));

	clipboard.set({
		nodes: clonedNodes,
		sourceTabId,
	});
}

/**
 * Get nodes from clipboard
 */
export function getClipboardNodes(): FlowNode[] {
	return get(clipboard).nodes;
}

/**
 * Check if clipboard has content
 */
export function hasClipboardContent(): boolean {
	return get(clipboard).nodes.length > 0;
}

/**
 * Clear clipboard
 */
export function clearClipboard(): void {
	clipboard.set({
		nodes: [],
		sourceTabId: null,
	});
}
