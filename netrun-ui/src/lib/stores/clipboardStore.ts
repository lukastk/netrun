/**
 * Clipboard store for copy/paste operations
 *
 * Maintains a clipboard of copied nodes that works across tabs.
 */
import { writable, get } from 'svelte/store';
import type { NetrunNode } from './flowStore';

interface ClipboardState {
	nodes: NetrunNode[];
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
export function copyToClipboard(nodes: NetrunNode[], sourceTabId: string | null): void {
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
export function getClipboardNodes(): NetrunNode[] {
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
