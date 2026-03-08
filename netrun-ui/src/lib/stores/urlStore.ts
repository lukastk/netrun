/**
 * URL Store - Manages browser URL state for deep linking
 *
 * Provides utilities for:
 * - Updating URL when files are opened/closed
 * - Updating URL when tabs are switched
 * - Preserving URL state across navigation
 */
const isBrowser = typeof window !== 'undefined';

function replaceUrl(url: string): void {
	if (!isBrowser) return;
	try {
		window.history.replaceState({}, '', url);
	} catch {
		// ignore — may fail in VS Code webview or other restricted environments
	}
}

/**
 * Update the URL to reflect the currently active file
 * Uses replaceState to avoid creating new history entries for every tab switch
 */
export function updateUrlWithFile(filePath: string | null): void {
	if (!isBrowser) return;

	const url = new URL(window.location.href);

	if (filePath) {
		url.searchParams.set('file', filePath);
	} else {
		url.searchParams.delete('file');
	}

	url.searchParams.delete('node');

	replaceUrl(url.toString());
}

/**
 * Update URL to show multiple open files
 * The first file in the array is considered the active one
 */
export function updateUrlWithFiles(filePaths: string[]): void {
	if (!isBrowser) return;

	const url = new URL(window.location.href);

	url.searchParams.delete('file');

	filePaths.forEach(path => {
		if (path) {
			url.searchParams.append('file', path);
		}
	});

	url.searchParams.delete('node');

	replaceUrl(url.toString());
}

/**
 * Update URL with a selected node
 * Useful for sharing links that highlight specific nodes
 */
export function updateUrlWithNode(nodeName: string | null): void {
	if (!isBrowser) return;

	const url = new URL(window.location.href);

	if (nodeName) {
		url.searchParams.set('node', nodeName);
	} else {
		url.searchParams.delete('node');
	}

	replaceUrl(url.toString());
}

/**
 * Clear all netrun-specific query parameters from URL
 */
export function clearUrlParams(): void {
	if (!isBrowser) return;

	const url = new URL(window.location.href);
	url.searchParams.delete('file');
	url.searchParams.delete('node');

	replaceUrl(url.toString());
}

/**
 * Get the current file path from URL (if any)
 */
export function getFileFromUrl(): string | null {
	if (!isBrowser) return null;

	const url = new URL(window.location.href);
	return url.searchParams.get('file');
}

/**
 * Get all file paths from URL
 */
export function getFilesFromUrl(): string[] {
	if (!isBrowser) return [];

	const url = new URL(window.location.href);
	return url.searchParams.getAll('file').filter(f => f.length > 0);
}

/**
 * Get the node name from URL (if any)
 */
export function getNodeFromUrl(): string | null {
	if (!isBrowser) return null;

	const url = new URL(window.location.href);
	return url.searchParams.get('node');
}
