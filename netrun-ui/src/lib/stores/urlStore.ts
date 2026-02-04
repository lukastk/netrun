/**
 * URL Store - Manages browser URL state for deep linking
 *
 * Provides utilities for:
 * - Updating URL when files are opened/closed
 * - Updating URL when tabs are switched
 * - Preserving URL state across navigation
 */
import { browser } from '$app/environment';
import { goto } from '$app/navigation';

/**
 * Update the URL to reflect the currently active file
 * Uses replaceState to avoid creating new history entries for every tab switch
 */
export function updateUrlWithFile(filePath: string | null): void {
	if (!browser) return;

	const url = new URL(window.location.href);

	if (filePath) {
		// Set the file parameter to the active file
		url.searchParams.set('file', filePath);
	} else {
		// No active file, remove the parameter
		url.searchParams.delete('file');
	}

	// Remove node parameter when switching files
	url.searchParams.delete('node');

	// Use replaceState to update URL without navigation
	goto(url.toString(), {
		replaceState: true,
		noScroll: true,
		keepFocus: true,
	});
}

/**
 * Update URL to show multiple open files
 * The first file in the array is considered the active one
 */
export function updateUrlWithFiles(filePaths: string[]): void {
	if (!browser) return;

	const url = new URL(window.location.href);

	// Clear existing file parameters
	url.searchParams.delete('file');

	// Add each file path
	filePaths.forEach(path => {
		if (path) {
			url.searchParams.append('file', path);
		}
	});

	// Remove node parameter
	url.searchParams.delete('node');

	goto(url.toString(), {
		replaceState: true,
		noScroll: true,
		keepFocus: true,
	});
}

/**
 * Update URL with a selected node
 * Useful for sharing links that highlight specific nodes
 */
export function updateUrlWithNode(nodeName: string | null): void {
	if (!browser) return;

	const url = new URL(window.location.href);

	if (nodeName) {
		url.searchParams.set('node', nodeName);
	} else {
		url.searchParams.delete('node');
	}

	goto(url.toString(), {
		replaceState: true,
		noScroll: true,
		keepFocus: true,
	});
}

/**
 * Clear all netrun-specific query parameters from URL
 */
export function clearUrlParams(): void {
	if (!browser) return;

	const url = new URL(window.location.href);
	url.searchParams.delete('file');
	url.searchParams.delete('node');

	goto(url.toString(), {
		replaceState: true,
		noScroll: true,
		keepFocus: true,
	});
}

/**
 * Get the current file path from URL (if any)
 */
export function getFileFromUrl(): string | null {
	if (!browser) return null;

	const url = new URL(window.location.href);
	return url.searchParams.get('file');
}

/**
 * Get all file paths from URL
 */
export function getFilesFromUrl(): string[] {
	if (!browser) return [];

	const url = new URL(window.location.href);
	return url.searchParams.getAll('file').filter(f => f.length > 0);
}

/**
 * Get the node name from URL (if any)
 */
export function getNodeFromUrl(): string | null {
	if (!browser) return null;

	const url = new URL(window.location.href);
	return url.searchParams.get('node');
}
