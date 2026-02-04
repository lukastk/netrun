/**
 * File explorer state and events
 *
 * Used to trigger refresh of the file explorer from other components,
 * and to track the current directory for relative path resolution.
 */
import { writable, get } from 'svelte/store';

// Counter that increments to trigger refresh
export const fileExplorerRefreshTrigger = writable(0);

// Current directory path in the file explorer
export const currentExplorerPath = writable<string>('~');

/**
 * Trigger a refresh of the file explorer
 */
export function triggerFileExplorerRefresh(): void {
	fileExplorerRefreshTrigger.update(n => n + 1);
}

/**
 * Set the current explorer path
 */
export function setCurrentExplorerPath(path: string): void {
	currentExplorerPath.set(path);
}

/**
 * Resolve a path - if relative, prepend current explorer directory
 */
export function resolveFilePath(inputPath: string): string {
	if (inputPath.startsWith('/')) {
		// Absolute path
		return inputPath;
	}
	// Relative path - prepend current directory
	const currentDir = get(currentExplorerPath);
	// Handle ~ expansion (will be done by backend, but we need a valid path)
	if (currentDir === '~') {
		return inputPath; // Let backend handle ~ resolution
	}
	return `${currentDir}/${inputPath}`;
}
