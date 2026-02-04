/**
 * File explorer state and events
 *
 * Used to trigger refresh of the file explorer from other components.
 */
import { writable } from 'svelte/store';

// Counter that increments to trigger refresh
export const fileExplorerRefreshTrigger = writable(0);

/**
 * Trigger a refresh of the file explorer
 */
export function triggerFileExplorerRefresh(): void {
	fileExplorerRefreshTrigger.update(n => n + 1);
}
