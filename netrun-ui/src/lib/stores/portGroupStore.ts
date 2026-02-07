/**
 * Port group collapse/expand state management.
 *
 * Stores ephemeral UI state for which port groups are collapsed.
 * Not saved to file — groups auto-collapse based on port count threshold.
 */
import { writable, get } from 'svelte/store';

/** Default: groups with 3+ ports start collapsed */
const AUTO_COLLAPSE_THRESHOLD = 3;

/** Key format: "${nodeId}:${side}:${groupPath}" */
function makeKey(nodeId: string, side: 'in' | 'out', groupPath: string): string {
	return `${nodeId}:${side}:${groupPath}`;
}

/** Map of explicit collapse state overrides. If a key is absent, use default. */
const overrides = writable<Map<string, boolean>>(new Map());

/** Get default collapsed state based on port count. */
export function getDefaultCollapsed(portCount: number): boolean {
	return portCount >= AUTO_COLLAPSE_THRESHOLD;
}

/** Check if a specific group is collapsed. */
export function isGroupCollapsed(nodeId: string, side: 'in' | 'out', groupPath: string, portCount: number): boolean {
	const key = makeKey(nodeId, side, groupPath);
	const map = get(overrides);
	if (map.has(key)) {
		return map.get(key)!;
	}
	return getDefaultCollapsed(portCount);
}

/** Toggle a group's collapsed state. */
export function toggleGroup(nodeId: string, side: 'in' | 'out', groupPath: string, portCount: number): void {
	const key = makeKey(nodeId, side, groupPath);
	overrides.update(map => {
		const newMap = new Map(map);
		const current = map.has(key) ? map.get(key)! : getDefaultCollapsed(portCount);
		newMap.set(key, !current);
		return newMap;
	});
}

/** Explicitly set a group's collapsed state. */
export function setGroupCollapsed(nodeId: string, side: 'in' | 'out', groupPath: string, collapsed: boolean): void {
	const key = makeKey(nodeId, side, groupPath);
	overrides.update(map => {
		const newMap = new Map(map);
		newMap.set(key, collapsed);
		return newMap;
	});
}

/** Clear all overrides (e.g., when switching tabs). */
export function clearPortGroupOverrides(): void {
	overrides.set(new Map());
}

/** Clear overrides for a specific node (e.g., when its ports change). */
export function clearNodePortGroupOverrides(nodeId: string): void {
	overrides.update(map => {
		const newMap = new Map(map);
		for (const key of map.keys()) {
			if (key.startsWith(`${nodeId}:`)) {
				newMap.delete(key);
			}
		}
		return newMap;
	});
}

/**
 * Subscribe to overrides store to be reactive.
 * Returns the store itself so components can use $overrides for reactivity.
 */
export const portGroupOverrides = overrides;
