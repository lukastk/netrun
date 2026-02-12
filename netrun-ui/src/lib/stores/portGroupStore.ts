/**
 * Port group collapse/expand state utilities.
 *
 * Provides stateless helpers for determining port group collapse state
 * from node data stored in _config.extra.ui.portGroups.
 */
import { ROOT_GROUP_PATH } from '$lib/utils/portGroups';

/** Default: groups with 3+ ports start collapsed */
export const AUTO_COLLAPSE_THRESHOLD = 3;

/** Get default collapsed state based on port count. */
export function getDefaultCollapsed(portCount: number): boolean {
	return portCount >= AUTO_COLLAPSE_THRESHOLD;
}

/**
 * Check if a specific port group is collapsed.
 * Uses persisted state from node data (_config.extra.ui.portGroups).
 *
 * @param portGroupStates - The portGroups record from node UI metadata, or undefined
 * @param side - 'in' or 'out'
 * @param groupPath - The group path (e.g. ROOT_GROUP_PATH, "group.subgroup")
 * @param portCount - Number of ports in this group (used for auto-collapse default)
 */
export function isPortGroupCollapsed(
	portGroupStates: Record<string, boolean> | undefined,
	side: 'in' | 'out',
	groupPath: string,
	portCount: number
): boolean {
	const key = `${side}:${groupPath}`;
	if (portGroupStates && key in portGroupStates) {
		return portGroupStates[key];
	}
	// Root groups default to expanded
	if (groupPath === ROOT_GROUP_PATH) {
		return false;
	}
	return getDefaultCollapsed(portCount);
}

/**
 * Extract portGroups state from a node's data object.
 * Works with both NetrunNodeData and SubgraphNodeData.
 */
export function getPortGroupStates(nodeData: Record<string, unknown>): Record<string, boolean> | undefined {
	const config = nodeData._config as Record<string, unknown> | undefined;
	const extra = config?.extra as Record<string, unknown> | undefined;
	const ui = extra?.ui as Record<string, unknown> | undefined;
	return ui?.portGroups as Record<string, boolean> | undefined;
}
