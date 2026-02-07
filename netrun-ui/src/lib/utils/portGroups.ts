/**
 * Port group utilities for grouping ports by dot-separated naming convention.
 *
 * Ports named "group.subgroup.port" are organized into a nested tree structure.
 * This is purely a UI concept — the underlying data model is unchanged.
 */
import type { PortConfig, FlowNode } from '$lib/stores/flowStore';

// --- Data structures ---

export interface PortGroupTree {
	type: 'group';
	name: string;        // segment name (e.g., "my_group")
	fullPath: string;    // full dot-joined path (e.g., "a.b")
	children: PortDisplayItem[];
	portCount: number;   // total leaf port count (recursive)
}

export interface PortLeaf {
	type: 'port';
	port: PortConfig;
	depth: number;       // nesting level (0 = top-level)
}

export type PortDisplayItem = PortGroupTree | PortLeaf;

// --- Group handle ID format: "group:{in|out}:{dotted.path}" ---

const GROUP_HANDLE_PREFIX = 'group:';

export function makeGroupHandleId(side: 'in' | 'out', groupPath: string): string {
	return `${GROUP_HANDLE_PREFIX}${side}:${groupPath}`;
}

export function parseGroupHandleId(handleId: string | null | undefined): { side: 'in' | 'out'; groupPath: string } | null {
	if (!handleId || !handleId.startsWith(GROUP_HANDLE_PREFIX)) return null;
	const rest = handleId.slice(GROUP_HANDLE_PREFIX.length);
	const colonIdx = rest.indexOf(':');
	if (colonIdx === -1) return null;
	const side = rest.slice(0, colonIdx);
	if (side !== 'in' && side !== 'out') return null;
	const groupPath = rest.slice(colonIdx + 1);
	if (!groupPath) return null;
	return { side, groupPath };
}

export function isGroupHandle(handleId: string | null | undefined): boolean {
	return parseGroupHandleId(handleId) !== null;
}

// --- Tree building ---

/**
 * Build a display tree from a flat list of ports.
 * Ports with dots in their names are grouped hierarchically.
 * Ports without dots are standalone leaves.
 */
export function buildPortTree(ports: PortConfig[], depth: number = 0, pathPrefix: string = ''): PortDisplayItem[] {
	// Bucket ports by their first segment
	const groups = new Map<string, PortConfig[]>();
	const standalone: PortConfig[] = [];

	for (const port of ports) {
		// Get the "remaining" name relative to the current path prefix
		const relativeName = pathPrefix ? port.name.slice(pathPrefix.length) : port.name;
		const dotIdx = relativeName.indexOf('.');
		if (dotIdx === -1) {
			standalone.push(port);
		} else {
			const firstSegment = relativeName.slice(0, dotIdx);
			if (!groups.has(firstSegment)) {
				groups.set(firstSegment, []);
			}
			groups.get(firstSegment)!.push(port);
		}
	}

	const result: PortDisplayItem[] = [];

	// We want to preserve the original ordering as much as possible.
	// Walk through ports in order and emit items as we encounter their group/standalone position.
	const emittedGroups = new Set<string>();

	for (const port of ports) {
		const relativeName = pathPrefix ? port.name.slice(pathPrefix.length) : port.name;
		const dotIdx = relativeName.indexOf('.');

		if (dotIdx === -1) {
			// Standalone port
			result.push({ type: 'port', port, depth });
		} else {
			const firstSegment = relativeName.slice(0, dotIdx);
			if (!emittedGroups.has(firstSegment)) {
				emittedGroups.add(firstSegment);
				const groupPorts = groups.get(firstSegment)!;
				const fullPath = pathPrefix ? `${pathPrefix}${firstSegment}` : firstSegment;
				const children = buildPortTree(groupPorts, depth + 1, `${fullPath}.`);
				result.push({
					type: 'group',
					name: firstSegment,
					fullPath,
					children,
					portCount: countLeafPorts(children),
				});
			}
		}
	}

	return result;
}

function countLeafPorts(items: PortDisplayItem[]): number {
	let count = 0;
	for (const item of items) {
		if (item.type === 'port') {
			count++;
		} else {
			count += item.portCount;
		}
	}
	return count;
}

// --- Leaf port extraction ---

/** Get all leaf ports from a display item (recursively). */
export function getLeafPorts(item: PortDisplayItem): PortConfig[] {
	if (item.type === 'port') return [item.port];
	const result: PortConfig[] = [];
	for (const child of item.children) {
		result.push(...getLeafPorts(child));
	}
	return result;
}

/** Get all leaf ports from a list of display items. */
export function getAllLeafPorts(items: PortDisplayItem[]): PortConfig[] {
	const result: PortConfig[] = [];
	for (const item of items) {
		result.push(...getLeafPorts(item));
	}
	return result;
}

/**
 * Get the leaf port suffixes for a group (the part of the port name after the group path).
 * For group "a.b" with ports ["a.b.x", "a.b.y"], returns ["x", "y"].
 */
export function getGroupLeafSuffixes(groupPath: string, ports: PortConfig[]): string[] {
	const prefix = groupPath + '.';
	return ports
		.filter(p => p.name.startsWith(prefix))
		.map(p => p.name.slice(prefix.length))
		.sort();
}

/**
 * Find a group node in a port tree by its full path.
 */
export function findGroupInTree(items: PortDisplayItem[], groupPath: string): PortGroupTree | null {
	for (const item of items) {
		if (item.type === 'group') {
			if (item.fullPath === groupPath) return item;
			const found = findGroupInTree(item.children, groupPath);
			if (found) return found;
		}
	}
	return null;
}

// --- Group compatibility ---

/**
 * Get the ports belonging to a specific side of a node.
 */
function getNodePortsBySide(node: FlowNode, side: 'in' | 'out'): PortConfig[] {
	return side === 'in' ? node.data.inPorts : node.data.outPorts;
}

/**
 * Check if two port groups on different nodes are compatible for group connection.
 * Compatible means: same set of leaf port suffixes (names after the group prefix).
 */
export function areGroupsCompatible(
	sourceNode: FlowNode,
	sourceGroupPath: string,
	targetNode: FlowNode,
	targetGroupPath: string,
): boolean {
	const sourcePorts = getNodePortsBySide(sourceNode, 'out');
	const targetPorts = getNodePortsBySide(targetNode, 'in');

	const sourceSuffixes = getGroupLeafSuffixes(sourceGroupPath, sourcePorts);
	const targetSuffixes = getGroupLeafSuffixes(targetGroupPath, targetPorts);

	if (sourceSuffixes.length === 0 || targetSuffixes.length === 0) return false;
	if (sourceSuffixes.length !== targetSuffixes.length) return false;

	for (let i = 0; i < sourceSuffixes.length; i++) {
		if (sourceSuffixes[i] !== targetSuffixes[i]) return false;
	}

	return true;
}

/**
 * Get the matched port pairs for a group-to-group connection.
 * Returns pairs of [sourcePortName, targetPortName].
 */
export function getGroupConnectionPairs(
	sourceNode: FlowNode,
	sourceGroupPath: string,
	targetNode: FlowNode,
	targetGroupPath: string,
): [string, string][] {
	const sourcePorts = getNodePortsBySide(sourceNode, 'out');
	const targetPorts = getNodePortsBySide(targetNode, 'in');

	const sourcePrefix = sourceGroupPath + '.';
	const targetPrefix = targetGroupPath + '.';

	// Build a map of suffix → source port name
	const sourceBySuffix = new Map<string, string>();
	for (const port of sourcePorts) {
		if (port.name.startsWith(sourcePrefix)) {
			sourceBySuffix.set(port.name.slice(sourcePrefix.length), port.name);
		}
	}

	// Match target ports by suffix
	const pairs: [string, string][] = [];
	for (const port of targetPorts) {
		if (port.name.startsWith(targetPrefix)) {
			const suffix = port.name.slice(targetPrefix.length);
			const sourcePortName = sourceBySuffix.get(suffix);
			if (sourcePortName) {
				pairs.push([sourcePortName, port.name]);
			}
		}
	}

	return pairs;
}

/**
 * Get the innermost group path that a port name belongs to.
 * For port "a.b.c", returns "a.b". For port "a.x", returns "a".
 * For a port with no dots (standalone), returns null.
 */
export function getPortGroupPath(portName: string): string | null {
	const lastDot = portName.lastIndexOf('.');
	if (lastDot === -1) return null;
	return portName.slice(0, lastDot);
}

/**
 * Get all port names that belong to a group (recursively includes all leaf ports).
 */
export function getGroupPortNames(groupPath: string, ports: PortConfig[]): string[] {
	const prefix = groupPath + '.';
	return ports.filter(p => p.name.startsWith(prefix)).map(p => p.name);
}

// --- Utility for counting visible items (for handle positioning) ---

/**
 * Count the number of visible "rows" in a port display tree,
 * given the current collapse state.
 */
export function countVisibleRows(
	items: PortDisplayItem[],
	isCollapsed: (groupPath: string) => boolean,
): number {
	let count = 0;
	for (const item of items) {
		if (item.type === 'port') {
			count++;
		} else {
			count++; // The group header row itself
			if (!isCollapsed(item.fullPath)) {
				count += countVisibleRows(item.children, isCollapsed);
			}
		}
	}
	return count;
}

/**
 * Flatten the port tree into a list of visible rows for handle positioning.
 * Returns handle IDs in display order.
 */
export function getVisibleHandleIds(
	items: PortDisplayItem[],
	side: 'in' | 'out',
	isCollapsed: (groupPath: string) => boolean,
): string[] {
	const ids: string[] = [];
	for (const item of items) {
		if (item.type === 'port') {
			ids.push(item.port.name);
		} else {
			const collapsed = isCollapsed(item.fullPath);
			if (collapsed) {
				ids.push(makeGroupHandleId(side, item.fullPath));
			} else {
				// Group header row doesn't have a visible handle, but children do
				ids.push(...getVisibleHandleIds(item.children, side, isCollapsed));
			}
		}
	}
	return ids;
}

/**
 * Get ALL handle IDs that should exist in the DOM (visible + hidden).
 * This includes both individual port handles and group handles.
 */
export function getAllHandleIds(
	items: PortDisplayItem[],
	side: 'in' | 'out',
): { portHandles: string[]; groupHandles: string[] } {
	const portHandles: string[] = [];
	const groupHandles: string[] = [];

	function walk(items: PortDisplayItem[]) {
		for (const item of items) {
			if (item.type === 'port') {
				portHandles.push(item.port.name);
			} else {
				groupHandles.push(makeGroupHandleId(side, item.fullPath));
				walk(item.children);
			}
		}
	}

	walk(items);
	return { portHandles, groupHandles };
}
