/**
 * Client-side BFS for dependency cascade analysis.
 * Mirrors netrun-sim's cascade_backward logic.
 */
import type { Node, Edge } from '@xyflow/svelte';

export interface CascadeResult {
	/** Nodes with no incoming edges (sources of the cascade) */
	sourceNodes: Set<string>;
	/** All nodes visited during BFS */
	visitedNodes: Set<string>;
	/** All edges traversed during BFS */
	visitedEdges: Set<string>;
	/** Source nodes whose salvo conditions can't be satisfied with zero input packets */
	unstartableNodes: Set<string>;
}

/**
 * Check if a salvo condition term can be satisfied when all ports have zero packets.
 * Returns true if the term is satisfiable with empty ports.
 */
function isTermSatisfiableEmpty(term: unknown): boolean {
	if (term === true || term === 'true') return true;
	if (term === false || term === 'false') return false;
	if (typeof term !== 'object' || term === null) return false;

	const obj = term as Record<string, unknown>;

	// { "port": "name", "condition": "empty" } — empty ports satisfy "empty"
	if (obj.port !== undefined) {
		const cond = obj.condition as string | undefined;
		if (cond === 'empty') return true;
		if (cond === 'full' || cond === 'not_empty' || cond === 'eq' || cond === 'gte') return false;
		// Default: a port condition requires packets
		return false;
	}

	// { "and": [...] }
	if (Array.isArray(obj.and)) {
		return (obj.and as unknown[]).every(isTermSatisfiableEmpty);
	}

	// { "or": [...] }
	if (Array.isArray(obj.or)) {
		return (obj.or as unknown[]).some(isTermSatisfiableEmpty);
	}

	// { "not": term }
	if (obj.not !== undefined) {
		return !isTermSatisfiableEmpty(obj.not);
	}

	return false;
}

/**
 * Check if a node can be started with zero input packets.
 * Looks at in_salvo_conditions — if any condition's term is satisfiable
 * with all ports empty, the node is startable.
 */
function isStartableWithZeroPackets(nodeData: Record<string, unknown>): boolean {
	const config = (nodeData._config || {}) as Record<string, unknown>;
	const salvoConditions = config.in_salvo_conditions as Record<string, unknown> | undefined;

	if (!salvoConditions) {
		// Default salvo condition: all ports must have packets → NOT satisfiable empty
		return false;
	}

	for (const condName of Object.keys(salvoConditions)) {
		const cond = salvoConditions[condName] as Record<string, unknown> | undefined;
		if (!cond) continue;
		const term = cond.term;
		if (term !== undefined && isTermSatisfiableEmpty(term)) {
			return true;
		}
	}

	return false;
}

/**
 * Perform backward BFS from a dependency edge to find source nodes.
 * Walks backward through ALL edges (not just dependency edges) to find
 * nodes with no incoming edges (source nodes).
 */
export function analyzeDependencyCascade(
	startEdge: Edge,
	allNodes: Node[],
	allEdges: Edge[],
): CascadeResult {
	const visitedNodes = new Set<string>();
	const visitedEdges = new Set<string>();
	const sourceNodes = new Set<string>();
	const unstartableNodes = new Set<string>();

	// Build adjacency: for each node, which edges lead INTO it
	const incomingEdges = new Map<string, Edge[]>();
	for (const edge of allEdges) {
		if (!incomingEdges.has(edge.target)) {
			incomingEdges.set(edge.target, []);
		}
		incomingEdges.get(edge.target)!.push(edge);
	}

	const nodeMap = new Map<string, Node>();
	for (const node of allNodes) {
		nodeMap.set(node.id, node);
	}

	// BFS queue: start from the source node of the clicked dependency edge
	const queue: string[] = [startEdge.source];
	visitedNodes.add(startEdge.source);
	visitedEdges.add(startEdge.id);

	while (queue.length > 0) {
		const nodeId = queue.shift()!;
		const incoming = incomingEdges.get(nodeId) || [];

		if (incoming.length === 0) {
			// This is a source node (no incoming edges)
			sourceNodes.add(nodeId);
			const node = nodeMap.get(nodeId);
			if (node && !isStartableWithZeroPackets(node.data as Record<string, unknown>)) {
				unstartableNodes.add(nodeId);
			}
		} else {
			// Continue walking backward
			for (const edge of incoming) {
				visitedEdges.add(edge.id);
				if (!visitedNodes.has(edge.source)) {
					visitedNodes.add(edge.source);
					queue.push(edge.source);
				}
			}
		}
	}

	return { sourceNodes, visitedNodes, visitedEdges, unstartableNodes };
}

/**
 * Analyze cascade starting from a node: find all incoming dependency edges
 * on that node and walk backward from each. Returns null if the node has
 * no incoming dependency edges.
 */
export function analyzeDependencyCascadeFromNode(
	nodeId: string,
	allNodes: Node[],
	allEdges: Edge[],
): CascadeResult | null {
	const depEdges = allEdges.filter(
		e => e.target === nodeId && (e.data as Record<string, unknown> | undefined)?.dependency
	);
	if (depEdges.length === 0) return null;

	const visitedNodes = new Set<string>();
	const visitedEdges = new Set<string>();
	const sourceNodes = new Set<string>();
	const unstartableNodes = new Set<string>();

	const incomingEdges = new Map<string, Edge[]>();
	for (const edge of allEdges) {
		if (!incomingEdges.has(edge.target)) {
			incomingEdges.set(edge.target, []);
		}
		incomingEdges.get(edge.target)!.push(edge);
	}

	const nodeMap = new Map<string, Node>();
	for (const node of allNodes) {
		nodeMap.set(node.id, node);
	}

	// Seed BFS from all incoming dependency edges
	const queue: string[] = [];
	for (const edge of depEdges) {
		visitedEdges.add(edge.id);
		if (!visitedNodes.has(edge.source)) {
			visitedNodes.add(edge.source);
			queue.push(edge.source);
		}
	}

	while (queue.length > 0) {
		const nid = queue.shift()!;
		const incoming = incomingEdges.get(nid) || [];

		if (incoming.length === 0) {
			sourceNodes.add(nid);
			const node = nodeMap.get(nid);
			if (node && !isStartableWithZeroPackets(node.data as Record<string, unknown>)) {
				unstartableNodes.add(nid);
			}
		} else {
			for (const edge of incoming) {
				visitedEdges.add(edge.id);
				if (!visitedNodes.has(edge.source)) {
					visitedNodes.add(edge.source);
					queue.push(edge.source);
				}
			}
		}
	}

	return { sourceNodes, visitedNodes, visitedEdges, unstartableNodes };
}
