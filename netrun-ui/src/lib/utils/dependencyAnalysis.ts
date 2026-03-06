/**
 * Client-side BFS for dependency cascade analysis.
 * Mirrors netrun-sim's cascade_backward logic.
 */
import type { Edge } from '@xyflow/svelte';

export interface CascadeResult {
	/** Nodes with no incoming edges (sources of the cascade) */
	sourceNodes: Set<string>;
	/** All nodes visited during BFS */
	visitedNodes: Set<string>;
	/** All edges traversed during BFS */
	visitedEdges: Set<string>;
}

/**
 * Perform backward BFS from a dependency edge to find source nodes.
 * Walks backward through ALL edges (not just dependency edges) to find
 * nodes with no incoming edges (source nodes).
 */
export function analyzeDependencyCascade(
	startEdge: Edge,
	allEdges: Edge[],
): CascadeResult {
	const visitedNodes = new Set<string>();
	const visitedEdges = new Set<string>();
	const sourceNodes = new Set<string>();

	// Build adjacency: for each node, which edges lead INTO it
	const incomingEdges = new Map<string, Edge[]>();
	for (const edge of allEdges) {
		if (!incomingEdges.has(edge.target)) {
			incomingEdges.set(edge.target, []);
		}
		incomingEdges.get(edge.target)!.push(edge);
	}

	// BFS queue: start from the source node of the clicked dependency edge
	const queue: string[] = [startEdge.source];
	visitedNodes.add(startEdge.source);
	visitedEdges.add(startEdge.id);

	while (queue.length > 0) {
		const nodeId = queue.shift()!;
		const incoming = incomingEdges.get(nodeId) || [];

		if (incoming.length === 0) {
			sourceNodes.add(nodeId);
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

	return { sourceNodes, visitedNodes, visitedEdges };
}

/**
 * Analyze cascade starting from a node: find all incoming dependency edges
 * on that node and walk backward from each. Returns null if the node has
 * no incoming dependency edges.
 */
export function analyzeDependencyCascadeFromNode(
	nodeId: string,
	allEdges: Edge[],
): CascadeResult | null {
	const depEdges = allEdges.filter(
		e => e.target === nodeId && (e.data as Record<string, unknown> | undefined)?.dependency
	);
	if (depEdges.length === 0) return null;

	const visitedNodes = new Set<string>();
	const visitedEdges = new Set<string>();
	const sourceNodes = new Set<string>();

	const incomingEdges = new Map<string, Edge[]>();
	for (const edge of allEdges) {
		if (!incomingEdges.has(edge.target)) {
			incomingEdges.set(edge.target, []);
		}
		incomingEdges.get(edge.target)!.push(edge);
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

	return { sourceNodes, visitedNodes, visitedEdges };
}
