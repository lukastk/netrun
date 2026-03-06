/**
 * Auto-layout utilities using ELK.js (Eclipse Layout Kernel).
 *
 * Converts SvelteFlow nodes/edges into an ELK graph, runs layout,
 * and returns the computed positions.
 */
import ELK, { type ElkNode, type ElkPort, type ElkExtendedEdge } from 'elkjs/lib/elk.bundled.js';
import type { Node, Edge } from '@xyflow/svelte';

// Module-level singleton
const elk = new ELK();

export interface LayoutAlgorithm {
	id: string;
	label: string;
	description: string;
	elkAlgorithm: string;
	elkDirection?: string; // RIGHT, DOWN, etc.
	extraOptions?: Record<string, string>;
}

export const LAYOUT_ALGORITHMS: LayoutAlgorithm[] = [
	{
		id: 'layered-lr',
		label: 'Layered L\u2192R',
		description: 'Sugiyama layered layout, left to right',
		elkAlgorithm: 'layered',
		elkDirection: 'RIGHT',
	},
	{
		id: 'layered-tb',
		label: 'Layered T\u2193B',
		description: 'Sugiyama layered layout, top to bottom',
		elkAlgorithm: 'layered',
		elkDirection: 'DOWN',
	},
	{
		id: 'tree-lr',
		label: 'Tree L\u2192R',
		description: 'Tree layout, left to right',
		elkAlgorithm: 'mrtree',
		elkDirection: 'RIGHT',
	},
	{
		id: 'tree-tb',
		label: 'Tree T\u2193B',
		description: 'Tree layout, top to bottom',
		elkAlgorithm: 'mrtree',
		elkDirection: 'DOWN',
	},
	{
		id: 'force',
		label: 'Force',
		description: 'Force-directed layout (works with cycles)',
		elkAlgorithm: 'force',
	},
	{
		id: 'stress',
		label: 'Stress',
		description: 'Stress-minimization layout (works with cycles)',
		elkAlgorithm: 'stress',
	},
];

const DEFAULT_WIDTH = 200;
const DEFAULT_HEIGHT = 100;

export interface NodeDimensions {
	width: number;
	height: number;
}

export interface LayoutResult {
	positions: Array<{ id: string; position: { x: number; y: number } }>;
}

/**
 * Compute layout positions for the given nodes and edges.
 */
export async function computeLayout(
	nodes: Node[],
	edges: Edge[],
	algorithmId: string,
	nodeDimensions?: Map<string, NodeDimensions>,
): Promise<LayoutResult> {
	const algorithm = LAYOUT_ALGORITHMS.find((a) => a.id === algorithmId);
	if (!algorithm) {
		throw new Error(`Unknown layout algorithm: ${algorithmId}`);
	}

	const isHorizontal = algorithm.elkDirection === 'RIGHT' || algorithm.elkDirection === 'LEFT';

	// Filter out decoration nodes — they should not participate in layout
	const layoutNodes = nodes.filter(
		(n) => (n.data as Record<string, unknown>)?.nodeType !== 'decoration'
	);

	// Build ELK graph
	const elkNodes: ElkNode[] = layoutNodes.map((node) => {
		const dims = nodeDimensions?.get(node.id);
		const width = dims?.width ?? DEFAULT_WIDTH;
		const height = dims?.height ?? DEFAULT_HEIGHT;

		const ports: ElkPort[] = [];
		const data = node.data as Record<string, unknown> | undefined;
		const inPorts = (data?.inPorts as Array<{ name: string }>) ?? [];
		const outPorts = (data?.outPorts as Array<{ name: string }>) ?? [];

		// Input ports
		inPorts.forEach((port, i) => {
			ports.push({
				id: `${node.id}__in__${port.name}`,
				layoutOptions: {
					'port.side': isHorizontal ? 'WEST' : 'NORTH',
					'port.index': String(i),
				},
			});
		});

		// Output ports
		outPorts.forEach((port, i) => {
			ports.push({
				id: `${node.id}__out__${port.name}`,
				layoutOptions: {
					'port.side': isHorizontal ? 'EAST' : 'SOUTH',
					'port.index': String(i),
				},
			});
		});

		return {
			id: node.id,
			width,
			height,
			ports,
		};
	});

	// Collect all port IDs that were actually created so edges only reference existing ports
	const portIds = new Set<string>();
	for (const elkNode of elkNodes) {
		for (const port of elkNode.ports ?? []) {
			portIds.add(port.id);
		}
	}

	// Build a set of valid node IDs for edge filtering (use layoutNodes, not unfiltered nodes)
	const nodeIds = new Set(layoutNodes.map((n) => n.id));

	const elkEdges: ElkExtendedEdge[] = edges
		.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target))
		.map((edge) => {
			const sourcePortId = edge.sourceHandle
				? `${edge.source}__out__${edge.sourceHandle}`
				: undefined;
			const targetPortId = edge.targetHandle
				? `${edge.target}__in__${edge.targetHandle}`
				: undefined;

			return {
				id: edge.id,
				sources: [sourcePortId && portIds.has(sourcePortId) ? sourcePortId : edge.source],
				targets: [targetPortId && portIds.has(targetPortId) ? targetPortId : edge.target],
			};
		});

	const layoutOptions: Record<string, string> = {
		'elk.algorithm': algorithm.elkAlgorithm,
		'elk.spacing.nodeNode': '80',
		'elk.layered.spacing.nodeNodeBetweenLayers': '100',
		'elk.spacing.portPort': '8',
		'elk.portConstraints': 'FIXED_SIDE',
		...algorithm.extraOptions,
	};

	if (algorithm.elkDirection) {
		layoutOptions['elk.direction'] = algorithm.elkDirection;
	}

	const elkGraph: ElkNode = {
		id: 'root',
		children: elkNodes,
		edges: elkEdges,
		layoutOptions,
	};

	const result = await elk.layout(elkGraph);

	const positions: LayoutResult['positions'] = (result.children ?? []).map((child) => ({
		id: child.id,
		position: { x: child.x ?? 0, y: child.y ?? 0 },
	}));

	return { positions };
}
