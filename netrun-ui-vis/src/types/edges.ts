/**
 * Edge data types for netrun graph visualization.
 */
import type { Edge } from '@xyflow/svelte';

// Data attached to edges (e.g. dependency flag)
export interface NetrunEdgeData extends Record<string, unknown> {
	dependency?: boolean;
}

export type NetrunEdge = Edge<NetrunEdgeData>;
