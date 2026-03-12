/**
 * Top-level data structure for rendering a netrun graph.
 */
import type { FlowNode } from './nodes.js';
import type { NetrunEdge } from './edges.js';

/** Visual settings for the graph viewer. */
export interface GraphSettings {
	edgeStyle?: 'smoothstep' | 'straight' | 'simplebezier' | 'bezier';
	edgeMarkers?: 'arrow-end' | 'arrow-start' | 'arrow-both' | 'none';
	minZoom?: number;
	maxZoom?: number;
	nodeTitleFontSize?: number;
	nodeDescFontSize?: number;
	nodePortFontSize?: number;
}

/** Everything needed to render a netrun graph. No backend, no stores. */
export interface NetrunGraph {
	nodes: FlowNode[];
	edges: NetrunEdge[];
	settings?: GraphSettings;
}
