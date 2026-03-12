/**
 * Node and port data types for netrun graph visualization.
 */
import type { Node } from '@xyflow/svelte';

// Node shape types
export type NodeShape =
	| 'rectangle'
	| 'rounded'
	| 'pill'
	| 'diamond'
	| 'hexagon'
	| 'cylinder'
	| 'triangle-right'
	| 'triangle-left';

export const NODE_SHAPES: { value: NodeShape; label: string }[] = [
	{ value: 'rectangle', label: 'Rectangle' },
	{ value: 'rounded', label: 'Rounded' },
	{ value: 'pill', label: 'Pill' },
	{ value: 'diamond', label: 'Diamond' },
	{ value: 'hexagon', label: 'Hexagon' },
	{ value: 'cylinder', label: 'Cylinder' },
	{ value: 'triangle-right', label: 'Triangle \u25B6' },
	{ value: 'triangle-left', label: 'Triangle \u25C0' },
];

// Types for netrun node data
export interface PortConfig {
	name: string;
	type?: string;
	isSignal?: boolean;
	isControl?: boolean;
	[key: string]: unknown; // Allow additional properties
}

// Base data interface shared by all node types
export interface BaseNodeData extends Record<string, unknown> {
	label: string;
	nodeType: 'regular' | 'factory' | 'subgraph';
	inPorts: PortConfig[];
	outPorts: PortConfig[];
	description?: string;
	// Validation state
	isValid?: boolean;
	validationErrors?: string[];
}

// Factory-provided default values (stored separately from user overrides)
export interface FactoryDefaults {
	description?: string;
}

// Extended data for regular/factory nodes
export interface NetrunNodeData extends BaseNodeData {
	nodeType: 'regular' | 'factory';
	// For factory nodes
	factory?: string;
	factoryArgs?: Record<string, unknown>;
	// Factory-provided defaults (transient, not saved to file)
	_factoryDefaults?: FactoryDefaults;
	// Extra config data
	_config?: Record<string, unknown>;
}

// Extended data for subgraph nodes
export interface SubgraphNodeData extends BaseNodeData {
	nodeType: 'subgraph';
	// Subgraph-specific
	source?: string; // "Inline" or file path
	nodeCount?: number; // Number of nodes inside (for inline)
	// Store full subgraph config for round-trip serialization
	_subgraphConfig?: Record<string, unknown>;
}

// Decoration types
export type DecorationType =
	| 'rectangle'
	| 'rounded-rectangle'
	| 'circle'
	| 'triangle'
	| 'divider'
	| 'label'
	| 'textbox'
	| 'image';

export const DECORATION_TYPES: { value: DecorationType; label: string }[] = [
	{ value: 'rectangle', label: 'Rectangle' },
	{ value: 'rounded-rectangle', label: 'Rounded Rectangle' },
	{ value: 'circle', label: 'Circle' },
	{ value: 'triangle', label: 'Triangle' },
	{ value: 'divider', label: 'Divider' },
	{ value: 'label', label: 'Label' },
	{ value: 'textbox', label: 'Textbox' },
	{ value: 'image', label: 'Image' },
];

// Extended data for decoration nodes (non-functional visual annotations)
export interface DecorationNodeData extends Record<string, unknown> {
	label: string;
	nodeType: 'decoration';
	decorationType: DecorationType;
	inPorts: PortConfig[];
	outPorts: PortConfig[];
	description?: string;
	isValid?: boolean;
	validationErrors?: string[];
	text?: string;
	imagePath?: string;
	orientation?: 'horizontal' | 'vertical';
	fillColor?: string;
	strokeColor?: string;
	strokeWidth?: number;
	fontSize?: number;
	fontColor?: string;
	opacity?: number;
	locked?: boolean;
}

// Combined type for any flow node data
export type AnyNodeData = NetrunNodeData | SubgraphNodeData | DecorationNodeData;

// Typed node aliases
export type FlowNode = Node<AnyNodeData>;
export type NetrunNode = Node<NetrunNodeData, 'netrunNode'>;
export type SubgraphNodeType = Node<SubgraphNodeData, 'subgraphNode'>;
export type DecorationNodeType = Node<DecorationNodeData, 'decorationNode'>;

// Expanded subgraph child node ID helpers
const CHILD_SEPARATOR = '::';

export function isExpandedChildNode(nodeId: string): boolean {
	return nodeId.includes(CHILD_SEPARATOR);
}

export function getParentSubgraphId(childNodeId: string): string {
	const idx = childNodeId.indexOf(CHILD_SEPARATOR);
	return idx >= 0 ? childNodeId.substring(0, idx) : childNodeId;
}

export function getOriginalChildId(childNodeId: string): string {
	const idx = childNodeId.indexOf(CHILD_SEPARATOR);
	return idx >= 0 ? childNodeId.substring(idx + CHILD_SEPARATOR.length) : childNodeId;
}

export function makeChildNodeId(parentId: string, childId: string): string {
	return `${parentId}${CHILD_SEPARATOR}${childId}`;
}

/** Check if an edge ID is a dynamically-generated exposed port edge (not deletable). */
export function isExposedPortEdge(edgeId: string): boolean {
	return edgeId.includes('::exposed-in::') || edgeId.includes('::exposed-out::');
}
