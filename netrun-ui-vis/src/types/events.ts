/**
 * Event callback types for the flow viewer.
 */
import type { FlowNode } from './nodes.js';
import type { NetrunEdge } from './edges.js';

/** Result of a dependency cascade BFS analysis. */
export interface CascadeHighlightState {
	/** Nodes with no incoming edges (sources of the cascade) */
	sourceNodes: Set<string>;
	/** All nodes visited during BFS */
	visitedNodes: Set<string>;
	/** All edges traversed during BFS */
	visitedEdges: Set<string>;
}

/** Signal/control port type configuration (prefix, suffix, known types). */
export interface PortTypeConfig {
	prefix: string;
	suffix: string;
	types: string[];
}

/** Context menu item descriptor. */
export interface ContextMenuItem {
	label: string;
	action: string;
	danger?: boolean;
	/** Show a separator line before this item. */
	separator?: boolean;
}

/** Event callbacks for the flow viewer component. */
export interface FlowViewerCallbacks {
	/** Node was clicked */
	onNodeClick?: (event: { node: FlowNode; nativeEvent: MouseEvent }) => void;
	/** Node was double-clicked */
	onNodeDoubleClick?: (event: { node: FlowNode; nativeEvent: MouseEvent }) => void;
	/** Edge was clicked */
	onEdgeClick?: (event: { edge: NetrunEdge; nativeEvent: MouseEvent }) => void;
	/** Selection changed */
	onSelectionChange?: (event: { nodes: FlowNode[]; edges: NetrunEdge[] }) => void;
	/** Node was resized (drag ended) */
	onNodeResize?: (event: { nodeId: string; width: number; height: number; position: { x: number; y: number } }) => void;
	/** Nodes were dragged to new positions */
	onNodeDragStop?: (event: { nodes: Array<{ id: string; position: { x: number; y: number } }> }) => void;
	/** Connection was created between ports */
	onConnect?: (event: { source: string; target: string; sourceHandle: string | null; targetHandle: string | null }) => void;
	/** Nodes/edges were deleted */
	onDelete?: (event: { nodeIds: string[]; edgeIds: string[] }) => void;
	/** Context menu action triggered on node */
	onNodeContextAction?: (event: { node: FlowNode; action: string }) => void;
	/** Context menu action triggered on edge */
	onEdgeContextAction?: (event: { edge: NetrunEdge; action: string }) => void;
	/** Port group was toggled */
	onPortGroupToggle?: (event: { nodeId: string; side: 'in' | 'out'; groupPath: string; portCount: number }) => void;
	/** Description expand/collapse was toggled */
	onDescriptionToggle?: (event: { nodeId: string }) => void;
}
