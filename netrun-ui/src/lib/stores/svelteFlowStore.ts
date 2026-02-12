/**
 * Store to share SvelteFlow instance methods outside the SvelteFlowProvider boundary.
 *
 * FlowEditor.svelte populates this on mount; Toolbar and commands consume it.
 */
import { writable } from 'svelte/store';
import type { FitViewOptions } from '@xyflow/svelte';

export interface SvelteFlowRef {
	fitView: (options?: FitViewOptions) => void;
	getNodes: () => Array<{ id: string; position: { x: number; y: number }; measured?: { width?: number; height?: number } }>;
}

export const svelteFlowRef = writable<SvelteFlowRef | null>(null);
