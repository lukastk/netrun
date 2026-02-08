<script lang="ts">
	import { NodeResizer } from '@xyflow/svelte';
	import type { NetrunNodeData } from '$lib/stores/flowStore';
	import { updateNodeDimensions, pushHistory } from '$lib/stores/flowStore';
	import PortList from './PortList.svelte';

	interface Props {
		id: string;
		data: NetrunNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

	function handleResizeEnd(_event: unknown, params: { x: number; y: number; width: number; height: number }) {
		updateNodeDimensions([{
			id,
			width: params.width,
			height: params.height,
			position: { x: params.x, y: params.y },
		}]);
		pushHistory();
	}
</script>

<div
	class="netrun-node"
	class:selected
	class:factory={data.nodeType === 'factory'}
	class:invalid={data.isValid === false}
>
	<NodeResizer
		minWidth={150}
		minHeight={60}
		isVisible={selected}
		color="var(--node-selected, #3b82f6)"
		onResizeEnd={handleResizeEnd}
	/>
	<!-- Header -->
	<div class="node-header">
		{#if data.nodeType === 'factory'}
			<span class="factory-badge">F</span>
		{/if}
		<span class="node-label">{data.label}</span>
	</div>

	<!-- Ports container -->
	<div class="ports-container">
		<PortList nodeId={id} ports={data.inPorts} side="in" />
		<PortList nodeId={id} ports={data.outPorts} side="out" />
	</div>

	<!-- Validation errors -->
	{#if data.isValid === false && data.validationErrors}
		<div class="validation-errors">
			{#each data.validationErrors as error}
				<div class="error">{error}</div>
			{/each}
		</div>
	{/if}
</div>

<style>
	.netrun-node {
		background: var(--node-bg, #2d2d2d);
		border: 2px solid var(--node-border, #404040);
		border-radius: 8px;
		min-width: 150px;
		font-size: 12px;
		width: 100%;
		height: 100%;
		box-sizing: border-box;
		display: flex;
		flex-direction: column;
	}

	.netrun-node.selected {
		border-color: var(--node-selected, #3b82f6);
		box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3);
	}

	.netrun-node.invalid {
		border-color: var(--error-color, #ef4444);
	}

	.netrun-node.factory .node-header {
		background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
	}

	.node-header {
		background: var(--bg-tertiary, #3d3d3d);
		padding: 8px 12px;
		border-radius: 6px 6px 0 0;
		display: flex;
		align-items: center;
		gap: 6px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.factory-badge {
		background: rgba(255, 255, 255, 0.2);
		padding: 2px 6px;
		border-radius: 3px;
		font-size: 10px;
		font-weight: 600;
	}

	.node-label {
		font-weight: 500;
		color: var(--text-primary, #fff);
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.ports-container {
		display: flex;
		justify-content: space-between;
		padding: 8px 0;
		min-height: 40px;
		flex: 1;
	}

	.validation-errors {
		padding: 6px 12px;
		border-top: 1px solid var(--border-color, #404040);
		background: rgba(239, 68, 68, 0.1);
	}

	.error {
		color: var(--error-color, #ef4444);
		font-size: 10px;
	}

	/* Handle styling */
	:global(.netrun-node .svelte-flow__handle) {
		width: 10px;
		height: 10px;
		border: 2px solid var(--bg-secondary, #242424);
	}

	:global(.netrun-node .svelte-flow__handle-left) {
		left: -12px;
		background: var(--port-input, #22c55e);
	}

	:global(.netrun-node .svelte-flow__handle-right) {
		right: -12px;
		background: var(--port-output, #f59e0b);
	}

	:global(.netrun-node .svelte-flow__handle.connecting) {
		background: var(--accent-color, #3b82f6);
	}

	/* Group handle styling — slightly larger, rounded rectangle */
	:global(.netrun-node .svelte-flow__handle.group-handle) {
		width: 12px;
		height: 12px;
		border-radius: 3px;
	}

	/* Resize control styling — z-index ensures controls sit above node content */
	:global(.netrun-node .svelte-flow__resize-control) {
		z-index: 10;
	}
	:global(.netrun-node .svelte-flow__resize-control.handle) {
		width: 8px;
		height: 8px;
		border-radius: 2px;
		border: 1px solid rgba(255, 255, 255, 0.5);
	}
</style>
