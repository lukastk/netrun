<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import type { NetrunNodeData } from '$lib/stores/flowStore';

	interface Props {
		id: string;
		data: NetrunNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

	// Calculate handle positions for multiple ports
	function getHandleStyle(index: number, total: number): string {
		if (total === 1) return 'top: 50%';
		const spacing = 100 / (total + 1);
		const top = spacing * (index + 1);
		return `top: ${top}%`;
	}
</script>

<div
	class="netrun-node"
	class:selected
	class:factory={data.nodeType === 'factory'}
	class:invalid={data.isValid === false}
>
	<!-- Header -->
	<div class="node-header">
		{#if data.nodeType === 'factory'}
			<span class="factory-badge">F</span>
		{/if}
		<span class="node-label">{data.label}</span>
	</div>

	<!-- Ports container -->
	<div class="ports-container">
		<!-- Input ports (left) -->
		<div class="ports input-ports">
			{#each data.inPorts as port, i}
				<div class="port-row">
					<Handle
						type="target"
						position={Position.Left}
						id={port.name}
						style={getHandleStyle(i, data.inPorts.length)}
					/>
					<span class="port-label">{port.name}</span>
					{#if port.type && port.type !== 'any'}
						<span class="port-type">{port.type}</span>
					{/if}
				</div>
			{/each}
		</div>

		<!-- Output ports (right) -->
		<div class="ports output-ports">
			{#each data.outPorts as port, i}
				<div class="port-row">
					{#if port.type && port.type !== 'any'}
						<span class="port-type">{port.type}</span>
					{/if}
					<span class="port-label">{port.name}</span>
					<Handle
						type="source"
						position={Position.Right}
						id={port.name}
						style={getHandleStyle(i, data.outPorts.length)}
					/>
				</div>
			{/each}
		</div>
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
	}

	.ports {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.input-ports {
		align-items: flex-start;
		padding-left: 12px;
	}

	.output-ports {
		align-items: flex-end;
		padding-right: 12px;
	}

	.port-row {
		display: flex;
		align-items: center;
		gap: 4px;
		position: relative;
	}

	.port-label {
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
	}

	.port-type {
		color: var(--text-secondary, #666);
		font-size: 10px;
		opacity: 0.7;
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
</style>
