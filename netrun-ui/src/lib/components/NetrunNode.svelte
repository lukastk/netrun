<script lang="ts">
	import { NodeResizer } from '@xyflow/svelte';
	import type { NetrunNodeData } from '$lib/stores/flowStore';
	import { updateNodeDimensions, pushHistory, toggleNodeDescExpanded } from '$lib/stores/flowStore';
	import PortList from './PortList.svelte';

	interface Props {
		id: string;
		data: NetrunNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

	let descExpanded = $derived((() => {
		const extra = (data._config?.extra ?? undefined) as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		return (ui?.descriptionExpanded as boolean) ?? false;
	})());

	let portGroupStates = $derived((() => {
		const extra = (data._config?.extra ?? undefined) as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		return ui?.portGroups as Record<string, boolean> | undefined;
	})());

	let shape = $derived((() => {
		const extra = (data._config?.extra ?? undefined) as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		return (ui?.shape as string) ?? 'rectangle';
	})());

	let hideLabel = $derived((() => {
		const extra = (data._config?.extra ?? undefined) as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		return (ui?.hideLabel as boolean) ?? false;
	})());

	let hideDescription = $derived((() => {
		const extra = (data._config?.extra ?? undefined) as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		return (ui?.hideDescription as boolean) ?? false;
	})());

	let hidePortNames = $derived((() => {
		const extra = (data._config?.extra ?? undefined) as Record<string, unknown> | undefined;
		const ui = (extra?.ui ?? undefined) as Record<string, unknown> | undefined;
		return (ui?.hidePortNames as boolean) ?? false;
	})());

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
	class="netrun-node shape-{shape}"
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
	{#if !hideLabel}
		<div class="node-header">
			{#if data.nodeType === 'factory'}
				<span class="factory-badge">F</span>
			{/if}
			<span class="node-label">{data.label}</span>
		</div>
	{/if}

	<!-- Description -->
	{#if data.description && !hideDescription}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="node-description" onclick={() => toggleNodeDescExpanded(id)}>
			<span class="desc-chevron" class:expanded={descExpanded}>&#9656;</span>
			{#if descExpanded}
				<span class="desc-content">{data.description}</span>
			{:else}
				<span class="desc-preview">{data.description.split('\n')[0]}</span>
			{/if}
		</div>
	{/if}

	<!-- Ports container -->
	<div class="ports-container">
		<PortList nodeId={id} ports={data.inPorts} side="in" {portGroupStates} {hidePortNames} />
		<PortList nodeId={id} ports={data.outPorts} side="out" {portGroupStates} {hidePortNames} />
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

	/* ── Shape: rounded ─────────────────────────────── */
	.netrun-node.shape-rounded {
		border-radius: 16px;
	}
	.netrun-node.shape-rounded .node-header {
		border-radius: 14px 14px 0 0;
	}

	/* ── Shape: pill / stadium ──────────────────────── */
	.netrun-node.shape-pill {
		border-radius: 999px;
	}
	.netrun-node.shape-pill .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
		justify-content: center;
	}
	.netrun-node.shape-pill .ports-container {
		padding-left: 16px;
		padding-right: 16px;
	}

	/* ── Shape: diamond ─────────────────────────────── */
	.netrun-node.shape-diamond {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.netrun-node.shape-diamond::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
		background: var(--node-border, #404040);
		z-index: -2;
	}
	.netrun-node.shape-diamond::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.netrun-node.shape-diamond.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.netrun-node.shape-diamond.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.netrun-node.shape-diamond .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
		justify-content: center;
	}
	.netrun-node.shape-diamond .ports-container {
		padding: 0 25%;
	}

	/* ── Shape: hexagon ─────────────────────────────── */
	.netrun-node.shape-hexagon {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.netrun-node.shape-hexagon::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%);
		background: var(--node-border, #404040);
		z-index: -2;
	}
	.netrun-node.shape-hexagon::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.netrun-node.shape-hexagon.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.netrun-node.shape-hexagon.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.netrun-node.shape-hexagon .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
		justify-content: center;
	}
	.netrun-node.shape-hexagon .ports-container {
		padding: 0 15%;
	}

	/* ── Shape: cylinder ────────────────────────────── */
	.netrun-node.shape-cylinder {
		border-radius: 50% / 12px;
		position: relative;
		padding-bottom: 10px;
	}
	.netrun-node.shape-cylinder .node-header {
		border-radius: 50% 50% 0 0 / 12px 12px 0 0;
	}
	.netrun-node.shape-cylinder::after {
		content: '';
		position: absolute;
		bottom: -2px;
		left: -2px;
		right: -2px;
		height: 16px;
		border-radius: 0 0 50% 50% / 0 0 100% 100%;
		border: 2px solid var(--node-border, #404040);
		border-top: none;
		background: var(--node-bg, #2d2d2d);
	}
	.netrun-node.shape-cylinder.selected::after {
		border-color: var(--node-selected, #3b82f6);
	}
	.netrun-node.shape-cylinder.invalid::after {
		border-color: var(--error-color, #ef4444);
	}

	/* ── Shape: triangle-right ──────────────────────── */
	.netrun-node.shape-triangle-right {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.netrun-node.shape-triangle-right::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(0% 0%, 100% 50%, 0% 100%);
		background: var(--node-border, #404040);
		z-index: -2;
	}
	.netrun-node.shape-triangle-right::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(0% 0%, 100% 50%, 0% 100%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.netrun-node.shape-triangle-right.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.netrun-node.shape-triangle-right.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.netrun-node.shape-triangle-right .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
	}
	.netrun-node.shape-triangle-right .ports-container {
		padding-right: 30%;
	}
	/* Move output handles to the triangle tip */
	:global(.netrun-node.shape-triangle-right .svelte-flow__handle-right) {
		right: -1px;
	}

	/* ── Shape: triangle-left ───────────────────────── */
	.netrun-node.shape-triangle-left {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.netrun-node.shape-triangle-left::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(100% 0%, 0% 50%, 100% 100%);
		background: var(--node-border, #404040);
		z-index: -2;
	}
	.netrun-node.shape-triangle-left::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(100% 0%, 0% 50%, 100% 100%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.netrun-node.shape-triangle-left.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.netrun-node.shape-triangle-left.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.netrun-node.shape-triangle-left .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
	}
	.netrun-node.shape-triangle-left .ports-container {
		padding-left: 30%;
	}
	/* Move input handles to the triangle tip */
	:global(.netrun-node.shape-triangle-left .svelte-flow__handle-left) {
		left: -1px;
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

	.node-description {
		padding: 4px 10px;
		border-bottom: 1px solid var(--border-color, #404040);
		cursor: pointer;
		display: flex;
		align-items: flex-start;
		gap: 4px;
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		overflow: hidden;
		width: 0;
		min-width: 100%;
		box-sizing: border-box;
	}

	.desc-chevron {
		display: inline-block;
		font-size: 9px;
		transition: transform 0.15s ease;
		flex-shrink: 0;
		line-height: 14px;
	}

	.desc-chevron.expanded {
		transform: rotate(90deg);
	}

	.desc-preview {
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
		min-width: 0;
	}

	.desc-content {
		white-space: pre-wrap;
		word-wrap: break-word;
		overflow-wrap: break-word;
		min-width: 0;
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

	/* Root group handle styling — larger than sub-group handles */
	:global(.netrun-node .svelte-flow__handle.root-group-handle) {
		width: 14px;
		height: 14px;
		border-radius: 3px;
	}
</style>
