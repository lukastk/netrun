<script lang="ts">
	import { NodeResizer } from '@xyflow/svelte';
	import type { SubgraphNodeData } from '$lib/stores/flowStore';
	import { updateNodeDimensions, pushHistory, toggleNodeDescExpanded } from '$lib/stores/flowStore';
	import { PORT_ROW_HEIGHT, NODE_BORDER_WIDTH } from '$lib/constants';
	import { openSubgraphTab } from '$lib/stores/tabsStore';
	import { toggleSubgraphExpansion, isSubgraphExpanded } from '$lib/stores/subgraphExpandStore';
	import PortList from './PortList.svelte';

	interface Props {
		id: string;
		data: SubgraphNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

	let descExpanded = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.descriptionExpanded as boolean) ?? false;
	})());

	let portGroupStates = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return ui?.portGroups as Record<string, boolean> | undefined;
	})());

	let shape = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.shape as string) ?? 'rectangle';
	})());

	let hideLabel = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.hideLabel as boolean) ?? false;
	})());

	let hideDescription = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.hideDescription as boolean) ?? false;
	})());

	let hidePortNames = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.hidePortNames as boolean) ?? false;
	})());

	let headerColor = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.headerColor as string) ?? null;
	})());

	let fontColor = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.fontColor as string) ?? null;
	})());

	let locked = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.locked as boolean) ?? false;
	})());

	let isExpanded = $derived((() => {
		const config = (data as Record<string, unknown>)._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.expanded as boolean) ?? false;
	})());

	// Exposed port names for inner handles (only needed when expanded)
	let exposedInPortNames = $derived((() => {
		if (!isExpanded) return [] as string[];
		const sgConfig = data._subgraphConfig;
		if (!sgConfig) return [] as string[];
		const exposed = sgConfig.exposed_in_ports as Record<string, unknown> | undefined;
		return exposed ? Object.keys(exposed) : [] as string[];
	})());

	let exposedOutPortNames = $derived((() => {
		if (!isExpanded) return [] as string[];
		const sgConfig = data._subgraphConfig;
		if (!sgConfig) return [] as string[];
		const exposed = sgConfig.exposed_out_ports as Record<string, unknown> | undefined;
		return exposed ? Object.keys(exposed) : [] as string[];
	})());

	// True when the pointy side of a triangle has exactly 1 port
	let singleTipPort = $derived(
		(shape === 'triangle-right' && data.outPorts.length === 1) ||
		(shape === 'triangle-left' && data.inPorts.length === 1)
	);

	// Port alignment: quantize pre-ports area to PORT_ROW_HEIGHT multiples
	let prePortsEl: HTMLDivElement | undefined = $state();
	let portsPaddingTop = $state(0);

	$effect(() => {
		if (!prePortsEl) return;
		const observer = new ResizeObserver(() => {
			const h = prePortsEl!.offsetHeight;
			const raw = NODE_BORDER_WIDTH + h + PORT_ROW_HEIGHT / 2;
			portsPaddingTop = Math.ceil(raw / PORT_ROW_HEIGHT) * PORT_ROW_HEIGHT - raw;
		});
		observer.observe(prePortsEl);
		return () => observer.disconnect();
	});

	function handleResizeEnd(_event: unknown, params: { x: number; y: number; width: number; height: number }) {
		updateNodeDimensions([{
			id,
			width: params.width,
			height: params.height,
			position: { x: params.x, y: params.y },
		}]);
		pushHistory();
	}

	// Get display source (truncate long paths)
	function getDisplaySource(source: string): string {
		if (source === 'Inline') return source;
		// Get just the filename for file paths
		const parts = source.split('/');
		return parts[parts.length - 1];
	}

	// Handle double-click to open subgraph
	function handleDoubleClick() {
		openSubgraphTab(id, data);
	}

	// Handle expand/collapse toggle
	function handleToggleExpand(e: MouseEvent) {
		e.stopPropagation();
		toggleSubgraphExpansion(id);
	}
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	class="subgraph-node shape-{shape}"
	class:selected
	class:invalid={data.isValid === false}
	class:expanded={isExpanded}
	class:single-tip-port={singleTipPort}
	ondblclick={handleDoubleClick}
	style:background={headerColor ? headerColor + '22' : undefined}
>
	<NodeResizer
		minWidth={isExpanded ? 400 : 160}
		minHeight={isExpanded ? 300 : 80}
		isVisible={selected}
		color="var(--node-selected, #3b82f6)"
		onResizeEnd={handleResizeEnd}
	/>
	<!-- Pre-ports area (measured for port alignment) -->
	<div bind:this={prePortsEl}>
		<!-- Header -->
		{#if !hideLabel}
			<div class="node-header" style:background={headerColor || undefined} style:color={fontColor || undefined}>
				<button class="expand-toggle" onclick={handleToggleExpand} title={isExpanded ? 'Collapse subgraph' : 'Expand subgraph inline'}>
					{isExpanded ? '\u25BC' : '\u25B6'}
				</button>
				<span class="subgraph-badge">SG</span>
				<span class="node-label" style:color={fontColor || undefined}>{data.label}</span>
				{#if locked}
					<span class="lock-icon" title="Position locked">&#x1F512;</span>
				{/if}
			</div>
		{/if}

		<!-- Description (shown in both collapsed and expanded views) -->
		{#if data.description && !hideDescription}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="node-description" style:cursor={selected ? 'pointer' : undefined} onclick={selected ? (e: MouseEvent) => { e.stopPropagation(); toggleNodeDescExpanded(id); } : undefined}>
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<span class="desc-chevron" class:expanded={descExpanded} onclick={!selected ? (e: MouseEvent) => { e.stopPropagation(); toggleNodeDescExpanded(id); } : undefined}>&#9656;</span>
				{#if descExpanded}
					<span class="desc-content">{data.description}</span>
				{:else}
					<span class="desc-preview">{data.description.split('\n')[0]}</span>
				{/if}
			</div>
		{/if}
	</div>

	{#if !isExpanded}
		<!-- Ports container (collapsed view) -->
		<div class="ports-container" style:padding-top="{portsPaddingTop}px">
			<PortList nodeId={id} ports={data.inPorts} side="in" {portGroupStates} {hidePortNames} />
			<PortList nodeId={id} ports={data.outPorts} side="out" {portGroupStates} {hidePortNames} />
		</div>

		<!-- Subgraph info footer -->
		<div class="subgraph-info">
			{#if data.nodeCount !== undefined && data.nodeCount !== null}
				<span class="node-count">{data.nodeCount} node{data.nodeCount !== 1 ? 's' : ''}</span>
			{/if}
			<span class="source-info" title={data.source}>{getDisplaySource(data.source || 'Inline')}</span>
		</div>

		<!-- Double-click hint -->
		<div class="edit-hint">Double-click to edit</div>
	{:else}
		<!-- Expanded view: ports on edges, body is container for child nodes -->
		<div class="expanded-ports" style:padding-top="{portsPaddingTop}px">
			<PortList nodeId={id} ports={data.inPorts} side="in" {portGroupStates} {hidePortNames}
				exposedPortNames={exposedInPortNames} />
			<PortList nodeId={id} ports={data.outPorts} side="out" {portGroupStates} {hidePortNames}
				exposedPortNames={exposedOutPortNames} />
		</div>
		<div class="expanded-body">
			<!-- Child nodes rendered by SvelteFlow via parentId -->
		</div>
	{/if}

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
	.subgraph-node {
		background: var(--node-bg, #2d2d2d);
		border: 2px solid var(--subgraph-border, #22c55e);
		border-radius: 8px;
		min-width: 160px;
		font-size: var(--node-title-font-size, 12px);
		width: 100%;
		height: 100%;
		box-sizing: border-box;
		display: flex;
		flex-direction: column;
	}

	/* Larger resize handles when selected */
	:global(.subgraph-node .svelte-flow__resize-control.handle) {
		width: 12px;
		height: 12px;
	}

	.subgraph-node.selected {
		border-color: var(--node-selected, #3b82f6);
		box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3);
	}

	.subgraph-node.invalid {
		border-color: var(--error-color, #ef4444);
	}

	/* ── Expanded state ────────────────────────────── */
	.subgraph-node.expanded {
		border-style: dashed;
		border-width: 2px;
		background: var(--bg-primary, #1a1a1a);
		min-width: 400px;
		min-height: 300px;
	}

	.subgraph-node.expanded.selected {
		border-color: var(--node-selected, #3b82f6);
	}

	.expand-toggle {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 20px;
		height: 20px;
		padding: 0;
		background: rgba(255, 255, 255, 0.15);
		border: none;
		border-radius: 3px;
		color: white;
		font-size: 10px;
		cursor: pointer;
		flex-shrink: 0;
		transition: background 0.15s ease;
	}

	.expand-toggle:hover {
		background: rgba(255, 255, 255, 0.3);
	}

	.expanded-ports {
		display: flex;
		justify-content: space-between;
		padding: 0;
	}

	.expanded-body {
		flex: 1;
		min-height: 200px;
		/* This space is where SvelteFlow renders child nodes via parentId */
	}

	/* ── Shape: rounded ─────────────────────────────── */
	.subgraph-node.shape-rounded {
		border-radius: 16px;
	}
	.subgraph-node.shape-rounded .node-header {
		border-radius: 14px 14px 0 0;
	}

	/* ── Shape: pill / stadium ──────────────────────── */
	.subgraph-node.shape-pill {
		border-radius: 999px;
	}
	.subgraph-node.shape-pill .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
		justify-content: center;
	}
	.subgraph-node.shape-pill .ports-container {
		padding-left: 16px;
		padding-right: 16px;
	}

	/* ── Shape: diamond ─────────────────────────────── */
	.subgraph-node.shape-diamond {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.subgraph-node.shape-diamond::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
		background: var(--subgraph-border, #22c55e);
		z-index: -2;
	}
	.subgraph-node.shape-diamond::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.subgraph-node.shape-diamond.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.subgraph-node.shape-diamond.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.subgraph-node.shape-diamond .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
		justify-content: center;
	}
	.subgraph-node.shape-diamond .ports-container {
		padding: 0 25%;
	}

	/* ── Shape: hexagon ─────────────────────────────── */
	.subgraph-node.shape-hexagon {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.subgraph-node.shape-hexagon::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%);
		background: var(--subgraph-border, #22c55e);
		z-index: -2;
	}
	.subgraph-node.shape-hexagon::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.subgraph-node.shape-hexagon.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.subgraph-node.shape-hexagon.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.subgraph-node.shape-hexagon .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
		justify-content: center;
	}
	.subgraph-node.shape-hexagon .ports-container {
		padding: 0 15%;
	}

	/* ── Shape: cylinder ────────────────────────────── */
	.subgraph-node.shape-cylinder {
		border-radius: 50% / 12px;
		position: relative;
		padding-bottom: 10px;
	}
	.subgraph-node.shape-cylinder .node-header {
		border-radius: 50% 50% 0 0 / 12px 12px 0 0;
	}
	.subgraph-node.shape-cylinder::after {
		content: '';
		position: absolute;
		bottom: -2px;
		left: -2px;
		right: -2px;
		height: 16px;
		border-radius: 0 0 50% 50% / 0 0 100% 100%;
		border: 2px solid var(--subgraph-border, #22c55e);
		border-top: none;
		background: var(--node-bg, #2d2d2d);
	}
	.subgraph-node.shape-cylinder.selected::after {
		border-color: var(--node-selected, #3b82f6);
	}
	.subgraph-node.shape-cylinder.invalid::after {
		border-color: var(--error-color, #ef4444);
	}

	/* ── Shape: triangle-right ──────────────────────── */
	.subgraph-node.shape-triangle-right {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.subgraph-node.shape-triangle-right::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(0% 0%, 100% 50%, 0% 100%);
		background: var(--subgraph-border, #22c55e);
		z-index: -2;
	}
	.subgraph-node.shape-triangle-right::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(0% 0%, 100% 50%, 0% 100%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.subgraph-node.shape-triangle-right.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.subgraph-node.shape-triangle-right.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.subgraph-node.shape-triangle-right .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
	}
	.subgraph-node.shape-triangle-right .ports-container {
		padding-right: 30%;
	}
	/* Single output handle on the pointy (right) side: pin to the tip */
	.subgraph-node.shape-triangle-right.single-tip-port :global(.out-ports .port-row) {
		position: static;
	}
	.subgraph-node.shape-triangle-right.single-tip-port :global(.out-ports .svelte-flow__handle-right:not(.hidden-handle)) {
		top: 50% !important;
	}

	/* ── Shape: triangle-left ───────────────────────── */
	.subgraph-node.shape-triangle-left {
		background: transparent;
		border-color: transparent;
		position: relative;
		overflow: visible;
	}
	.subgraph-node.shape-triangle-left::before {
		content: '';
		position: absolute;
		inset: -2px;
		clip-path: polygon(100% 0%, 0% 50%, 100% 100%);
		background: var(--subgraph-border, #22c55e);
		z-index: -2;
	}
	.subgraph-node.shape-triangle-left::after {
		content: '';
		position: absolute;
		inset: 0;
		clip-path: polygon(100% 0%, 0% 50%, 100% 100%);
		background: var(--node-bg, #2d2d2d);
		z-index: -1;
	}
	.subgraph-node.shape-triangle-left.selected::before {
		background: var(--node-selected, #3b82f6);
	}
	.subgraph-node.shape-triangle-left.invalid::before {
		background: var(--error-color, #ef4444);
	}
	.subgraph-node.shape-triangle-left .node-header {
		background: transparent;
		border-bottom: none;
		border-radius: 0;
	}
	.subgraph-node.shape-triangle-left .ports-container {
		padding-left: 30%;
	}
	/* Single input handle on the pointy (left) side: pin to the tip */
	.subgraph-node.shape-triangle-left.single-tip-port :global(.in-ports .port-row) {
		position: static;
	}
	.subgraph-node.shape-triangle-left.single-tip-port :global(.in-ports .svelte-flow__handle-left:not(.hidden-handle)) {
		top: 50% !important;
	}

	.node-header {
		background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%);
		padding: 8px 12px;
		border-radius: 6px 6px 0 0;
		display: flex;
		align-items: center;
		gap: 6px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.subgraph-badge {
		background: rgba(255, 255, 255, 0.2);
		padding: 2px 6px;
		border-radius: 3px;
		font-size: 10px;
		font-weight: 600;
		color: white;
	}

	.node-label {
		font-weight: 500;
		color: var(--text-primary, #fff);
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.lock-icon {
		font-size: 10px;
		opacity: 0.6;
		flex-shrink: 0;
		margin-left: auto;
	}

	.node-description {
		padding: 4px 10px;
		border-bottom: 1px solid var(--border-color, #404040);
		display: flex;
		align-items: flex-start;
		gap: 4px;
		font-size: var(--node-desc-font-size, 10px);
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
		cursor: pointer;
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
		padding: 0 0 8px 0;
		min-height: 40px;
		flex: 1;
	}

	.subgraph-info {
		display: flex;
		justify-content: space-between;
		padding: 4px 12px;
		border-top: 1px solid var(--border-color, #404040);
		background: rgba(34, 197, 94, 0.1);
		font-size: 10px;
	}

	.node-count {
		color: var(--text-secondary, #a0a0a0);
	}

	.source-info {
		color: var(--text-secondary, #666);
		opacity: 0.7;
		max-width: 100px;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.edit-hint {
		padding: 4px 12px;
		text-align: center;
		color: var(--text-secondary, #666);
		font-size: 9px;
		font-style: italic;
		opacity: 0.6;
		border-top: 1px solid var(--border-color, #404040);
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
	:global(.subgraph-node .svelte-flow__handle) {
		width: 10px;
		height: 10px;
		border: 2px solid var(--bg-secondary, #242424);
	}

	:global(.subgraph-node .svelte-flow__handle-left) {
		background: var(--port-input, #22c55e);
	}

	:global(.subgraph-node .svelte-flow__handle-right) {
		background: var(--port-output, #f59e0b);
	}

	:global(.subgraph-node .svelte-flow__handle.connecting) {
		background: var(--accent-color, #3b82f6);
	}

	/* Group handle styling — slightly larger, rounded rectangle */
	:global(.subgraph-node .svelte-flow__handle.group-handle) {
		width: 12px;
		height: 12px;
		border-radius: 3px;
	}

	/* Root group handle styling — larger than sub-group handles */
	:global(.subgraph-node .svelte-flow__handle.root-group-handle) {
		width: 14px;
		height: 14px;
		border-radius: 3px;
	}
</style>
