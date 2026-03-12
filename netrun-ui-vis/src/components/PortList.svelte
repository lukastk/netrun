<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import {
		buildPortTree,
		makeGroupHandleId,
		ROOT_GROUP_PATH,
		type PortGroupTree,
		type PortLeaf,
	} from '../utils/portGroups.js';
	import { isPortGroupCollapsed } from '../utils/portGroupCollapse.js';
	import { extractPortTypeName } from '../utils/portTypeDetection.js';
	import type { PortConfig } from '../types/nodes.js';
	import type { PortTypeConfig } from '../types/events.js';

	interface Props {
		nodeId: string;
		ports: PortConfig[];
		side: 'in' | 'out';
		portGroupStates?: Record<string, boolean>;
		hidePortNames?: boolean;
		/** Port names that need inner handles for exposed port edges (expanded subgraphs) */
		exposedPortNames?: string[];
		/** Signal port configuration (prefix, suffix, types) */
		signalConfig?: PortTypeConfig;
		/** Control port configuration (prefix, suffix, types) */
		controlConfig?: PortTypeConfig;
		/** Called when a port group is toggled */
		onPortGroupToggle?: (event: { nodeId: string; side: 'in' | 'out'; groupPath: string; portCount: number }) => void;
	}

	let {
		nodeId, ports, side, portGroupStates, hidePortNames = false,
		exposedPortNames, signalConfig, controlConfig, onPortGroupToggle,
	}: Props = $props();

	// Build the port tree reactively
	let portTree = $derived(buildPortTree(ports));

	// Root group state
	let totalPortCount = $derived(ports.length);
	let rootHandleId = $derived(makeGroupHandleId(side, ROOT_GROUP_PATH));

	// Check collapsed state (reactive via portGroupStates prop)
	function collapsed(groupPath: string, portCount: number): boolean {
		return isPortGroupCollapsed(portGroupStates, side, groupPath, portCount);
	}

	function rootCollapsed(): boolean {
		return isPortGroupCollapsed(portGroupStates, side, ROOT_GROUP_PATH, totalPortCount);
	}

	function handleToggleGroup(groupPath: string, portCount: number) {
		onPortGroupToggle?.({ nodeId, side, groupPath, portCount });
	}

	const HIDDEN_STYLE = 'opacity:0;width:0;height:0;pointer-events:none;';

	let handlePosition = $derived(side === 'in' ? Position.Left : Position.Right);
	let handleType = $derived(side === 'in' ? 'target' as const : 'source' as const);
</script>

{#snippet portLeaf(item: PortLeaf, isHidden: boolean)}
	{#if isHidden}
		<!-- Hidden: only render the handle for edge routing, no visible row -->
		<Handle
			type={handleType}
			position={handlePosition}
			id={item.port.name}
			style="opacity:0;width:0;height:0;pointer-events:none;"
			class="hidden-handle"
		/>
	{:else}
		{@const signalType = item.port.isSignal ? extractPortTypeName(item.port.name, signalConfig) : null}
		{@const controlType = item.port.isControl ? extractPortTypeName(item.port.name, controlConfig) : null}
		<div class="port-row" class:port-indented={item.depth > 0} class:signal-port={!!signalType} class:control-port={!!controlType} style:padding-left="{item.depth * 12}px">
			{#if side === 'in'}
				<Handle
					type={handleType}
					position={handlePosition}
					id={item.port.name}
					class={signalType ? 'signal-handle' : controlType ? 'control-handle' : ''}
				/>
			{/if}
			{#if !hidePortNames}
				{#if signalType}
					<span class="port-label signal-label">{signalType}</span>
				{:else if controlType}
					<span class="port-label control-label">{controlType}</span>
				{:else}
					{#if side === 'out' && item.port.type && item.port.type !== 'any'}
						<span class="port-type">{item.port.type}</span>
					{/if}
					<span class="port-label">{item.port.name.split('.').pop()}</span>
					{#if side === 'in' && item.port.type && item.port.type !== 'any'}
						<span class="port-type">{item.port.type}</span>
					{/if}
				{/if}
			{/if}
			{#if side === 'out'}
				<Handle
					type={handleType}
					position={handlePosition}
					id={item.port.name}
					class={signalType ? 'signal-handle' : controlType ? 'control-handle' : ''}
				/>
			{/if}
			{#if exposedPortNames?.includes(item.port.name)}
				<!-- Inner handle for exposed port edges: opposite type/position so edges curve inward -->
				<Handle
					type={side === 'in' ? 'source' : 'target'}
					position={side === 'in' ? Position.Right : Position.Left}
					id={item.port.name}
					class="inner-exposed-handle {side === 'in' ? 'inner-exposed-left' : 'inner-exposed-right'}"
					style="opacity:0;width:0;height:0;min-width:0;min-height:0;pointer-events:none;"
				/>
			{/if}
		</div>
	{/if}
{/snippet}

{#snippet groupNode(item: PortGroupTree)}
	{@const isCollapsed = collapsed(item.fullPath, item.portCount)}
	<div class="port-group" style:padding-left="{(item.fullPath.split('.').length - 1) * 12}px">
		<!-- Group header row — entire row is clickable -->
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div
			class="group-header"
			onclick={() => handleToggleGroup(item.fullPath, item.portCount)}
			title={isCollapsed ? 'Expand group' : 'Collapse group'}
		>
			{#if side === 'in'}
				<!-- Group handle: visible when collapsed, hidden when expanded -->
				<Handle
					type={handleType}
					position={handlePosition}
					id={makeGroupHandleId(side, item.fullPath)}
					style={isCollapsed ? undefined : HIDDEN_STYLE}
					class="group-handle {isCollapsed ? '' : 'hidden-handle'}"
				/>
			{/if}
			<span class="chevron" class:expanded={!isCollapsed}>{'\u25B6'}</span>
			{#if !hidePortNames}
				<span class="group-name">{item.name}</span>
				{#if isCollapsed}
					<span class="group-count">({item.portCount})</span>
				{/if}
			{/if}
			{#if side === 'out'}
				<Handle
					type={handleType}
					position={handlePosition}
					id={makeGroupHandleId(side, item.fullPath)}
					style={isCollapsed ? undefined : HIDDEN_STYLE}
					class="group-handle {isCollapsed ? '' : 'hidden-handle'}"
				/>
			{/if}
		</div>

		<!-- Children: visible when expanded, hidden handles when collapsed -->
		{#each item.children as child}
			{#if child.type === 'port'}
				{@render portLeaf(child, isCollapsed)}
			{:else}
				{#if isCollapsed}
					<!-- When parent is collapsed, render nested group's leaf ports as hidden handles -->
					{@render hiddenGroupHandles(child)}
				{:else}
					{@render groupNode(child)}
				{/if}
			{/if}
		{/each}
	</div>
{/snippet}

{#snippet hiddenGroupHandles(item: PortGroupTree)}
	<!-- Render all handles in a collapsed parent group as hidden DOM elements -->
	<Handle
		type={handleType}
		position={handlePosition}
		id={makeGroupHandleId(side, item.fullPath)}
		style="opacity:0;width:0;height:0;pointer-events:none;"
		class="hidden-handle"
	/>
	{#each item.children as child}
		{#if child.type === 'port'}
			<Handle
				type={handleType}
				position={handlePosition}
				id={child.port.name}
				style="opacity:0;width:0;height:0;pointer-events:none;"
				class="hidden-handle"
			/>
		{:else}
			{@render hiddenGroupHandles(child)}
		{/if}
	{/each}
{/snippet}

<div class="ports {side}-ports">
{#if ports.length > 0}
	{@const isRootCollapsed = rootCollapsed()}
		{#if ports.length > 1}
			<!-- Root group header (only for 2+ ports) -->
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div
				class="root-group-header"
				onclick={() => handleToggleGroup(ROOT_GROUP_PATH, totalPortCount)}
				title={isRootCollapsed ? 'Expand all ports' : 'Collapse all ports'}
			>
				{#if side === 'in'}
					<Handle
						type={handleType}
						position={handlePosition}
						id={rootHandleId}
						style={isRootCollapsed ? undefined : HIDDEN_STYLE}
						class="root-group-handle {isRootCollapsed ? '' : 'hidden-handle'}"
					/>
				{/if}
				<span class="chevron" class:expanded={!isRootCollapsed}>{'\u25B6'}</span>
				{#if isRootCollapsed}
					<span class="group-count">({totalPortCount})</span>
				{/if}
				{#if side === 'out'}
					<Handle
						type={handleType}
						position={handlePosition}
						id={rootHandleId}
						style={isRootCollapsed ? undefined : HIDDEN_STYLE}
						class="root-group-handle {isRootCollapsed ? '' : 'hidden-handle'}"
					/>
				{/if}
			</div>

			<!-- Children: visible when root expanded, hidden handles when root collapsed -->
			{#if isRootCollapsed}
				<!-- Render all port handles as hidden for edge routing -->
				{#each portTree as item}
					{#if item.type === 'port'}
						<Handle
							type={handleType}
							position={handlePosition}
							id={item.port.name}
							style="opacity:0;width:0;height:0;pointer-events:none;"
							class="hidden-handle"
						/>
					{:else}
						{@render hiddenGroupHandles(item)}
					{/if}
				{/each}
			{:else}
				{#each portTree as item}
					{#if item.type === 'port'}
						{@render portLeaf(item, false)}
					{:else}
						{@render groupNode(item)}
					{/if}
				{/each}
			{/if}
		{:else}
			<!-- Single port: render directly without collapse -->
			{#each portTree as item}
				{#if item.type === 'port'}
					{@render portLeaf(item, false)}
				{:else}
					{@render groupNode(item)}
				{/if}
			{/each}
		{/if}
{/if}
</div>

<style>
	.ports {
		display: flex;
		flex-direction: column;
		gap: 0;
	}

	.in-ports {
		align-items: flex-start;
		padding-left: 12px;
	}

	.out-ports {
		align-items: flex-end;
		padding-right: 12px;
	}

	.port-row {
		position: relative;
		display: flex;
		align-items: center;
		gap: 4px;
		height: 20px;
	}

	/* Offset handles to reach node edge (compensating for .in-ports/.out-ports padding) */
	.in-ports .port-row :global(.svelte-flow__handle-left:not(.inner-exposed-handle)) {
		left: -12px !important;
	}
	.out-ports .port-row :global(.svelte-flow__handle-right:not(.inner-exposed-handle)) {
		right: -12px !important;
	}

	/* Inner exposed handles: positioned at same edge but with opposite routing direction */
	.port-row :global(.inner-exposed-left.svelte-flow__handle-right) {
		right: auto !important;
		left: -12px !important;
		transform: translate(-50%, -50%) !important;
	}
	.port-row :global(.inner-exposed-right.svelte-flow__handle-left) {
		left: auto !important;
		right: -12px !important;
		transform: translate(50%, -50%) !important;
	}

	.port-label {
		color: var(--netrun-text-secondary, #a0a0a0);
		font-size: var(--netrun-node-port-font-size, 11px);
	}

	.port-type {
		color: var(--netrun-text-secondary, #666);
		font-size: var(--netrun-node-port-font-size, 11px);
		opacity: 0.7;
	}

	.port-group {
		display: flex;
		flex-direction: column;
		gap: 0;
	}

	.group-header {
		display: flex;
		align-items: center;
		gap: 4px;
		position: relative;
		height: 20px;
		cursor: pointer;
		padding: 0 4px;
		margin: 0 -4px;
		border-radius: 4px;
		transition: background 0.15s;
	}

	.group-header:hover {
		background: rgba(255, 255, 255, 0.06);
	}

	.chevron {
		display: inline-block;
		font-size: 8px;
		color: var(--netrun-text-secondary, #a0a0a0);
		transition: transform 0.15s;
	}

	.chevron.expanded {
		transform: rotate(90deg);
	}

	.group-name {
		color: var(--netrun-text-primary, #fff);
		font-size: var(--netrun-node-port-font-size, 11px);
		font-weight: 500;
	}

	.group-count {
		color: var(--netrun-text-secondary, #666);
		font-size: 10px;
	}

	/* Root group header — subtle by default, visible on hover */
	.root-group-header {
		display: flex;
		align-items: center;
		gap: 4px;
		position: relative;
		height: 20px;
		cursor: pointer;
		padding: 0 4px;
		margin: 0 -4px;
		border-radius: 4px;
		transition: background 0.15s, opacity 0.15s;
		opacity: 0.35;
	}

	.root-group-header:hover {
		background: rgba(255, 255, 255, 0.06);
		opacity: 1;
	}

	/* Group handle styling — slightly larger, rounded rectangle */
	:global(.group-handle) {
		width: 12px !important;
		height: 12px !important;
		border-radius: 3px !important;
	}

	/* Root group handle styling — larger than sub-group handles */
	:global(.root-group-handle) {
		width: 14px !important;
		height: 14px !important;
		border-radius: 3px !important;
	}

	/* Signal port styling */
	.signal-port {
		opacity: 0.7;
	}

	.signal-label {
		font-style: italic;
		color: #d97706 !important;
		font-size: calc(var(--netrun-node-port-font-size, 11px) - 1px);
	}

	:global(.signal-handle) {
		background: #d97706 !important;
		width: 6px !important;
		height: 6px !important;
	}

	/* Control port styling */
	.control-port {
		opacity: 0.7;
	}

	.control-label {
		font-style: italic;
		color: #7c3aed !important;
		font-size: calc(var(--netrun-node-port-font-size, 11px) - 1px);
	}

	:global(.control-handle) {
		background: #7c3aed !important;
		width: 6px !important;
		height: 6px !important;
	}
</style>
