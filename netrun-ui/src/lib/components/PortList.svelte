<script lang="ts">
	import { Handle, Position } from '@xyflow/svelte';
	import {
		buildPortTree,
		makeGroupHandleId,
		ROOT_GROUP_PATH,
		type PortDisplayItem,
		type PortGroupTree,
		type PortLeaf,
	} from '$lib/utils/portGroups';
	import { isPortGroupCollapsed } from '$lib/stores/portGroupStore';
	import { toggleNodePortGroup } from '$lib/stores/flowStore';
	import type { PortConfig } from '$lib/stores/flowStore';

	interface Props {
		nodeId: string;
		ports: PortConfig[];
		side: 'in' | 'out';
		portGroupStates?: Record<string, boolean>;
		hidePortNames?: boolean;
		/** Port names that need inner handles for exposed port edges (expanded subgraphs) */
		exposedPortNames?: string[];
	}

	let { nodeId, ports, side, portGroupStates, hidePortNames = false, exposedPortNames }: Props = $props();

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

	// Collect visible handle IDs for children (excluding root)
	function getChildVisibleIds(items: PortDisplayItem[]): string[] {
		const ids: string[] = [];
		for (const item of items) {
			if (item.type === 'port') {
				ids.push(item.port.name);
			} else {
				if (collapsed(item.fullPath, item.portCount)) {
					ids.push(makeGroupHandleId(side, item.fullPath));
				} else {
					ids.push(...getChildVisibleIds(item.children));
				}
			}
		}
		return ids;
	}

	let visibleHandleIds = $derived(
		ports.length <= 1
			? getChildVisibleIds(portTree)
			: rootCollapsed() ? [rootHandleId] : getChildVisibleIds(portTree)
	);

	function getHandleStyle(handleId: string): string {
		const total = visibleHandleIds.length;
		const index = visibleHandleIds.indexOf(handleId);
		if (index === -1 || total === 0) return 'top: 50%';
		if (total === 1) return 'top: 50%';
		const spacing = 100 / (total + 1);
		const top = spacing * (index + 1);
		return `top: ${top}%`;
	}

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
		<div class="port-row" class:port-indented={item.depth > 0} style:padding-left="{item.depth * 12}px">
			{#if side === 'in'}
				<Handle
					type={handleType}
					position={handlePosition}
					id={item.port.name}
				/>
			{/if}
			{#if !hidePortNames}
				{#if side === 'out' && item.port.type && item.port.type !== 'any'}
					<span class="port-type">{item.port.type}</span>
				{/if}
				<span class="port-label">{item.port.name.split('.').pop()}</span>
				{#if side === 'in' && item.port.type && item.port.type !== 'any'}
					<span class="port-type">{item.port.type}</span>
				{/if}
			{/if}
			{#if side === 'out'}
				<Handle
					type={handleType}
					position={handlePosition}
					id={item.port.name}
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
			onclick={() => toggleNodePortGroup(nodeId, side, item.fullPath, item.portCount)}
			title={isCollapsed ? 'Expand group' : 'Collapse group'}
		>
			{#if side === 'in'}
				<!-- Group handle: visible when collapsed, hidden when expanded -->
				<Handle
					type={handleType}
					position={handlePosition}
					id={makeGroupHandleId(side, item.fullPath)}
					style={isCollapsed
						? getHandleStyle(makeGroupHandleId(side, item.fullPath))
						: 'opacity:0;width:0;height:0;pointer-events:none;'}
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
					style={isCollapsed
						? getHandleStyle(makeGroupHandleId(side, item.fullPath))
						: 'opacity:0;width:0;height:0;pointer-events:none;'}
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
				onclick={() => toggleNodePortGroup(nodeId, side, ROOT_GROUP_PATH, totalPortCount)}
				title={isRootCollapsed ? 'Expand all ports' : 'Collapse all ports'}
			>
				{#if side === 'in'}
					<Handle
						type={handleType}
						position={handlePosition}
						id={rootHandleId}
						style={isRootCollapsed
							? getHandleStyle(rootHandleId)
							: 'opacity:0;width:0;height:0;pointer-events:none;'}
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
						style={isRootCollapsed
							? getHandleStyle(rootHandleId)
							: 'opacity:0;width:0;height:0;pointer-events:none;'}
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
		gap: 2px;
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
		min-height: 18px;
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
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
	}

	.port-type {
		color: var(--text-secondary, #666);
		font-size: 10px;
		opacity: 0.7;
	}

	.port-group {
		display: flex;
		flex-direction: column;
		gap: 2px;
	}

	.group-header {
		display: flex;
		align-items: center;
		gap: 4px;
		position: relative;
		min-height: 18px;
		cursor: pointer;
		padding: 2px 4px;
		margin: -2px -4px;
		border-radius: 4px;
		transition: background 0.15s;
	}

	.group-header:hover {
		background: rgba(255, 255, 255, 0.06);
	}

	.chevron {
		display: inline-block;
		font-size: 8px;
		color: var(--text-secondary, #a0a0a0);
		transition: transform 0.15s;
	}

	.chevron.expanded {
		transform: rotate(90deg);
	}

	.group-name {
		color: var(--text-primary, #fff);
		font-size: 11px;
		font-weight: 500;
	}

	.group-count {
		color: var(--text-secondary, #666);
		font-size: 10px;
	}

	/* Root group header — subtle by default, visible on hover */
	.root-group-header {
		display: flex;
		align-items: center;
		gap: 4px;
		position: relative;
		min-height: 14px;
		cursor: pointer;
		padding: 1px 4px;
		margin: -1px -4px;
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
</style>
