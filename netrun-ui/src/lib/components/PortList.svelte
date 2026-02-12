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
	import {
		isGroupCollapsed,
		toggleGroup,
		portGroupOverrides,
	} from '$lib/stores/portGroupStore';
	import type { PortConfig } from '$lib/stores/flowStore';

	interface Props {
		nodeId: string;
		ports: PortConfig[];
		side: 'in' | 'out';
	}

	let { nodeId, ports, side }: Props = $props();

	// Build the port tree reactively
	let portTree = $derived(buildPortTree(ports));

	// Subscribe to overrides for reactivity
	let _overrides = $derived($portGroupOverrides);

	// Root group state
	let totalPortCount = $derived(ports.length);
	let rootHandleId = $derived(makeGroupHandleId(side, ROOT_GROUP_PATH));

	// Check collapsed state (reactive via _overrides dependency)
	function collapsed(groupPath: string, portCount: number): boolean {
		// Touch _overrides to establish reactivity
		void _overrides;
		return isGroupCollapsed(nodeId, side, groupPath, portCount);
	}

	function rootCollapsed(): boolean {
		void _overrides;
		return isGroupCollapsed(nodeId, side, ROOT_GROUP_PATH, totalPortCount);
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
		rootCollapsed() ? [rootHandleId] : getChildVisibleIds(portTree)
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
					style={getHandleStyle(item.port.name)}
				/>
			{/if}
			{#if side === 'out' && item.port.type && item.port.type !== 'any'}
				<span class="port-type">{item.port.type}</span>
			{/if}
			<span class="port-label">{item.port.name.split('.').pop()}</span>
			{#if side === 'in' && item.port.type && item.port.type !== 'any'}
				<span class="port-type">{item.port.type}</span>
			{/if}
			{#if side === 'out'}
				<Handle
					type={handleType}
					position={handlePosition}
					id={item.port.name}
					style={getHandleStyle(item.port.name)}
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
			onclick={() => toggleGroup(nodeId, side, item.fullPath, item.portCount)}
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
			<span class="group-name">{item.name}</span>
			{#if isCollapsed}
				<span class="group-count">({item.portCount})</span>
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

{#if ports.length > 0}
	{@const isRootCollapsed = rootCollapsed()}
	<div class="ports {side}-ports">
		<!-- Root group header -->
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div
			class="root-group-header"
			onclick={() => toggleGroup(nodeId, side, ROOT_GROUP_PATH, totalPortCount)}
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
	</div>
{/if}

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
		display: flex;
		align-items: center;
		gap: 4px;
		position: relative;
		min-height: 18px;
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

	/* Root group header */
	.root-group-header {
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

	.root-group-header:hover {
		background: rgba(255, 255, 255, 0.06);
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
