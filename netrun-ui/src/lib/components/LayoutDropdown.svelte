<script lang="ts">
	import { get } from 'svelte/store';
	import { LAYOUT_ALGORITHMS, computeLayout } from '$lib/utils/autoLayout';
	import { svelteFlowRef } from '$lib/stores/svelteFlowStore';
	import { nodes, edges, pushHistory, updateNodePositions } from '$lib/stores/flowStore';
	import { toasts } from '$lib/stores/toastStore';

	let isOpen = $state(false);
	let isLayingOut = $state(false);

	function toggle() {
		isOpen = !isOpen;
	}

	function closeDropdown() {
		isOpen = false;
	}

	async function applyLayout(algorithmId: string) {
		isOpen = false;

		const currentNodes = get(nodes);
		// Filter out decoration nodes — they don't participate in layout
		const layoutNodes = currentNodes.filter(n => (n.data as Record<string, unknown>)?.nodeType !== 'decoration');
		if (layoutNodes.length < 2) {
			toasts.info(layoutNodes.length === 0 ? 'No nodes to layout.' : 'Need at least 2 nodes to layout.');
			return;
		}

		const flowRef = get(svelteFlowRef);
		if (!flowRef) {
			toasts.error('Flow editor not available.');
			return;
		}

		isLayingOut = true;
		try {
			// Get measured dimensions from SvelteFlow's internal state
			const measuredNodes = flowRef.getNodes();
			const nodeDimensions = new Map<string, { width: number; height: number }>();
			for (const n of measuredNodes) {
				if (n.measured?.width && n.measured?.height) {
					nodeDimensions.set(n.id, { width: n.measured.width, height: n.measured.height });
				}
			}

			const currentEdges = get(edges);
			const result = await computeLayout(layoutNodes, currentEdges, algorithmId, nodeDimensions);

			pushHistory();
			updateNodePositions(result.positions);
			flowRef.fitView({ padding: 0.2, maxZoom: 1.5 });
		} catch (e) {
			toasts.error(`Layout failed: ${(e as Error).message}`);
		} finally {
			isLayingOut = false;
		}
	}
</script>

<svelte:window onclick={(e) => {
	if (isOpen) {
		const target = e.target as HTMLElement;
		if (!target.closest('.layout-dropdown')) {
			closeDropdown();
		}
	}
}} />

<div class="layout-dropdown">
	<button class="layout-button" onclick={toggle} disabled={isLayingOut}>
		<span class="icon">&#x2B9E;</span>
		<span class="label">{isLayingOut ? 'Laying out...' : 'Layout'}</span>
		<span class="caret">{isOpen ? '\u25B4' : '\u25BE'}</span>
	</button>

	{#if isOpen}
		<div class="dropdown-menu">
			{#each LAYOUT_ALGORITHMS as algo}
				<button class="dropdown-item" onclick={() => applyLayout(algo.id)}>
					<span class="item-label">{algo.label}</span>
					<span class="item-desc">{algo.description}</span>
				</button>
			{/each}
		</div>
	{/if}
</div>

<style>
	.layout-dropdown {
		position: relative;
	}

	.layout-button {
		display: flex;
		align-items: center;
		gap: 6px;
		padding: 6px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid transparent;
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.layout-button:hover:not(:disabled) {
		background: var(--border-color, #404040);
		border-color: var(--border-color, #404040);
	}

	.layout-button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.icon {
		font-size: 14px;
	}

	.label {
		font-weight: 500;
	}

	.caret {
		font-size: 10px;
		opacity: 0.7;
	}

	.dropdown-menu {
		position: absolute;
		top: 100%;
		right: 0;
		margin-top: 4px;
		min-width: 260px;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
		z-index: 100;
		overflow: hidden;
	}

	.dropdown-item {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 2px;
		width: 100%;
		padding: 8px 12px;
		background: none;
		border: none;
		border-radius: 0;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
		text-align: left;
		transition: background 0.1s ease;
	}

	.dropdown-item:hover {
		background: var(--bg-tertiary, #2d2d2d);
	}

	.dropdown-item + .dropdown-item {
		border-top: 1px solid var(--border-color, #404040);
	}

	.item-label {
		font-weight: 500;
	}

	.item-desc {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}
</style>
