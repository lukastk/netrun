<script lang="ts">
	import {
		addNode,
		createDecorationNode,
		selectedNodeIds,
		selectedEdgeIds,
		DECORATION_TYPES,
		type DecorationType,
	} from '$lib/stores/flowStore';

	let isOpen = $state(false);

	function toggle() {
		isOpen = !isOpen;
	}

	function closeDropdown() {
		isOpen = false;
	}

	function handleAdd(decorationType: DecorationType) {
		isOpen = false;
		const node = createDecorationNode({ x: 200, y: 200 }, decorationType);
		addNode(node);
		selectedNodeIds.set(new Set([node.id]));
		selectedEdgeIds.set(new Set());
	}
</script>

<svelte:window onclick={(e) => {
	if (isOpen) {
		const target = e.target as HTMLElement;
		if (!target.closest('.decoration-dropdown')) {
			closeDropdown();
		}
	}
}} />

<div class="decoration-dropdown">
	<button class="decoration-button" onclick={toggle}>
		<span class="icon">&#x25A0;</span>
		<span class="label">Decor</span>
		<span class="caret">{isOpen ? '\u25B4' : '\u25BE'}</span>
	</button>

	{#if isOpen}
		<div class="dropdown-menu">
			{#each DECORATION_TYPES as dt}
				<button class="dropdown-item" onclick={() => handleAdd(dt.value)}>
					<span class="item-icon">
						{#if dt.value === 'rectangle'}&#x25AD;
						{:else if dt.value === 'rounded-rectangle'}&#x25A2;
						{:else if dt.value === 'circle'}&#x25CB;
						{:else if dt.value === 'triangle'}&#x25B3;
						{:else if dt.value === 'divider'}&#x2500;
						{:else if dt.value === 'label'}T
						{:else if dt.value === 'textbox'}&#x2263;
						{:else if dt.value === 'image'}&#x1F5BC;
						{/if}
					</span>
					<span class="item-label">{dt.label}</span>
				</button>
			{/each}
		</div>
	{/if}
</div>

<style>
	.decoration-dropdown {
		position: relative;
	}

	.decoration-button {
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

	.decoration-button:hover {
		background: var(--border-color, #404040);
		border-color: var(--border-color, #404040);
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
		min-width: 200px;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
		z-index: 100;
		overflow: hidden;
	}

	.dropdown-item {
		display: flex;
		align-items: center;
		gap: 8px;
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

	.item-icon {
		font-size: 16px;
		width: 20px;
		text-align: center;
		opacity: 0.8;
	}

	.item-label {
		font-weight: 500;
	}
</style>
