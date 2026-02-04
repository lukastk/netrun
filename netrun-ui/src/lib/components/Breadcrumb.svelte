<script lang="ts">
	import { activeTab, navigateToBreadcrumb } from '$lib/stores/tabsStore';

	// Get breadcrumb from subgraph context
	const breadcrumb = $derived(
		$activeTab?.subgraphContext?.path || [$activeTab?.fileName || 'Untitled']
	);

	// Handle click on breadcrumb item
	function handleClick(index: number) {
		navigateToBreadcrumb(index);
	}
</script>

{#if breadcrumb.length > 1}
	<nav class="breadcrumb" aria-label="Subgraph navigation">
		{#each breadcrumb as item, i}
			{#if i > 0}
				<span class="separator">/</span>
			{/if}
			{#if i < breadcrumb.length - 1}
				<button
					class="breadcrumb-item clickable"
					onclick={() => handleClick(i)}
					type="button"
				>
					{item}
				</button>
			{:else}
				<span class="breadcrumb-item current">{item}</span>
			{/if}
		{/each}
	</nav>
{/if}

<style>
	.breadcrumb {
		display: flex;
		align-items: center;
		gap: 4px;
		padding: 4px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border-bottom: 1px solid var(--border-color, #404040);
		font-size: 12px;
		min-height: 28px;
	}

	.separator {
		color: var(--text-secondary, #a0a0a0);
	}

	.breadcrumb-item {
		color: var(--text-secondary, #a0a0a0);
	}

	.breadcrumb-item.clickable {
		background: none;
		border: none;
		padding: 2px 6px;
		border-radius: 4px;
		cursor: pointer;
		font-size: inherit;
		font-family: inherit;
	}

	.breadcrumb-item.clickable:hover {
		background: var(--bg-hover, #404040);
		color: var(--text-primary, #fff);
	}

	.breadcrumb-item.current {
		color: var(--text-primary, #fff);
		font-weight: 500;
	}
</style>
