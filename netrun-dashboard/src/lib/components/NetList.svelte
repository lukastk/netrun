<script lang="ts">
	import { getRegistryState, selectNet, addManualNet } from '../stores/registryStore.svelte.js';

	interface Props {
		hideHeader?: boolean;
	}

	let { hideHeader = false }: Props = $props();

	const registry = getRegistryState();

	let manualName = $state('');
	let manualUrl = $state('');
	let adding = $state(false);

	async function handleAdd() {
		if (!manualName.trim() || !manualUrl.trim()) return;
		adding = true;
		try {
			await addManualNet(manualName.trim(), manualUrl.trim());
			manualName = '';
			manualUrl = '';
		} catch {
			// ignore
		}
		adding = false;
	}
</script>

<div class="net-list">
	{#if !hideHeader}
		<div class="header">Nets</div>
	{/if}

	<div class="list">
		{#each registry.nets as net (net.url)}
			<button
				class="net-item"
				class:selected={registry.selectedUrl === net.url}
				onclick={() => selectNet(net.url)}
			>
				<span class="health-dot" class:healthy={net.healthy} class:unhealthy={!net.healthy}></span>
				<span class="net-name">{net.name}</span>
			</button>
		{:else}
			<div class="empty">No nets registered</div>
		{/each}
	</div>

	<div class="add-section">
		<div class="add-label">Add manually</div>
		<input bind:value={manualName} placeholder="Name" />
		<input bind:value={manualUrl} placeholder="http://host:port" />
		<button onclick={handleAdd} disabled={adding}>Add</button>
	</div>
</div>

<style>
	.net-list {
		display: flex;
		flex-direction: column;
		height: 100%;
		overflow: hidden;
	}

	.header {
		padding: 12px 16px;
		font-size: 13px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		color: var(--text-secondary);
		border-bottom: 1px solid var(--border-color);
	}

	.list {
		flex: 1;
		overflow-y: auto;
		padding: 4px 0;
	}

	.net-item {
		display: flex;
		align-items: center;
		gap: 8px;
		width: 100%;
		padding: 10px 16px;
		text-align: left;
		border-radius: 0;
		background: transparent;
		font-size: 13px;
	}

	.net-item:hover {
		background: var(--bg-tertiary);
	}

	.net-item.selected {
		background: var(--bg-tertiary);
		border-left: 2px solid var(--accent-color);
	}

	.health-dot {
		width: 8px;
		height: 8px;
		border-radius: 50%;
		flex-shrink: 0;
	}

	.health-dot.healthy {
		background: var(--success-color);
	}

	.health-dot.unhealthy {
		background: var(--error-color);
	}

	.net-name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.empty {
		padding: 16px;
		color: var(--text-secondary);
		font-size: 12px;
		text-align: center;
	}

	.add-section {
		border-top: 1px solid var(--border-color);
		padding: 12px;
		display: flex;
		flex-direction: column;
		gap: 6px;
	}

	.add-label {
		font-size: 11px;
		color: var(--text-secondary);
		text-transform: uppercase;
		letter-spacing: 0.05em;
	}

	.add-section input {
		width: 100%;
	}

	.add-section button {
		width: 100%;
		background: var(--accent-color);
	}

	.add-section button:hover {
		background: var(--accent-hover);
	}

	.add-section button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}
</style>
