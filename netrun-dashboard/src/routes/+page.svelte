<script lang="ts">
	import { getRegistryState } from '$lib/stores/registryStore.svelte.js';
	import { getNetState, connectToNet, disconnectFromNet } from '$lib/stores/netStore.svelte.js';
	import StatusBar from '$lib/components/StatusBar.svelte';
	import NetGraphView from '$lib/components/NetGraphView.svelte';

	const registry = getRegistryState();
	const netState = getNetState();

	// React to selection changes
	let lastUrl: string | null = null;

	$effect(() => {
		const url = registry.selectedUrl;
		if (url !== lastUrl) {
			lastUrl = url;
			if (url) {
				connectToNet(url);
			} else {
				disconnectFromNet();
			}
		}
	});

	const selectedName = $derived(
		registry.nets.find((n) => n.url === registry.selectedUrl)?.name ?? 'unknown',
	);
</script>

{#if registry.selectedUrl}
	<StatusBar name={selectedName} state={netState.liveState} connected={netState.connected} />
	{#if netState.config}
		<NetGraphView config={netState.config} liveState={netState.liveState} />
	{:else}
		<div class="loading">Loading config...</div>
	{/if}
{:else}
	<div class="empty-state">
		<div class="empty-title">netrun dashboard</div>
		<div class="empty-desc">Select a net from the sidebar or wait for one to register.</div>
	</div>
{/if}

<style>
	.empty-state {
		flex: 1;
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		gap: 8px;
	}

	.empty-title {
		font-size: 20px;
		font-weight: 600;
		color: var(--text-secondary);
	}

	.empty-desc {
		font-size: 13px;
		color: var(--text-secondary);
		opacity: 0.6;
	}

	.loading {
		flex: 1;
		display: flex;
		align-items: center;
		justify-content: center;
		color: var(--text-secondary);
		font-size: 13px;
	}
</style>
