<script lang="ts">
	import { getRegistryState } from '$lib/stores/registryStore.svelte.js';
	import { getNetState, connectToNet, disconnectFromNet } from '$lib/stores/netStore.svelte.js';
	import StatusBar from '$lib/components/StatusBar.svelte';
	import NetGraphView from '$lib/components/NetGraphView.svelte';
	import EpochTable from '$lib/components/EpochTable.svelte';
	import LogViewer from '$lib/components/LogViewer.svelte';

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

	let activeTab = $state<'epochs' | 'logs'>('epochs');

	// Resizable bottom panel
	let panelHeight = $state(260);
	let dragging = $state(false);
	let mainAreaEl: HTMLDivElement | undefined = $state();

	function onPointerDown(e: PointerEvent) {
		dragging = true;
		e.preventDefault();
	}

	function onPointerMove(e: PointerEvent) {
		if (!dragging || !mainAreaEl) return;
		const rect = mainAreaEl.getBoundingClientRect();
		const newHeight = rect.bottom - e.clientY;
		panelHeight = Math.max(60, Math.min(newHeight, rect.height - 100));
	}

	function onPointerUp() {
		dragging = false;
	}
</script>

<svelte:window onpointermove={onPointerMove} onpointerup={onPointerUp} />

{#if registry.selectedUrl}
	<StatusBar name={selectedName} state={netState.liveState} connected={netState.connected} />
	{#if netState.config}
		<div class="main-area" bind:this={mainAreaEl}>
			<div class="graph-pane">
				<NetGraphView config={netState.config} liveState={netState.liveState} />
			</div>
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="resize-handle" onpointerdown={onPointerDown}></div>
			<div class="bottom-panel" style:height="{panelHeight}px">
				<div class="tab-bar">
					<button class="tab" class:active={activeTab === 'epochs'} onclick={() => (activeTab = 'epochs')}>
						Epochs
						{#if netState.liveState}
							<span class="tab-count">{netState.liveState.epochs.length}</span>
						{/if}
					</button>
					<button class="tab" class:active={activeTab === 'logs'} onclick={() => (activeTab = 'logs')}>
						Logs
						{#if netState.liveState}
							<span class="tab-count">{netState.liveState.logs.length}</span>
						{/if}
					</button>
				</div>
				<div class="tab-content">
					{#if activeTab === 'epochs'}
						<EpochTable epochs={netState.liveState?.epochs ?? []} />
					{:else}
						<LogViewer logs={netState.liveState?.logs ?? []} />
					{/if}
				</div>
			</div>
		</div>
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
	.main-area {
		flex: 1;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.graph-pane {
		flex: 1;
		min-height: 0;
		position: relative;
	}

	.resize-handle {
		height: 5px;
		cursor: ns-resize;
		background: var(--border-color);
		flex-shrink: 0;
	}

	.resize-handle:hover {
		background: var(--accent-color);
	}

	.bottom-panel {
		min-height: 60px;
		display: flex;
		flex-direction: column;
		background: var(--bg-secondary);
	}

	.tab-bar {
		display: flex;
		gap: 0;
		border-bottom: 1px solid var(--border-color);
		flex-shrink: 0;
	}

	.tab {
		padding: 6px 16px;
		font-size: 12px;
		font-weight: 600;
		color: var(--text-secondary);
		background: transparent;
		border-radius: 0;
		border-bottom: 2px solid transparent;
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.tab:hover {
		color: var(--text-primary);
		background: transparent;
	}

	.tab.active {
		color: var(--text-primary);
		border-bottom-color: var(--accent-color);
	}

	.tab-count {
		font-size: 10px;
		font-weight: 400;
		color: var(--text-secondary);
		background: var(--bg-tertiary);
		padding: 0 5px;
		border-radius: 8px;
	}

	.tab-content {
		flex: 1;
		overflow: hidden;
	}

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
