<script lang="ts">
	import '../app.css';
	import 'netrun-ui-vis/theme.css';
	import '@xyflow/svelte/dist/style.css';
	import NetList from '$lib/components/NetList.svelte';
	import { startPolling, stopPolling } from '$lib/stores/registryStore.svelte.js';
	import { onMount } from 'svelte';

	let { children } = $props();

	let sidebarOpen = $state(true);
	let sidebarWidth = $state(280);
	let draggingSidebar = $state(false);

	function onSidebarPointerDown(e: PointerEvent) {
		draggingSidebar = true;
		e.preventDefault();
	}

	function onPointerMove(e: PointerEvent) {
		if (!draggingSidebar) return;
		sidebarWidth = Math.max(160, Math.min(e.clientX, 500));
	}

	function onPointerUp() {
		draggingSidebar = false;
	}

	onMount(() => {
		startPolling();
		return () => stopPolling();
	});
</script>

<svelte:head>
	<title>netrun dashboard</title>
</svelte:head>

<svelte:window onpointermove={onPointerMove} onpointerup={onPointerUp} />

<div class="layout">
	{#if sidebarOpen}
		<aside class="sidebar" style:width="{sidebarWidth}px" style:min-width="{sidebarWidth}px">
			<div class="sidebar-header">
				<span class="sidebar-title">Nets</span>
				<button class="sidebar-toggle" onclick={() => (sidebarOpen = false)}>&#x2715;</button>
			</div>
			<NetList hideHeader />
		</aside>
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div class="resize-handle-v" onpointerdown={onSidebarPointerDown}></div>
	{:else}
		<button class="sidebar-reopen" onclick={() => (sidebarOpen = true)}>▶</button>
	{/if}
	<main class="content">
		{@render children()}
	</main>
</div>

<style>
	.layout {
		display: flex;
		height: 100vh;
		width: 100vw;
		overflow: hidden;
	}

	.sidebar {
		background: var(--bg-secondary);
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}

	.resize-handle-v {
		width: 5px;
		cursor: ew-resize;
		background: var(--border-color);
		flex-shrink: 0;
	}

	.resize-handle-v:hover {
		background: var(--accent-color);
	}

	.sidebar-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 12px 16px;
		border-bottom: 1px solid var(--border-color);
	}

	.sidebar-title {
		font-size: 13px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		color: var(--text-secondary);
	}

	.sidebar-toggle {
		padding: 2px 6px;
		font-size: 11px;
		color: var(--text-secondary);
		background: transparent;
	}

	.sidebar-toggle:hover {
		color: var(--text-primary);
		background: transparent;
	}

	.sidebar-reopen {
		width: 28px;
		min-width: 28px;
		background: var(--bg-secondary);
		border-right: 1px solid var(--border-color);
		border-radius: 0;
		color: var(--text-secondary);
		font-size: 10px;
		padding: 0;
		display: flex;
		align-items: center;
		justify-content: center;
	}

	.sidebar-reopen:hover {
		color: var(--text-primary);
		background: var(--bg-tertiary);
	}

	.content {
		flex: 1;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}
</style>
