<script lang="ts">
	import '../app.css';
	import 'netrun-ui-vis/theme.css';
	import '@xyflow/svelte/dist/style.css';
	import NetList from '$lib/components/NetList.svelte';
	import { startPolling, stopPolling } from '$lib/stores/registryStore.svelte.js';
	import { onMount } from 'svelte';

	let { children } = $props();

	onMount(() => {
		startPolling();
		return () => stopPolling();
	});
</script>

<svelte:head>
	<title>netrun dashboard</title>
</svelte:head>

<div class="layout">
	<aside class="sidebar">
		<NetList />
	</aside>
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
		width: var(--sidebar-width);
		min-width: var(--sidebar-width);
		background: var(--bg-secondary);
		border-right: 1px solid var(--border-color);
		overflow: hidden;
	}

	.content {
		flex: 1;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}
</style>
