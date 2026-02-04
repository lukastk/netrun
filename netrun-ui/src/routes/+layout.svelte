<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import ToastContainer from '$lib/components/ToastContainer.svelte';
	import '../app.css';

	let { children } = $props();
</script>

<svelte:head>
	<title>netrun-ui</title>
	<link rel="icon" href={favicon} />
</svelte:head>

{@render children()}
<ToastContainer />
