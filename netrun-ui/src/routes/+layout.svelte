<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import ToastContainer from '$lib/components/ToastContainer.svelte';
	import '../app.css';

	let { children } = $props();

	// macOS system-level "Smart Quotes" replaces straight quotes with curly
	// ones in text inputs. There's no native HTML attribute to disable this,
	// so we intercept and fix on input.
	const SMART_QUOTES = /[\u201C\u201D\u201E\u201F]/g;
	const SMART_APOSTROPHES = /[\u2018\u2019\u201A\u201B]/g;

	function fixSmartQuotes(e: Event) {
		const el = e.target;
		if (!(el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) return;
		const v = el.value;
		const fixed = v.replace(SMART_QUOTES, '"').replace(SMART_APOSTROPHES, "'");
		if (fixed !== v) {
			const pos = el.selectionStart;
			el.value = fixed;
			if (pos !== null) el.selectionStart = el.selectionEnd = pos;
			el.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText' }));
		}
	}
</script>

<svelte:document oninput={fixSmartQuotes} />

<svelte:head>
	<title>netrun-ui v{__APP_VERSION__}</title>
	<link rel="icon" href={favicon} />
</svelte:head>

{@render children()}
<ToastContainer />
