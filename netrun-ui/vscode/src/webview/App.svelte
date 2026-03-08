<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import EditorShell from '$lib/components/EditorShell.svelte';
	import ToastContainer from '$lib/components/ToastContainer.svelte';
	import { loadFromFile, isDirty, saveToFile } from '$lib/stores/flowStore';
	import { loadConfigSchema } from '$lib/stores/schemaStore';
	import { loadSignalTypes } from '$lib/stores/signalStore';
	import { loadControlTypes } from '$lib/stores/controlStore';
	import '$lib/configFieldRegistrations';

	interface VsCodeApi {
		postMessage(message: unknown): void;
		getState(): unknown;
		setState(state: unknown): void;
	}

	let { filePath, vscode }: { filePath: string; vscode: VsCodeApi } = $props();

	// Track dirty state and notify extension host
	$effect(() => {
		vscode.postMessage({ type: 'dirty', isDirty: $isDirty });
	});

	// Listen for save command from extension host (Cmd+S)
	function handleVsCodeSave() {
		saveToFile().then(() => {
			vscode.postMessage({ type: 'saved' });
		}).catch((e) => {
			console.error('Save failed:', e);
		});
	}

	onMount(async () => {
		window.addEventListener('netrun-vscode-save', handleVsCodeSave);

		// Initialize schema and config
		await Promise.all([
			loadConfigSchema(),
			loadSignalTypes(),
			loadControlTypes(),
		]);

		// Load the file
		try {
			await loadFromFile(filePath);
		} catch (e) {
			console.error('Failed to load file:', e);
		}
	});

	onDestroy(() => {
		window.removeEventListener('netrun-vscode-save', handleVsCodeSave);
	});

	// macOS Smart Quotes fix (same as +layout.svelte)
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

<div class="app">
	<EditorShell hideFileActions />
</div>
<ToastContainer />

<style>
	.app {
		height: 100vh;
		width: 100vw;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}
</style>
