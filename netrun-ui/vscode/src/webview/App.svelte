<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import EditorShell from '$lib/components/EditorShell.svelte';
	import ToastContainer from '$lib/components/ToastContainer.svelte';
	import { loadFromFile, isDirty, saveToFile, graphExtra, selectedNodeIds } from '$lib/stores/flowStore';
	import { openCommandPalette } from '$lib/stores/commandStore';
	import { executeAction, type Action } from '$lib/stores/actionsStore';
	import { toasts } from '$lib/stores/toastStore';
	import { initializeCommands } from '$lib/commands';
	import { loadConfigSchema } from '$lib/stores/schemaStore';
	import { loadSignalTypes } from '$lib/stores/signalStore';
	import { loadControlTypes } from '$lib/stores/controlStore';
	import { get } from 'svelte/store';
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

	function handleVsCodeCommandPalette() {
		openCommandPalette();
	}

	function handleNodeDblClick(e: Event) {
		const { id, data, metaKey } = (e as CustomEvent).detail;
		const config = data._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;

		// Check for node-level 'open' action
		const nodeUi = extra?.ui as Record<string, unknown> | undefined;
		const nodeActions = (nodeUi?.actions as Action[]) || [];
		const nodeOpenAction = nodeActions.find((a: Action) => a.label === 'open');

		// Check project-level 'open' action
		const gExtra = get(graphExtra) as Record<string, unknown> | undefined;
		const gUi = gExtra?.ui as Record<string, unknown> | undefined;
		const projectActions = (gUi?.actions as Action[]) || [];
		const projectOpenAction = projectActions.find((a: Action) => a.label === 'open');

		// Node action takes precedence over project action
		const openAction = nodeOpenAction || projectOpenAction;

		if (openAction) {
			// Ensure node is selected so executeAction has the right context
			selectedNodeIds.set(new Set([id]));
			executeAction(openAction);
		} else {
			// Fall back to opening source_path in VS Code
			const sourcePath = extra?.source_path as string | undefined;
			if (sourcePath) {
				vscode.postMessage({ type: 'openFile', filePath: sourcePath, beside: metaKey });
			} else {
				toasts.info(`No 'open' action or extra.source_path defined on node "${data.label}"`);
			}
		}
	}

	function handleNodeOpen(e: Event) {
		const { id, data } = (e as CustomEvent).detail;
		const config = data._config as Record<string, unknown> | undefined;
		const extra = config?.extra as Record<string, unknown> | undefined;

		// Check for node-level 'open' action
		const nodeUi = extra?.ui as Record<string, unknown> | undefined;
		const nodeActions = (nodeUi?.actions as Action[]) || [];
		const nodeOpenAction = nodeActions.find((a: Action) => a.label === 'open');

		// Check project-level 'open' action
		const gExtra = get(graphExtra) as Record<string, unknown> | undefined;
		const gUi = gExtra?.ui as Record<string, unknown> | undefined;
		const projectActions = (gUi?.actions as Action[]) || [];
		const projectOpenAction = projectActions.find((a: Action) => a.label === 'open');

		const openAction = nodeOpenAction || projectOpenAction;

		if (openAction) {
			selectedNodeIds.set(new Set([id]));
			executeAction(openAction);
		} else {
			// Fall back to opening source_path in VS Code
			const sourcePath = extra?.source_path as string | undefined;
			if (sourcePath) {
				vscode.postMessage({ type: 'openFile', filePath: sourcePath });
			} else {
				toasts.info(`No 'open' action or extra.source_path defined on node "${data.label}"`);
			}
		}
	}

	onMount(async () => {
		window.addEventListener('netrun-vscode-save', handleVsCodeSave);
		window.addEventListener('netrun-vscode-command-palette', handleVsCodeCommandPalette);
		window.addEventListener('netrun-node-dblclick', handleNodeDblClick);
		window.addEventListener('netrun-node-open', handleNodeOpen);

		initializeCommands({ embedded: true });

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
			toasts.error(`Failed to load file: ${e}`);
		}
	});

	onDestroy(() => {
		window.removeEventListener('netrun-vscode-save', handleVsCodeSave);
		window.removeEventListener('netrun-vscode-command-palette', handleVsCodeCommandPalette);
		window.removeEventListener('netrun-node-dblclick', handleNodeDblClick);
		window.removeEventListener('netrun-node-open', handleNodeOpen);
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
