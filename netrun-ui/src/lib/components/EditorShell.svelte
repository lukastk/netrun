<script lang="ts">
	import { SvelteFlowProvider } from '@xyflow/svelte';
	import Toolbar from './Toolbar.svelte';
	import Breadcrumb from './Breadcrumb.svelte';
	import Sidebar from './Sidebar.svelte';
	import FlowEditor from './FlowEditor.svelte';
	import Modal from './Modal.svelte';
	import FactorySelectorModal from './FactorySelectorModal.svelte';
	import RecipeModal from './RecipeModal.svelte';
	import { extraData } from '$lib/stores/flowStore';
	import { factorySelectorState, closeFactorySelector } from '$lib/stores/factorySelectorStore';
	import { recipeModalState } from '$lib/stores/recipeStore';
	import { handleKeyboardEvent } from '$lib/stores/keyboardStore';

	let {
		showToolbar = true,
		hideFileActions = false,
	}: {
		showToolbar?: boolean;
		hideFileActions?: boolean;
	} = $props();

	function handleGlobalKeydown(event: KeyboardEvent) {
		const target = event.target as HTMLElement;
		const isInput = target instanceof HTMLInputElement ||
			target instanceof HTMLTextAreaElement ||
			target.getAttribute('contenteditable') === 'true';

		if (!isInput || event.metaKey || event.ctrlKey) {
			handleKeyboardEvent(event);
		}
	}
</script>

<svelte:window onkeydown={handleGlobalKeydown} />

{#if showToolbar}
	<Toolbar {hideFileActions} />
{/if}
<Breadcrumb />
<div class="editor-content">
	<div class="canvas-container">
		<SvelteFlowProvider>
			<FlowEditor />
		</SvelteFlowProvider>
	</div>
	<Sidebar />
</div>

<Modal />

{#if $factorySelectorState.isOpen}
	{@const factories = (($extraData as Record<string, unknown>)?.factories as string[]) || []}
	<FactorySelectorModal
		{factories}
		onSelect={(path) => closeFactorySelector(path)}
		onCancel={() => closeFactorySelector(null)}
	/>
{/if}

{#if $recipeModalState.show}
	<RecipeModal
		recipeName={$recipeModalState.recipeName}
		prompts={$recipeModalState.prompts}
		show={$recipeModalState.show}
		onsubmit={(inputs) => $recipeModalState.onSubmit(inputs)}
		oncancel={() => $recipeModalState.onCancel()}
	/>
{/if}

<style>
	.editor-content {
		flex: 1;
		display: flex;
		overflow: hidden;
	}

	.canvas-container {
		flex: 1;
		height: 100%;
		position: relative;
	}
</style>
