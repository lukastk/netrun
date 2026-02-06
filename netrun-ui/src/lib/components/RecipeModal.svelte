<script lang="ts">
	import type { RecipePrompt } from '$lib/stores/recipeStore';

	interface Props {
		recipeName: string;
		prompts: RecipePrompt[];
		show?: boolean;
		onsubmit: (inputs: Record<string, unknown>) => void;
		oncancel: () => void;
	}

	let { recipeName, prompts, show = false, onsubmit, oncancel }: Props = $props();

	// Initialize inputs with defaults
	let inputs = $state<Record<string, unknown>>({});

	// Reset inputs when prompts change
	$effect(() => {
		const newInputs: Record<string, unknown> = {};
		for (const prompt of prompts) {
			newInputs[prompt.name] = prompt.default ?? getDefaultForType(prompt.type);
		}
		inputs = newInputs;
	});

	function getDefaultForType(type: string): unknown {
		switch (type) {
			case 'number': return 0;
			case 'checkbox': return false;
			case 'select': return '';
			default: return '';
		}
	}

	function handleSubmit() {
		onsubmit(inputs);
	}

	function handleCancel() {
		oncancel();
	}

	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'Escape') {
			e.preventDefault();
			handleCancel();
		}
	}

	function handleInputKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter' && !e.shiftKey) {
			e.preventDefault();
			handleSubmit();
		}
	}

	function handleBackdropClick(e: MouseEvent) {
		if (e.target === e.currentTarget) {
			handleCancel();
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

{#if show}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="modal-backdrop" onclick={handleBackdropClick}>
		<div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
			<button class="close-button" onclick={handleCancel} aria-label="Close">
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
					<path d="M18 6L6 18M6 6l12 12" />
				</svg>
			</button>

			<h2 id="modal-title" class="modal-title">Recipe: {recipeName}</h2>

			<form onsubmit={(e) => { e.preventDefault(); handleSubmit(); }}>
				{#each prompts as prompt}
					<div class="field">
						<label for={prompt.name}>{prompt.label}</label>

						{#if prompt.type === 'text'}
							<input
								type="text"
								id={prompt.name}
								bind:value={inputs[prompt.name]}
								onkeydown={handleInputKeydown}
							/>
						{:else if prompt.type === 'number'}
							<input
								type="number"
								id={prompt.name}
								bind:value={inputs[prompt.name]}
								onkeydown={handleInputKeydown}
							/>
						{:else if prompt.type === 'select'}
							<select id={prompt.name} bind:value={inputs[prompt.name]}>
								{#each prompt.options ?? [] as option}
									<option value={option}>{option}</option>
								{/each}
							</select>
						{:else if prompt.type === 'checkbox'}
							<input
								type="checkbox"
								id={prompt.name}
								checked={inputs[prompt.name] as boolean}
								onchange={(e) => inputs[prompt.name] = (e.target as HTMLInputElement).checked}
							/>
						{/if}
					</div>
				{/each}

				<div class="actions">
					<button type="button" class="btn btn-secondary" onclick={handleCancel}>Cancel</button>
					<button type="submit" class="btn btn-primary">Run Recipe</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<style>
	.modal-backdrop {
		position: fixed;
		inset: 0;
		background: rgba(0, 0, 0, 0.6);
		backdrop-filter: blur(4px);
		display: flex;
		align-items: center;
		justify-content: center;
		z-index: 2000;
		animation: fadeIn 0.15s ease-out;
	}

	@keyframes fadeIn {
		from { opacity: 0; }
		to { opacity: 1; }
	}

	.modal {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 12px;
		padding: 24px;
		min-width: 400px;
		max-width: 600px;
		position: relative;
		box-shadow:
			0 20px 60px rgba(0, 0, 0, 0.5),
			0 8px 24px rgba(0, 0, 0, 0.3);
		animation: slideIn 0.15s ease-out;
	}

	@keyframes slideIn {
		from {
			opacity: 0;
			transform: scale(0.95) translateY(-10px);
		}
		to {
			opacity: 1;
			transform: scale(1) translateY(0);
		}
	}

	.close-button {
		position: absolute;
		top: 16px;
		right: 16px;
		background: transparent;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		padding: 4px;
		border-radius: 4px;
		display: flex;
		align-items: center;
		justify-content: center;
		transition: all 0.15s ease;
	}

	.close-button:hover {
		color: var(--text-primary, #fff);
		background: var(--bg-tertiary, #2d2d2d);
	}

	.modal-title {
		font-size: 18px;
		font-weight: 600;
		color: var(--text-primary, #fff);
		margin: 0 0 20px 0;
		padding-right: 24px;
	}

	.field {
		margin-bottom: 16px;
	}

	label {
		display: block;
		margin-bottom: 6px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 13px;
		font-weight: 500;
	}

	input[type="text"],
	input[type="number"],
	select {
		width: 100%;
		padding: 10px 12px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		color: var(--text-primary, #fff);
		font-size: 14px;
		outline: none;
		transition: border-color 0.15s ease;
		box-sizing: border-box;
	}

	input[type="text"]:focus,
	input[type="number"]:focus,
	select:focus {
		border-color: var(--accent-color, #3b82f6);
	}

	input[type="checkbox"] {
		width: 18px;
		height: 18px;
		cursor: pointer;
	}

	.actions {
		display: flex;
		justify-content: flex-end;
		gap: 12px;
		margin-top: 24px;
	}

	.btn {
		padding: 10px 20px;
		border-radius: 6px;
		font-size: 14px;
		font-weight: 500;
		cursor: pointer;
		transition: all 0.15s ease;
		border: none;
	}

	.btn-primary {
		background: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.btn-primary:hover {
		background: var(--accent-hover, #2563eb);
	}

	.btn-secondary {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
		border: 1px solid var(--border-color, #404040);
	}

	.btn-secondary:hover {
		background: var(--border-color, #404040);
	}
</style>
