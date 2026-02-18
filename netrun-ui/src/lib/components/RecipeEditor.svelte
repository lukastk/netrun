<script lang="ts">
	import type { RecipeDefinition } from '$lib/stores/recipeStore';

	interface Props {
		recipeName: string | null;
		recipeDefinition: RecipeDefinition | null;
		existingNames: string[];
		onSave: (name: string, definition: RecipeDefinition) => void;
		onCancel: () => void;
		onDelete?: () => void;
	}

	let { recipeName = null, recipeDefinition = null, existingNames, onSave, onCancel, onDelete }: Props = $props();

	// Form state
	let name = $state(recipeName || '');
	let path = $state(recipeDefinition?.path || '');
	let description = $state(recipeDefinition?.description || '');

	// Validation
	const NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

	let nameError = $derived.by(() => {
		const trimmed = name.trim();
		if (!trimmed) return '';
		if (!NAME_PATTERN.test(trimmed)) return 'Must start with a letter or underscore, and contain only letters, digits, or underscores';
		if (trimmed !== recipeName && existingNames.includes(trimmed)) return 'A recipe with this name already exists';
		return '';
	});

	let canSave = $derived(name.trim() !== '' && path.trim() !== '' && !nameError);

	function handleSave() {
		if (!canSave) return;
		const def: RecipeDefinition = { path: path.trim() };
		if (description.trim()) def.description = description.trim();
		onSave(name.trim(), def);
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.preventDefault();
			onCancel();
		} else if (event.key === 'Enter' && event.metaKey) {
			event.preventDefault();
			handleSave();
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="modal-backdrop" role="presentation">
	<div class="modal">
		<div class="modal-header">
			<h2>{recipeName ? 'Edit Recipe' : 'Add Recipe'}</h2>
			<button class="close-btn" onclick={onCancel}>&times;</button>
		</div>

		<div class="modal-body">
			<div class="field">
				<label for="recipe-name">Name</label>
				<input
					id="recipe-name"
					type="text"
					bind:value={name}
					placeholder="my_recipe"
				/>
				{#if nameError}
					<div class="field-error">{nameError}</div>
				{/if}
			</div>

			<div class="field">
				<label for="recipe-path">Path</label>
				<input
					id="recipe-path"
					type="text"
					bind:value={path}
					placeholder="./recipes/my_recipe.py"
				/>
				<div class="field-hint">Relative to project root</div>
			</div>

			<div class="field">
				<label for="recipe-description">Description <span class="optional">(optional)</span></label>
				<input
					id="recipe-description"
					type="text"
					bind:value={description}
					placeholder="What this recipe does"
				/>
			</div>
		</div>

		<div class="modal-footer">
			{#if recipeName && onDelete}
				<button class="btn btn-danger" onclick={onDelete}>
					Delete
				</button>
			{/if}
			<div class="spacer"></div>
			<button class="btn btn-secondary" onclick={onCancel}>
				Cancel
			</button>
			<button
				class="btn btn-primary"
				onclick={handleSave}
				disabled={!canSave}
			>
				{recipeName ? 'Save' : 'Add'}
			</button>
		</div>
	</div>
</div>

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
	}

	.modal {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 12px;
		width: 500px;
		max-width: 90vw;
		max-height: 90vh;
		display: flex;
		flex-direction: column;
		box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
	}

	.modal-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 16px 20px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.modal-header h2 {
		margin: 0;
		font-size: 16px;
		font-weight: 600;
		color: var(--text-primary, #fff);
	}

	.close-btn {
		background: none;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		font-size: 20px;
		cursor: pointer;
		padding: 4px 8px;
		line-height: 1;
		border-radius: 4px;
	}

	.close-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.modal-body {
		padding: 20px;
		overflow-y: auto;
	}

	.field {
		margin-bottom: 16px;
	}

	.field label {
		display: block;
		font-size: 12px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 6px;
	}

	.field .optional {
		font-weight: 400;
		color: var(--text-secondary, #666);
	}

	.field input {
		width: 100%;
		padding: 10px 12px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		color: var(--text-primary, #fff);
		font-size: 13px;
		font-family: inherit;
		box-sizing: border-box;
	}

	.field input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field-hint {
		font-size: 11px;
		color: var(--text-secondary, #666);
		margin-top: 6px;
	}

	.field-error {
		font-size: 11px;
		color: var(--error-color, #ef4444);
		margin-top: 6px;
	}

	.modal-footer {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 16px 20px;
		border-top: 1px solid var(--border-color, #404040);
	}

	.spacer {
		flex: 1;
	}

	.btn {
		padding: 8px 16px;
		border-radius: 6px;
		font-size: 13px;
		font-weight: 500;
		cursor: pointer;
		border: none;
		transition: all 0.15s ease;
	}

	.btn:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.btn-primary {
		background: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.btn-primary:hover:not(:disabled) {
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

	.btn-danger {
		background: transparent;
		color: var(--error-color, #ef4444);
		border: 1px solid var(--error-color, #ef4444);
	}

	.btn-danger:hover {
		background: var(--error-color, #ef4444);
		color: #fff;
	}
</style>
