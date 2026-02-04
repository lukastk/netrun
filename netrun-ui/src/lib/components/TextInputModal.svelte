<script lang="ts">
	interface Props {
		title: string;
		label?: string;
		placeholder?: string;
		initialValue?: string;
		submitLabel?: string;
		onSubmit: (value: string) => void;
		onCancel: () => void;
	}

	let {
		title,
		label,
		placeholder = '',
		initialValue = '',
		submitLabel = 'OK',
		onSubmit,
		onCancel,
	}: Props = $props();

	let value = $state(initialValue);
	let inputElement: HTMLInputElement | undefined = $state();

	// Focus input on mount
	$effect(() => {
		if (inputElement) {
			inputElement.focus();
			inputElement.select();
		}
	});

	function handleSubmit() {
		if (value.trim()) {
			onSubmit(value.trim());
		}
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.preventDefault();
			onCancel();
		} else if (event.key === 'Enter') {
			event.preventDefault();
			handleSubmit();
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="modal-backdrop" onclick={onCancel} onkeydown={() => {}} role="presentation">
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="modal" onclick={(e) => e.stopPropagation()}>
		<div class="modal-header">
			<h2>{title}</h2>
			<button class="close-btn" onclick={onCancel}>×</button>
		</div>

		<div class="modal-body">
			<div class="field">
				{#if label}
					<label for="text-input">{label}</label>
				{/if}
				<input
					id="text-input"
					type="text"
					bind:value
					bind:this={inputElement}
					placeholder={placeholder}
				/>
			</div>
		</div>

		<div class="modal-footer">
			<button class="btn btn-secondary" onclick={onCancel}>
				Cancel
			</button>
			<button
				class="btn btn-primary"
				onclick={handleSubmit}
				disabled={!value.trim()}
			>
				{submitLabel}
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
		width: 400px;
		max-width: 90vw;
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
	}

	.field {
		display: flex;
		flex-direction: column;
		gap: 6px;
	}

	.field label {
		font-size: 12px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
	}

	.field input {
		width: 100%;
		padding: 10px 12px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		color: var(--text-primary, #fff);
		font-size: 14px;
		box-sizing: border-box;
	}

	.field input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.modal-footer {
		display: flex;
		justify-content: flex-end;
		gap: 8px;
		padding: 16px 20px;
		border-top: 1px solid var(--border-color, #404040);
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
</style>
