<script lang="ts">
	import { modalState, closeModal, cancelModal } from '$lib/stores/modalStore';

	let inputValue = $state('');
	let inputRef = $state<HTMLInputElement | null>(null);

	// Reset input value when modal opens with a new default
	$effect(() => {
		if ($modalState.isOpen && $modalState.type === 'prompt') {
			inputValue = $modalState.defaultValue || '';
			// Focus input after a short delay to ensure it's rendered
			setTimeout(() => inputRef?.focus(), 10);
		}
	});

	function handleConfirm() {
		if ($modalState.type === 'prompt') {
			closeModal(inputValue);
		} else if ($modalState.type === 'confirm') {
			closeModal(true);
		} else {
			closeModal(null);
		}
	}

	function handleCancel() {
		cancelModal();
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.preventDefault();
			handleCancel();
		} else if (event.key === 'Enter' && $modalState.type !== 'prompt') {
			event.preventDefault();
			handleConfirm();
		}
	}

	function handleInputKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter') {
			event.preventDefault();
			handleConfirm();
		} else if (event.key === 'Escape') {
			event.preventDefault();
			handleCancel();
		}
	}

	function handleBackdropClick(event: MouseEvent) {
		if (event.target === event.currentTarget) {
			// Only close on backdrop click for alerts
			if ($modalState.type === 'alert') {
				handleConfirm();
			}
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

{#if $modalState.isOpen}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="modal-backdrop" onclick={handleBackdropClick}>
		<div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
			<button class="close-button" onclick={handleCancel} aria-label="Close">
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
					<path d="M18 6L6 18M6 6l12 12" />
				</svg>
			</button>

			<h2 id="modal-title" class="modal-title">{$modalState.title}</h2>

			{#if $modalState.message}
				<p class="modal-message">{$modalState.message}</p>
			{/if}

			{#if $modalState.type === 'prompt'}
				<div class="input-container">
					<input
						bind:this={inputRef}
						bind:value={inputValue}
						onkeydown={handleInputKeydown}
						type="text"
						class="modal-input"
						class:path={$modalState.inputType === 'path'}
						placeholder={$modalState.placeholder}
					/>
				</div>
			{/if}

			<div class="modal-actions">
				{#if $modalState.type !== 'alert'}
					<button class="btn btn-secondary" onclick={handleCancel}>
						{$modalState.cancelText}
					</button>
				{/if}
				<button class="btn btn-primary" onclick={handleConfirm}>
					{$modalState.confirmText}
				</button>
			</div>
		</div>
	</div>
{/if}

<style>
	.modal-backdrop {
		position: fixed;
		top: 0;
		left: 0;
		right: 0;
		bottom: 0;
		background: rgba(0, 0, 0, 0.6);
		backdrop-filter: blur(4px);
		display: flex;
		align-items: center;
		justify-content: center;
		z-index: 2000;
		animation: fadeIn 0.15s ease-out;
	}

	@keyframes fadeIn {
		from {
			opacity: 0;
		}
		to {
			opacity: 1;
		}
	}

	.modal {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 12px;
		padding: 24px;
		min-width: 400px;
		max-width: 500px;
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
		margin: 0 0 12px 0;
		padding-right: 24px;
	}

	.modal-message {
		font-size: 14px;
		color: var(--text-secondary, #a0a0a0);
		margin: 0 0 20px 0;
		line-height: 1.5;
		white-space: pre-line;
	}

	.input-container {
		margin-bottom: 24px;
	}

	.modal-input {
		width: 100%;
		padding: 12px 14px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		color: var(--text-primary, #fff);
		font-size: 14px;
		outline: none;
		transition: border-color 0.15s ease;
		box-sizing: border-box;
	}

	.modal-input:focus {
		border-color: var(--accent-color, #3b82f6);
	}

	.modal-input::placeholder {
		color: var(--text-secondary, #666);
	}

	.modal-input.path {
		font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
		font-size: 13px;
	}

	.modal-actions {
		display: flex;
		justify-content: flex-end;
		gap: 12px;
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
