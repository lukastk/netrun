<script lang="ts">
	import { toasts } from '$lib/stores/toastStore';

	const iconMap = {
		error: '✕',
		success: '✓',
		warning: '⚠',
		info: 'ℹ',
	};
</script>

<div class="toast-container">
	{#each $toasts as toast (toast.id)}
		<div class="toast toast-{toast.type}">
			<span class="toast-icon">{iconMap[toast.type]}</span>
			<span class="toast-message">{toast.message}</span>
			<button class="toast-close" onclick={() => toasts.remove(toast.id)}>×</button>
		</div>
	{/each}
</div>

<style>
	.toast-container {
		position: fixed;
		bottom: 20px;
		right: 20px;
		z-index: 9999;
		display: flex;
		flex-direction: column;
		gap: 8px;
		max-width: 400px;
	}

	.toast {
		display: flex;
		align-items: center;
		gap: 10px;
		padding: 12px 16px;
		border-radius: 8px;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
		animation: slideIn 0.2s ease-out;
	}

	@keyframes slideIn {
		from {
			transform: translateX(100%);
			opacity: 0;
		}
		to {
			transform: translateX(0);
			opacity: 1;
		}
	}

	.toast-icon {
		width: 20px;
		height: 20px;
		border-radius: 50%;
		display: flex;
		align-items: center;
		justify-content: center;
		font-size: 12px;
		font-weight: bold;
		flex-shrink: 0;
	}

	.toast-error .toast-icon {
		background: #ef4444;
		color: white;
	}

	.toast-success .toast-icon {
		background: #22c55e;
		color: white;
	}

	.toast-warning .toast-icon {
		background: #f59e0b;
		color: white;
	}

	.toast-info .toast-icon {
		background: #3b82f6;
		color: white;
	}

	.toast-message {
		flex: 1;
		font-size: 13px;
		color: var(--text-primary, #fff);
		line-height: 1.4;
	}

	.toast-close {
		background: none;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		font-size: 18px;
		cursor: pointer;
		padding: 0 4px;
		line-height: 1;
		flex-shrink: 0;
	}

	.toast-close:hover {
		color: var(--text-primary, #fff);
	}

	.toast-error {
		border-color: rgba(239, 68, 68, 0.3);
	}

	.toast-success {
		border-color: rgba(34, 197, 94, 0.3);
	}

	.toast-warning {
		border-color: rgba(245, 158, 11, 0.3);
	}

	.toast-info {
		border-color: rgba(59, 130, 246, 0.3);
	}
</style>
