<script lang="ts">
	import { resolveCommand, type Action } from '$lib/stores/actionsStore';

	interface Props {
		action?: Action | null;
		onSave: (action: Action) => void;
		onCancel: () => void;
		onDelete?: () => void;
	}

	let { action = null, onSave, onCancel, onDelete }: Props = $props();

	// Form state
	let label = $state(action?.label || '');
	let command = $state(action?.command || '');
	let previewCommand = $state('');
	let previewLoading = $state(false);

	// Update preview when command changes
	let previewTimeout: ReturnType<typeof setTimeout> | null = null;

	$effect(() => {
		if (previewTimeout) {
			clearTimeout(previewTimeout);
		}

		if (command.trim()) {
			previewLoading = true;
			previewTimeout = setTimeout(async () => {
				previewCommand = await resolveCommand(command);
				previewLoading = false;
			}, 300);
		} else {
			previewCommand = '';
			previewLoading = false;
		}
	});

	function handleSave() {
		if (!label.trim() || !command.trim()) return;

		onSave({
			id: action?.id || `action-${Date.now()}`,
			label: label.trim(),
			command: command.trim(),
		});
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

<div class="modal-backdrop" onclick={onCancel} onkeydown={() => {}} role="presentation">
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="modal" onclick={(e) => e.stopPropagation()}>
		<div class="modal-header">
			<h2>{action ? 'Edit Action' : 'Add Action'}</h2>
			<button class="close-btn" onclick={onCancel}>×</button>
		</div>

		<div class="modal-body">
			<div class="field">
				<label for="action-label">Label</label>
				<input
					id="action-label"
					type="text"
					bind:value={label}
					placeholder="Open in VS Code"
				/>
			</div>

			<div class="field">
				<label for="action-command">Command</label>
				<textarea
					id="action-command"
					bind:value={command}
					placeholder="$DEFAULT_CMD $PROJECT_ROOT/src/$NODE_NAME.py"
					rows="3"
				></textarea>
				<div class="field-hint">
					Use variables: <code>$NODE_NAME</code>, <code>$PROJECT_ROOT</code>,
					<code>$NET_FILE_PATH</code>, <code>$NODE_CONFIG</code>, <code>$DEFAULT_CMD</code>, or custom env vars
				</div>
			</div>

			{#if command.trim()}
				<div class="preview">
					<div class="preview-label">Preview (for selected node):</div>
					<div class="preview-command">
						{#if previewLoading}
							<span class="loading">Resolving...</span>
						{:else}
							<code>{previewCommand || command}</code>
						{/if}
					</div>
				</div>
			{/if}
		</div>

		<div class="modal-footer">
			{#if action && onDelete}
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
				disabled={!label.trim() || !command.trim()}
			>
				{action ? 'Save' : 'Add'}
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

	.field input,
	.field textarea {
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

	.field textarea {
		font-family: 'SF Mono', Monaco, monospace;
		resize: vertical;
		min-height: 80px;
	}

	.field input:focus,
	.field textarea:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field-hint {
		font-size: 11px;
		color: var(--text-secondary, #666);
		margin-top: 6px;
	}

	.field-hint code {
		background: var(--bg-tertiary, #2d2d2d);
		padding: 1px 4px;
		border-radius: 3px;
		font-size: 10px;
	}

	.preview {
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		padding: 12px;
	}

	.preview-label {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 8px;
	}

	.preview-command {
		font-family: 'SF Mono', Monaco, monospace;
		font-size: 12px;
		color: var(--text-primary, #fff);
		word-break: break-all;
	}

	.preview-command code {
		display: block;
		white-space: pre-wrap;
	}

	.preview-command .loading {
		color: var(--text-secondary, #666);
		font-style: italic;
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
