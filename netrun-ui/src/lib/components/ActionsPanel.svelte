<script lang="ts">
	import {
		availableActions,
		projectActions,
		actionExecutions,
		executeAction,
		clearActionExecution,
		type Action,
	} from '$lib/stores/actionsStore';
	import { selectedNode } from '$lib/stores/flowStore';

	// Props for opening settings/editor
	interface Props {
		onOpenSettings?: () => void;
		onAddAction?: () => void;
		onEditAction?: (action: Action) => void;
	}

	let { onOpenSettings, onAddAction, onEditAction }: Props = $props();

	// Handle action click
	async function handleActionClick(action: Action) {
		await executeAction(action);
	}

	// Get execution state for an action
	function getExecution(actionId: string) {
		return $actionExecutions.get(actionId);
	}

	// Check if action is a default (project-level) action
	function isDefaultAction(actionId: string): boolean {
		return $projectActions.some(a => a.id === actionId);
	}

	// Clear execution state when selected node changes
	$effect(() => {
		$selectedNode; // track dependency
		actionExecutions.set(new Map());
	});
</script>

{#if $selectedNode}
	<div class="actions-panel">
		<div class="panel-header">
			<span class="panel-title">Actions</span>
			<div class="header-buttons">
				{#if onOpenSettings}
					<button
						class="icon-btn"
						onclick={onOpenSettings}
						title="Project Settings"
					>
						<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
							<circle cx="12" cy="12" r="3"/>
							<path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>
						</svg>
					</button>
				{/if}
			</div>
		</div>

		<div class="actions-list">
			{#if $availableActions.length === 0}
				<div class="empty-message">
					No actions defined.
					{#if onOpenSettings}
						<button class="link-btn" onclick={onOpenSettings}>
							Configure in settings
						</button>
					{/if}
				</div>
			{:else}
				{#each $availableActions as action (action.id)}
					{@const execution = getExecution(action.id)}
					<div class="action-item" class:running={execution?.status === 'running'} class:error={execution?.status === 'error'}>
						<button
							class="action-btn"
							onclick={() => handleActionClick(action)}
							disabled={execution?.status === 'running'}
							title={action.command}
						>
							<span class="action-icon">
								{#if execution?.status === 'running'}
									<span class="spinner"></span>
								{:else if execution?.status === 'error'}
									<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<circle cx="12" cy="12" r="10"/>
										<line x1="15" y1="9" x2="9" y2="15"/>
										<line x1="9" y1="9" x2="15" y2="15"/>
									</svg>
								{:else if execution?.status === 'success'}
									<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<polyline points="20 6 9 17 4 12"/>
									</svg>
								{:else}
									<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<polygon points="5 3 19 12 5 21 5 3"/>
									</svg>
								{/if}
							</span>
							<span class="action-label">{action.label}</span>
							{#if isDefaultAction(action.id)}
								<span class="default-badge">default</span>
							{/if}
						</button>

						{#if onEditAction}
							<button
								class="edit-btn"
								onclick={() => onEditAction(action)}
								title="Edit action"
							>
								<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
									<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
									<path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
								</svg>
							</button>
						{/if}
					</div>

					{#if execution?.status === 'error' && execution.stderr}
						<div class="error-output">
							<div class="error-header">
								<span>Error</span>
								<button class="close-btn" onclick={() => clearActionExecution(action.id)}>×</button>
							</div>
							<pre>{execution.stderr}</pre>
						</div>
					{/if}
				{/each}
			{/if}
		</div>

		{#if onAddAction}
			<button class="add-action-btn" onclick={onAddAction}>
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
					<line x1="12" y1="5" x2="12" y2="19"/>
					<line x1="5" y1="12" x2="19" y2="12"/>
				</svg>
				Add Action
			</button>
		{/if}
	</div>
{/if}

<style>
	.actions-panel {
		border-top: 1px solid var(--border-color, #404040);
		padding: 12px;
	}

	.panel-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		margin-bottom: 12px;
	}

	.panel-title {
		font-size: 11px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.5px;
		color: var(--text-secondary, #a0a0a0);
	}

	.header-buttons {
		display: flex;
		gap: 4px;
	}

	.icon-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 24px;
		height: 24px;
		padding: 0;
		background: transparent;
		border: none;
		border-radius: 4px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.icon-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.actions-list {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.empty-message {
		font-size: 12px;
		color: var(--text-secondary, #666);
		text-align: center;
		padding: 16px 8px;
	}

	.link-btn {
		background: none;
		border: none;
		color: var(--accent-color, #3b82f6);
		font-size: 12px;
		cursor: pointer;
		text-decoration: underline;
		padding: 0;
		margin-top: 4px;
		display: block;
	}

	.link-btn:hover {
		color: var(--accent-hover, #2563eb);
	}

	.action-item {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.action-item.running .action-btn {
		opacity: 0.7;
	}

	.action-item.error .action-btn {
		border-color: var(--error-color, #ef4444);
	}

	.action-btn {
		flex: 1;
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 8px 10px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid transparent;
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
		transition: all 0.15s ease;
		text-align: left;
	}

	.action-btn:hover:not(:disabled) {
		background: var(--border-color, #404040);
	}

	.action-btn:disabled {
		cursor: not-allowed;
	}

	.action-icon {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 16px;
		height: 16px;
		color: var(--accent-color, #3b82f6);
	}

	.action-item.error .action-icon {
		color: var(--error-color, #ef4444);
	}

	.spinner {
		width: 12px;
		height: 12px;
		border: 2px solid var(--border-color, #404040);
		border-top-color: var(--accent-color, #3b82f6);
		border-radius: 50%;
		animation: spin 0.8s linear infinite;
	}

	@keyframes spin {
		to {
			transform: rotate(360deg);
		}
	}

	.action-label {
		flex: 1;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.default-badge {
		flex-shrink: 0;
		font-size: 9px;
		font-weight: 500;
		text-transform: uppercase;
		letter-spacing: 0.3px;
		color: var(--text-secondary, #a0a0a0);
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		padding: 1px 5px;
		line-height: 1.4;
	}

	.edit-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 24px;
		height: 24px;
		padding: 0;
		background: transparent;
		border: none;
		border-radius: 4px;
		color: var(--text-secondary, #666);
		cursor: pointer;
		opacity: 0;
		transition: all 0.15s ease;
	}

	.action-item:hover .edit-btn {
		opacity: 1;
	}

	.edit-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.error-output {
		margin: 4px 0 8px 0;
		background: rgba(239, 68, 68, 0.1);
		border: 1px solid var(--error-color, #ef4444);
		border-radius: 4px;
		overflow: hidden;
	}

	.error-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 4px 8px;
		background: rgba(239, 68, 68, 0.2);
		font-size: 11px;
		font-weight: 500;
		color: var(--error-color, #ef4444);
	}

	.close-btn {
		background: none;
		border: none;
		color: var(--error-color, #ef4444);
		font-size: 14px;
		cursor: pointer;
		padding: 0 4px;
		line-height: 1;
	}

	.error-output pre {
		margin: 0;
		padding: 8px;
		font-size: 11px;
		font-family: 'SF Mono', Monaco, monospace;
		color: var(--text-secondary, #a0a0a0);
		white-space: pre-wrap;
		word-break: break-all;
		max-height: 100px;
		overflow-y: auto;
	}

	.add-action-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		gap: 6px;
		width: 100%;
		padding: 8px;
		margin-top: 8px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.add-action-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}
</style>
