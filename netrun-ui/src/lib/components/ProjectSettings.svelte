<script lang="ts">
	import {
		actionSettings,
		updateActionSettings,
		addProjectAction,
		updateProjectAction,
		removeProjectAction,
		generateActionId,
		type Action,
	} from '$lib/stores/actionsStore';
	import ActionEditor from './ActionEditor.svelte';

	interface Props {
		onClose: () => void;
	}

	let { onClose }: Props = $props();

	// Local state bound to settings
	let projectRoot = $state($actionSettings.projectRoot || '');
	let defaultCmd = $state($actionSettings.defaultCmd || '');

	// Environment variables as array for editing
	let envVars = $state<Array<{ key: string; value: string }>>(
		Object.entries($actionSettings.env || {}).map(([key, value]) => ({ key, value }))
	);

	// Action editor state
	let showActionEditor = $state(false);
	let editingAction = $state<Action | null>(null);

	// Save settings on change
	function saveSettings() {
		const env: Record<string, string> = {};
		for (const { key, value } of envVars) {
			if (key.trim()) {
				env[key.trim()] = value;
			}
		}

		updateActionSettings({
			projectRoot: projectRoot.trim() || undefined,
			defaultCmd: defaultCmd.trim() || undefined,
			env: Object.keys(env).length > 0 ? env : undefined,
		});
	}

	// Add new env var row
	function addEnvVar() {
		envVars = [...envVars, { key: '', value: '' }];
	}

	// Remove env var
	function removeEnvVar(index: number) {
		envVars = envVars.filter((_, i) => i !== index);
		saveSettings();
	}

	// Handle action edit
	function handleEditAction(action: Action) {
		editingAction = action;
		showActionEditor = true;
	}

	// Handle action save
	function handleSaveAction(action: Action) {
		if (editingAction) {
			updateProjectAction(action.id, action);
		} else {
			addProjectAction({ ...action, id: generateActionId() });
		}
		showActionEditor = false;
		editingAction = null;
	}

	// Handle action delete
	function handleDeleteAction() {
		if (editingAction) {
			removeProjectAction(editingAction.id);
		}
		showActionEditor = false;
		editingAction = null;
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape' && !showActionEditor) {
			event.preventDefault();
			onClose();
		}
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="modal-backdrop" onclick={onClose} onkeydown={() => {}} role="presentation">
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="modal" onclick={(e) => e.stopPropagation()}>
		<div class="modal-header">
			<h2>Project Settings</h2>
			<button class="close-btn" onclick={onClose}>×</button>
		</div>

		<div class="modal-body">
			<section class="section">
				<h3>Paths</h3>

				<div class="field">
					<label for="project-root">Project Root</label>
					<input
						id="project-root"
						type="text"
						bind:value={projectRoot}
						onblur={saveSettings}
						placeholder="./ (relative to net file)"
					/>
					<div class="field-hint">
						Base path for file references. Relative paths are resolved from the net file location.
					</div>
				</div>

				<div class="field">
					<label for="default-cmd">Default Command</label>
					<input
						id="default-cmd"
						type="text"
						bind:value={defaultCmd}
						onblur={saveSettings}
						placeholder="code"
					/>
					<div class="field-hint">
						Default command for <code>$DEFAULT_CMD</code> variable (e.g., <code>code</code>, <code>vim</code>, <code>open</code>)
					</div>
				</div>
			</section>

			<section class="section">
				<div class="section-header">
					<h3>Environment Variables</h3>
					<button class="add-btn" onclick={addEnvVar}>+ Add</button>
				</div>

				<div class="env-list">
					{#if envVars.length === 0}
						<div class="empty-message">
							No custom variables defined. Click "Add" to create one.
						</div>
					{:else}
						{#each envVars as envVar, index (index)}
							<div class="env-row">
								<input
									type="text"
									bind:value={envVar.key}
									onblur={saveSettings}
									placeholder="VAR_NAME"
									class="env-key"
								/>
								<span class="env-equals">=</span>
								<input
									type="text"
									bind:value={envVar.value}
									onblur={saveSettings}
									placeholder="value"
									class="env-value"
								/>
								<button
									class="remove-btn"
									onclick={() => removeEnvVar(index)}
									title="Remove"
								>
									×
								</button>
							</div>
						{/each}
					{/if}
				</div>

				<div class="field-hint">
					Custom variables can be used in action commands as <code>$VAR_NAME</code>
				</div>
			</section>

			<section class="section">
				<div class="section-header">
					<h3>Default Actions</h3>
					<button class="add-btn" onclick={() => { editingAction = null; showActionEditor = true; }}>
						+ Add
					</button>
				</div>

				<div class="actions-list">
					{#if ($actionSettings.actions || []).length === 0}
						<div class="empty-message">
							No default actions defined. Actions run commands for the selected node.
						</div>
					{:else}
						{#each $actionSettings.actions || [] as action (action.id)}
							<div class="action-row">
								<div class="action-info">
									<span class="action-label">{action.label}</span>
									<code class="action-command">{action.command}</code>
								</div>
								<button
									class="edit-btn"
									onclick={() => handleEditAction(action)}
									title="Edit"
								>
									<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
										<path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
									</svg>
								</button>
							</div>
						{/each}
					{/if}
				</div>
			</section>
		</div>

		<div class="modal-footer">
			<button class="btn btn-primary" onclick={onClose}>
				Done
			</button>
		</div>
	</div>
</div>

{#if showActionEditor}
	<ActionEditor
		action={editingAction}
		onSave={handleSaveAction}
		onCancel={() => { showActionEditor = false; editingAction = null; }}
		onDelete={editingAction ? handleDeleteAction : undefined}
	/>
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
		z-index: 1500;
	}

	.modal {
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 12px;
		width: 550px;
		max-width: 90vw;
		max-height: 85vh;
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
		flex: 1;
	}

	.section {
		margin-bottom: 24px;
	}

	.section:last-child {
		margin-bottom: 0;
	}

	.section h3 {
		font-size: 13px;
		font-weight: 600;
		color: var(--text-primary, #fff);
		margin: 0 0 12px 0;
	}

	.section-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		margin-bottom: 12px;
	}

	.section-header h3 {
		margin: 0;
	}

	.field {
		margin-bottom: 16px;
	}

	.field:last-child {
		margin-bottom: 0;
	}

	.field label {
		display: block;
		font-size: 12px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 6px;
	}

	.field input {
		width: 100%;
		padding: 10px 12px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 6px;
		color: var(--text-primary, #fff);
		font-size: 13px;
		font-family: 'SF Mono', Monaco, monospace;
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

	.field-hint code {
		background: var(--bg-tertiary, #2d2d2d);
		padding: 1px 4px;
		border-radius: 3px;
		font-size: 10px;
	}

	.add-btn {
		padding: 4px 10px;
		background: transparent;
		border: 1px solid var(--accent-color, #3b82f6);
		border-radius: 4px;
		color: var(--accent-color, #3b82f6);
		font-size: 12px;
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.add-btn:hover {
		background: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.env-list {
		display: flex;
		flex-direction: column;
		gap: 8px;
		margin-bottom: 8px;
	}

	.empty-message {
		font-size: 12px;
		color: var(--text-secondary, #666);
		padding: 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border-radius: 6px;
		text-align: center;
	}

	.env-row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.env-key {
		width: 140px !important;
		flex-shrink: 0;
	}

	.env-equals {
		color: var(--text-secondary, #666);
		font-family: 'SF Mono', Monaco, monospace;
	}

	.env-value {
		flex: 1;
	}

	.env-row input {
		padding: 8px 10px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		font-family: 'SF Mono', Monaco, monospace;
	}

	.env-row input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.remove-btn {
		background: none;
		border: none;
		color: var(--text-secondary, #666);
		font-size: 18px;
		cursor: pointer;
		padding: 4px 8px;
		line-height: 1;
		border-radius: 4px;
	}

	.remove-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--error-color, #ef4444);
	}

	.actions-list {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.action-row {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 10px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		border-radius: 6px;
	}

	.action-info {
		flex: 1;
		min-width: 0;
	}

	.action-label {
		display: block;
		font-size: 13px;
		font-weight: 500;
		color: var(--text-primary, #fff);
		margin-bottom: 2px;
	}

	.action-command {
		display: block;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.edit-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 28px;
		height: 28px;
		padding: 0;
		background: transparent;
		border: none;
		border-radius: 4px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
		transition: all 0.15s ease;
	}

	.edit-btn:hover {
		background: var(--border-color, #404040);
		color: var(--text-primary, #fff);
	}

	.modal-footer {
		display: flex;
		justify-content: flex-end;
		padding: 16px 20px;
		border-top: 1px solid var(--border-color, #404040);
	}

	.btn {
		padding: 8px 20px;
		border-radius: 6px;
		font-size: 13px;
		font-weight: 500;
		cursor: pointer;
		border: none;
		transition: all 0.15s ease;
	}

	.btn-primary {
		background: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.btn-primary:hover {
		background: var(--accent-hover, #2563eb);
	}
</style>
