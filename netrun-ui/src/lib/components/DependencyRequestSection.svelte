<script lang="ts">
	import {
		updateNodeDataLive,
		pushHistory,
		type FlowNode,
	} from '$lib/stores/flowStore';

	interface Props {
		node: FlowNode;
	}

	let { node }: Props = $props();

	let config = $derived((node.data._config || {}) as Record<string, unknown>);
	let depReq = $derived(config.dependency_request as Record<string, unknown> | undefined);
	let hasConfig = $derived(depReq !== undefined && depReq !== null);

	let triggers = $derived((depReq?.triggers as string[]) || []);
	let label = $derived((depReq?.label as string) || '');

	function updateDepReq(updates: Record<string, unknown>) {
		const current = depReq || { triggers: ['on_startup'], label: '' };
		const newDepReq = { ...current, ...updates };
		updateNodeDataLive(node.id, {
			_config: { ...config, dependency_request: newDepReq },
		});
	}

	function addConfig() {
		updateNodeDataLive(node.id, {
			_config: {
				...config,
				dependency_request: { triggers: ['on_startup'], label: '' },
			},
		});
		pushHistory();
	}

	function removeConfig() {
		const { dependency_request: _removed, ...rest } = config;
		updateNodeDataLive(node.id, { _config: rest });
		pushHistory();
	}

	function toggleTrigger(trigger: string) {
		const current = [...triggers];
		const idx = current.indexOf(trigger);
		if (idx >= 0) {
			current.splice(idx, 1);
		} else {
			current.push(trigger);
		}
		updateDepReq({ triggers: current });
	}

	function updateLabel(e: Event) {
		const value = (e.target as HTMLInputElement).value;
		updateDepReq({ label: value });
	}
</script>

<div class="dep-request-content">
	{#if hasConfig}
		<div class="field">
			<span class="field-label">Triggers</span>
			<label class="checkbox-label">
				<input
					type="checkbox"
					checked={triggers.includes('on_startup')}
					onchange={() => toggleTrigger('on_startup')}
				/>
				on_startup
			</label>
			<label class="checkbox-label">
				<input
					type="checkbox"
					checked={triggers.includes('on_no_salvo_triggered')}
					onchange={() => toggleTrigger('on_no_salvo_triggered')}
				/>
				on_no_salvo_triggered
			</label>
		</div>
		<div class="field">
			<label for="dep-label">Label</label>
			<input
				id="dep-label"
				type="text"
				value={label}
				oninput={updateLabel}
				onblur={() => pushHistory()}
				placeholder="(optional)"
			/>
		</div>
		<button class="danger-btn" onclick={removeConfig}>
			Remove Config
		</button>
	{:else}
		<p class="empty-message">No dependency request configured.</p>
		<p class="default-note">
			Default: nodes with dependency edges automatically get <code>on_startup</code> trigger
			and label <code>"main"</code> if no config is set.
		</p>
		<button class="add-btn" onclick={addConfig}>
			Add Dependency Request
		</button>
	{/if}
</div>

<style>
	.dep-request-content {
		padding: 0;
	}

	.field {
		margin-bottom: 10px;
	}

	.field-label {
		display: block;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 4px;
	}

	.field label[for] {
		display: block;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 4px;
	}

	.checkbox-label {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		color: var(--text-primary, #fff);
		cursor: pointer;
		margin-bottom: 4px;
		font-family: monospace;
	}

	.checkbox-label input[type="checkbox"] {
		accent-color: #a78bfa;
	}

	input[type="text"] {
		width: 100%;
		padding: 4px 8px;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		box-sizing: border-box;
	}

	.empty-message {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin: 0 0 4px;
	}

	.default-note {
		font-size: 10px;
		color: var(--text-secondary, #808080);
		margin: 0 0 8px;
		line-height: 1.4;
	}

	.default-note code {
		background: var(--bg-secondary, #242424);
		padding: 1px 4px;
		border-radius: 3px;
		font-size: 10px;
	}

	.add-btn {
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		color: var(--text-primary, #fff);
		padding: 4px 10px;
		border-radius: 4px;
		font-size: 11px;
		cursor: pointer;
		width: 100%;
	}

	.add-btn:hover {
		background: var(--bg-secondary, #353535);
	}

	.danger-btn {
		background: rgba(239, 68, 68, 0.1);
		border: 1px solid rgba(239, 68, 68, 0.3);
		color: var(--error-color, #ef4444);
		padding: 4px 10px;
		border-radius: 4px;
		font-size: 11px;
		cursor: pointer;
		width: 100%;
	}

	.danger-btn:hover {
		background: rgba(239, 68, 68, 0.2);
	}
</style>
