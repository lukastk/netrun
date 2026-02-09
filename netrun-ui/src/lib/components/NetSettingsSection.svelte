<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';
	import { configSchema, getFieldDescription } from '$lib/stores/schemaStore';
	import AutoConfigFields from './AutoConfigFields.svelte';
	import { tooltip } from '$lib/utils/tooltip';
	import { isEnvVar, isVarRef, makeEnvVar, getEnvVarName, getEnvVarDefault } from '$lib/utils/envvar';
	import { projectVarNames } from '$lib/stores/variablesStore';

	function desc(field: string): string | undefined {
		return getFieldDescription($configSchema, 'NetConfig', field);
	}

	interface Props {
		extraData: Record<string, unknown> | null;
		onUpdate: (updates: Record<string, unknown>) => void;
	}

	let { extraData, onUpdate }: Props = $props();

	let netSchema = $derived($configSchema?.models['NetConfig'] ?? null);

	// Get values with defaults
	function getValue<T>(key: string, defaultValue: T): T {
		if (!extraData) return defaultValue;
		const value = extraData[key];
		return value !== undefined ? (value as T) : defaultValue;
	}

	let deadLetterQueue = $derived(getValue<boolean>('dead_letter_queue', true));
	let deadLetterCallback = $derived(getValue<unknown>('dead_letter_callback', null));

	// Track env var mode for dead_letter_callback
	let callbackEnvMode = $state(false);
	$effect(() => {
		callbackEnvMode = isEnvVar(deadLetterCallback);
	});

	function toggleCallbackEnvMode() {
		if (callbackEnvMode) {
			const def = isEnvVar(deadLetterCallback) ? getEnvVarDefault(deadLetterCallback) : null;
			updateValueLive('dead_letter_callback', def);
		} else {
			const current = typeof deadLetterCallback === 'string' ? deadLetterCallback : null;
			onUpdate({ ...extraData, dead_letter_callback: makeEnvVar('', current) });
		}
		callbackEnvMode = !callbackEnvMode;
		pushHistory();
	}

	function updateValueLive(key: string, value: unknown) {
		if (value === '' || value === null) {
			const { [key]: _removed, ...rest } = extraData || {};
			onUpdate(rest);
		} else {
			onUpdate({ ...extraData, [key]: value });
		}
	}
</script>

<div class="net-settings-section">
	{#if netSchema}
		<AutoConfigFields
			modelName="NetConfig"
			schema={netSchema}
			values={extraData ?? {}}
			onUpdate={(updates) => { onUpdate(updates); pushHistory(); }}
			onUpdateLive={(updates) => onUpdate(updates)}
			availableVarNames={$projectVarNames}
		/>
	{/if}

	<!-- Dead Letter Callback (custom: complex type rendered as text input) -->
	{#if deadLetterQueue}
		<div class="subsection">
			<div class="subsection-header">Dead Letter Callback</div>
			<div class="field">
				<label for="dead-letter-callback">
					Callback Import Path{#if desc('dead_letter_callback')}<span class="has-tooltip-icon" use:tooltip={desc('dead_letter_callback')}>?</span>{/if}
					<button
						class="envvar-toggle"
						class:active={callbackEnvMode}
						title={callbackEnvMode ? 'Switch to literal value' : 'Switch to environment variable'}
						onclick={toggleCallbackEnvMode}
					>$</button>
				</label>
				{#if callbackEnvMode}
					<div class="envvar-input-group">
						<div class="envvar-name-row">
							<span class="envvar-prefix">$</span>
							<input
								type="text"
								class="envvar-name-input"
								value={getEnvVarName(deadLetterCallback)}
								placeholder="ENV_VAR_NAME"
								oninput={(e) => {
									const name = (e.target as HTMLInputElement).value;
									const def = isEnvVar(deadLetterCallback) ? getEnvVarDefault(deadLetterCallback) : null;
									onUpdate({ ...extraData, dead_letter_callback: makeEnvVar(name, def) });
								}}
								onblur={() => pushHistory()}
							/>
						</div>
						<div class="envvar-default-row">
							<span class="envvar-default-label">default:</span>
							<input
								type="text"
								class="envvar-default-input"
								value={getEnvVarDefault(deadLetterCallback) ?? ''}
								placeholder="module.path.callback"
								oninput={(e) => {
									const def = (e.target as HTMLInputElement).value || null;
									const name = isEnvVar(deadLetterCallback) ? getEnvVarName(deadLetterCallback) : '';
									onUpdate({ ...extraData, dead_letter_callback: makeEnvVar(name, def) });
								}}
								onblur={() => pushHistory()}
							/>
						</div>
					</div>
				{:else}
					<input
						id="dead-letter-callback"
						type="text"
						value={typeof deadLetterCallback === 'string' ? deadLetterCallback : ''}
						placeholder="(optional) module.path.callback"
						oninput={(e) => updateValueLive('dead_letter_callback', (e.target as HTMLInputElement).value || null)}
						onblur={() => pushHistory()}
						class="mono"
					/>
				{/if}
				<span class="field-hint">Import path to callback function for dead letter packets</span>
			</div>
		</div>
	{/if}
</div>

<style>
	.net-settings-section {
		display: flex;
		flex-direction: column;
		gap: 12px;
	}

	.subsection {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		padding: 10px;
	}

	.subsection-header {
		font-size: 11px;
		font-weight: 500;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
		margin-bottom: 10px;
	}

	.field {
		margin-bottom: 10px;
	}

	.field:last-child {
		margin-bottom: 0;
	}

	.field label {
		display: flex;
		align-items: center;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 4px;
	}

	.field input {
		width: 100%;
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.field input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field input.mono {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
	}

	.field-hint {
		display: block;
		font-size: 10px;
		color: var(--text-secondary, #666);
		margin-top: 4px;
	}

	.has-tooltip-icon {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 13px;
		height: 13px;
		margin-left: 4px;
		border-radius: 50%;
		background: var(--border-color, #404040);
		color: var(--text-secondary, #a0a0a0);
		font-size: 9px;
		font-weight: 600;
		vertical-align: middle;
		cursor: help;
	}

	/* Env var toggle and inputs */
	.envvar-toggle {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		width: 16px;
		height: 16px;
		margin-left: auto;
		padding: 0;
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		background: transparent;
		color: var(--text-secondary, #666);
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 10px;
		font-weight: 700;
		cursor: pointer;
		line-height: 1;
		flex-shrink: 0;
	}

	.envvar-toggle:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.envvar-toggle.active {
		background: var(--accent-color, #3b82f6);
		border-color: var(--accent-color, #3b82f6);
		color: #fff;
	}

	.envvar-input-group {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.envvar-name-row {
		display: flex;
		align-items: center;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid rgba(59, 130, 246, 0.4);
		border-radius: 3px;
		overflow: hidden;
	}

	.envvar-prefix {
		padding: 6px 4px 6px 8px;
		color: var(--accent-color, #3b82f6);
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 12px;
		font-weight: 700;
		user-select: none;
	}

	.envvar-name-input {
		flex: 1;
		padding: 6px 8px 6px 0;
		background: transparent;
		border: none;
		color: var(--text-primary, #fff);
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 12px;
	}

	.envvar-name-input:focus {
		outline: none;
	}

	.envvar-default-row {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.envvar-default-label {
		font-size: 10px;
		color: var(--text-secondary, #666);
		white-space: nowrap;
		flex-shrink: 0;
	}

	.envvar-default-input {
		flex: 1;
		padding: 3px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
		min-width: 0;
	}

	.envvar-default-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
		color: var(--text-primary, #fff);
	}
</style>
