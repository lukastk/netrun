<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';
	import { configSchema, getFieldDescription } from '$lib/stores/schemaStore';
	import AutoConfigFields from './AutoConfigFields.svelte';
	import { tooltip } from '$lib/utils/tooltip';

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
	let deadLetterCallback = $derived(getValue<string | null>('dead_letter_callback', null));

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
		/>
	{/if}

	<!-- Dead Letter Callback (custom: complex type rendered as text input) -->
	{#if deadLetterQueue}
		<div class="subsection">
			<div class="subsection-header">Dead Letter Callback</div>
			<div class="field">
				<label for="dead-letter-callback">Callback Import Path{#if desc('dead_letter_callback')}<span class="has-tooltip-icon" use:tooltip={desc('dead_letter_callback')}>?</span>{/if}</label>
				<input
					id="dead-letter-callback"
					type="text"
					value={deadLetterCallback || ''}
					placeholder="(optional) module.path.callback"
					oninput={(e) => updateValueLive('dead_letter_callback', (e.target as HTMLInputElement).value || null)}
					onblur={() => pushHistory()}
					class="mono"
				/>
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
		display: block;
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
</style>
