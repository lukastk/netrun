<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';

	// Pool allocation method enum (matches Python RunAllocationMethod)
	type PoolAllocationMethod = 'round-robin' | 'random' | 'least-busy';

	interface Props {
		extraData: Record<string, unknown> | null;
		onUpdate: (updates: Record<string, unknown>) => void;
	}

	let { extraData, onUpdate }: Props = $props();

	// Get values with defaults
	function getValue<T>(key: string, defaultValue: T): T {
		if (!extraData) return defaultValue;
		const value = extraData[key];
		return value !== undefined ? (value as T) : defaultValue;
	}

	// Derived values
	let defaultPoolAllocation = $derived(getValue<PoolAllocationMethod>('default_pool_allocation_method', 'round-robin'));
	let deadLetterQueue = $derived(getValue<boolean>('dead_letter_queue', true));
	let deadLetterPath = $derived(getValue<string | null>('dead_letter_path', null));
	let deadLetterCallback = $derived(getValue<string | null>('dead_letter_callback', null));
	let catchAllOutputQueue = $derived(getValue<string | null>('catch_all_output_queue', null));
	let undeclaredOutputBehavior = $derived(getValue<'discard' | 'error'>('undeclared_output_behavior', 'discard'));

	function updateValue(key: string, value: unknown) {
		// If value is empty string or null, remove the key (use default)
		if (value === '' || value === null) {
			const { [key]: _removed, ...rest } = extraData || {};
			onUpdate(rest);
		} else {
			onUpdate({ ...extraData, [key]: value });
		}
	}

	function updateValueLive(key: string, value: unknown) {
		if (value === '' || value === null) {
			const { [key]: _removed, ...rest } = extraData || {};
			onUpdate(rest);
		} else {
			onUpdate({ ...extraData, [key]: value });
		}
	}

	// Allocation method labels
	const allocationMethodLabels: Record<PoolAllocationMethod, string> = {
		'round-robin': 'Round Robin',
		'random': 'Random',
		'least-busy': 'Least Busy',
	};
</script>

<div class="net-settings-section">
	<!-- Pool Allocation Method -->
	<div class="field">
		<label for="default-allocation">Default Pool Allocation</label>
		<select
			id="default-allocation"
			value={defaultPoolAllocation}
			onchange={(e) => {
				updateValue('default_pool_allocation_method', (e.target as HTMLSelectElement).value);
				pushHistory();
			}}
		>
			<option value="round-robin">{allocationMethodLabels['round-robin']}</option>
			<option value="random">{allocationMethodLabels['random']}</option>
			<option value="least-busy">{allocationMethodLabels['least-busy']}</option>
		</select>
		<span class="field-hint">How to select workers when a node has multiple pools</span>
	</div>

	<!-- Dead Letter Queue Section -->
	<div class="subsection">
		<div class="subsection-header">Dead Letter Queue</div>

		<label class="checkbox-field">
			<input
				type="checkbox"
				checked={deadLetterQueue}
				onchange={(e) => {
					updateValue('dead_letter_queue', (e.target as HTMLInputElement).checked);
					pushHistory();
				}}
			/>
			<span>Enable dead letter queue</span>
		</label>

		{#if deadLetterQueue}
			<div class="field">
				<label for="dead-letter-path">Dead Letter Path</label>
				<input
					id="dead-letter-path"
					type="text"
					value={deadLetterPath || ''}
					placeholder="(optional) path/to/dead_letters.json"
					oninput={(e) => updateValueLive('dead_letter_path', (e.target as HTMLInputElement).value || null)}
					onblur={() => pushHistory()}
					class="mono"
				/>
			</div>

			<div class="field">
				<label for="dead-letter-callback">Dead Letter Callback</label>
				<input
					id="dead-letter-callback"
					type="text"
					value={deadLetterCallback || ''}
					placeholder="(optional) module.path.callback"
					oninput={(e) => updateValueLive('dead_letter_callback', (e.target as HTMLInputElement).value || null)}
					onblur={() => pushHistory()}
					class="mono"
				/>
				<span class="field-hint">Import path to callback function</span>
			</div>
		{/if}
	</div>

	<!-- Output Queue Settings -->
	<div class="subsection">
		<div class="subsection-header">Output Queue Behavior</div>

		<div class="field">
			<label for="catch-all-queue">Catch-All Queue</label>
			<input
				id="catch-all-queue"
				type="text"
				value={catchAllOutputQueue || ''}
				placeholder="(optional) queue_name"
				oninput={(e) => updateValueLive('catch_all_output_queue', (e.target as HTMLInputElement).value || null)}
				onblur={() => pushHistory()}
			/>
			<span class="field-hint">Queue for packets from unconnected ports not in any configured queue</span>
		</div>

		<div class="field">
			<label for="undeclared-behavior">Undeclared Output Behavior</label>
			<select
				id="undeclared-behavior"
				value={undeclaredOutputBehavior}
				onchange={(e) => {
					updateValue('undeclared_output_behavior', (e.target as HTMLSelectElement).value);
					pushHistory();
				}}
			>
				<option value="discard">Discard silently</option>
				<option value="error">Raise error</option>
			</select>
			<span class="field-hint">What to do with packets from unconnected ports not in any queue</span>
		</div>
	</div>
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

	.field input,
	.field select {
		width: 100%;
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.field input:focus,
	.field select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field input.mono {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
	}

	.field select {
		cursor: pointer;
	}

	.field-hint {
		display: block;
		font-size: 10px;
		color: var(--text-secondary, #666);
		margin-top: 4px;
	}

	.checkbox-field {
		display: flex;
		align-items: center;
		gap: 8px;
		cursor: pointer;
		font-size: 12px;
		color: var(--text-primary, #fff);
		margin-bottom: 10px;
	}

	.checkbox-field input[type='checkbox'] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}
</style>
