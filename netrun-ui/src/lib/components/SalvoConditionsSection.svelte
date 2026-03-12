<script lang="ts">
	import type { SalvoConditionConfig } from '$lib/types/salvoConditions';
	import { createDefaultSalvoCondition } from '$lib/types/salvoConditions';
	import SalvoConditionEditor from './SalvoConditionEditor.svelte';

	interface Props {
		inConditions: Record<string, SalvoConditionConfig> | null;
		outConditions: Record<string, SalvoConditionConfig> | null;
		inPortNames: string[];
		outPortNames: string[];
		onUpdateIn: (conditions: Record<string, SalvoConditionConfig> | null) => void;
		onUpdateOut: (conditions: Record<string, SalvoConditionConfig> | null) => void;
		isFactory?: boolean;
	}

	let {
		inConditions,
		outConditions,
		inPortNames,
		outPortNames,
		onUpdateIn,
		onUpdateOut,
		isFactory = false,
	}: Props = $props();

	let defaultsLabel = $derived(isFactory ? 'Use defaults (from factory)' : 'Use defaults (auto-generate)');

	// Sub-section collapse state
	let inputOpen = $state(true);
	let outputOpen = $state(true);

	// Determine if using defaults
	let inUseDefaults = $derived(inConditions === null);
	let outUseDefaults = $derived(outConditions === null);

	// Get condition entries for display
	let inConditionEntries = $derived(
		inConditions ? Object.entries(inConditions) : []
	);
	let outConditionEntries = $derived(
		outConditions ? Object.entries(outConditions) : []
	);

	// Default conditions to show read-only when "Use defaults" is checked
	let defaultInCondition = $derived(
		inUseDefaults ? createDefaultSalvoCondition(inPortNames, 'all_ports_ready') : null
	);
	let defaultOutCondition = $derived(
		outUseDefaults ? createDefaultSalvoCondition(outPortNames, 'always') : null
	);

	function toggleInputDefaults() {
		if (inUseDefaults) {
			// Switch to explicit: create a default condition
			onUpdateIn({
				default: createDefaultSalvoCondition(inPortNames, 'all_ports_ready'),
			});
		} else {
			// Switch to defaults
			onUpdateIn(null);
		}
	}

	function toggleOutputDefaults() {
		if (outUseDefaults) {
			// Switch to explicit: create a default condition
			onUpdateOut({
				default: createDefaultSalvoCondition(outPortNames, 'always'),
			});
		} else {
			// Switch to defaults
			onUpdateOut(null);
		}
	}

	function addInCondition() {
		const current = inConditions || {};
		const name = generateUniqueName(current, 'condition');
		onUpdateIn({
			...current,
			[name]: createDefaultSalvoCondition(inPortNames, 'all_ports_ready'),
		});
	}

	function addOutCondition() {
		const current = outConditions || {};
		const name = generateUniqueName(current, 'condition');
		onUpdateOut({
			...current,
			[name]: createDefaultSalvoCondition(outPortNames, 'always'),
		});
	}

	function updateInCondition(name: string, config: SalvoConditionConfig) {
		const current = inConditions || {};
		onUpdateIn({ ...current, [name]: config });
	}

	function updateOutCondition(name: string, config: SalvoConditionConfig) {
		const current = outConditions || {};
		onUpdateOut({ ...current, [name]: config });
	}

	function removeInCondition(name: string) {
		const current = { ...(inConditions || {}) };
		delete current[name];
		// If no conditions left, switch to defaults
		if (Object.keys(current).length === 0) {
			onUpdateIn(null);
		} else {
			onUpdateIn(current);
		}
	}

	function removeOutCondition(name: string) {
		const current = { ...(outConditions || {}) };
		delete current[name];
		// If no conditions left, switch to defaults
		if (Object.keys(current).length === 0) {
			onUpdateOut(null);
		} else {
			onUpdateOut(current);
		}
	}

	function renameInCondition(oldName: string, newName: string) {
		if (oldName === newName) return;
		const current = inConditions || {};
		if (newName in current) return; // Name already exists
		if (newName.startsWith('__control_') || newName.startsWith('__signal_')) return; // Reserved prefix

		const updated: Record<string, SalvoConditionConfig> = {};
		for (const [name, config] of Object.entries(current)) {
			if (name === oldName) {
				updated[newName] = config;
			} else {
				updated[name] = config;
			}
		}
		onUpdateIn(updated);
	}

	function renameOutCondition(oldName: string, newName: string) {
		if (oldName === newName) return;
		const current = outConditions || {};
		if (newName in current) return; // Name already exists
		if (newName.startsWith('__control_') || newName.startsWith('__signal_')) return; // Reserved prefix

		const updated: Record<string, SalvoConditionConfig> = {};
		for (const [name, config] of Object.entries(current)) {
			if (name === oldName) {
				updated[newName] = config;
			} else {
				updated[name] = config;
			}
		}
		onUpdateOut(updated);
	}

	function generateUniqueName(existing: Record<string, unknown>, base: string): string {
		if (!(base in existing)) return base;
		let i = 1;
		while (`${base}_${i}` in existing) {
			i++;
		}
		return `${base}_${i}`;
	}
</script>

<div class="salvo-conditions-section">
	<!-- Input Conditions Sub-section -->
	<div class="subsection">
		<button class="subsection-header" onclick={() => (inputOpen = !inputOpen)}>
			<span class="subsection-title">Input Conditions</span>
			<span class="subsection-toggle">{inputOpen ? '−' : '+'}</span>
		</button>
		{#if inputOpen}
			<div class="subsection-content">
				<label class="defaults-toggle">
					<input type="checkbox" checked={inUseDefaults} onchange={toggleInputDefaults} />
					<span>{defaultsLabel}</span>
				</label>

				{#if inUseDefaults}
					{#if defaultInCondition}
						<SalvoConditionEditor
							name="default"
							config={defaultInCondition}
							portNames={inPortNames}
							isOutput={false}
							readOnly={true}
						/>
					{/if}
				{:else}
					{#each inConditionEntries as [name, config]}
						<SalvoConditionEditor
							{name}
							{config}
							portNames={inPortNames}
							isOutput={false}
							onChange={(newConfig) => updateInCondition(name, newConfig)}
							onRemove={() => removeInCondition(name)}
							onRename={(newName) => renameInCondition(name, newName)}
						/>
					{/each}
					<button class="add-btn" onclick={addInCondition}>+ Add Input Condition</button>
				{/if}
			</div>
		{/if}
	</div>

	<!-- Output Conditions Sub-section -->
	<div class="subsection">
		<button class="subsection-header" onclick={() => (outputOpen = !outputOpen)}>
			<span class="subsection-title">Output Conditions</span>
			<span class="subsection-toggle">{outputOpen ? '−' : '+'}</span>
		</button>
		{#if outputOpen}
			<div class="subsection-content">
				<label class="defaults-toggle">
					<input type="checkbox" checked={outUseDefaults} onchange={toggleOutputDefaults} />
					<span>{defaultsLabel}</span>
				</label>

				{#if outUseDefaults}
					{#if defaultOutCondition}
						<SalvoConditionEditor
							name="default"
							config={defaultOutCondition}
							portNames={outPortNames}
							isOutput={true}
							readOnly={true}
						/>
					{/if}
				{:else}
					{#each outConditionEntries as [name, config]}
						<SalvoConditionEditor
							{name}
							{config}
							portNames={outPortNames}
							isOutput={true}
							onChange={(newConfig) => updateOutCondition(name, newConfig)}
							onRemove={() => removeOutCondition(name)}
							onRename={(newName) => renameOutCondition(name, newName)}
						/>
					{/each}
					<button class="add-btn" onclick={addOutCondition}>+ Add Output Condition</button>
				{/if}
			</div>
		{/if}
	</div>
</div>

<style>
	.salvo-conditions-section {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.subsection {
		background: var(--bg-primary, #1a1a1a);
		border-radius: 4px;
		overflow: hidden;
	}

	.subsection-header {
		width: 100%;
		padding: 8px 10px;
		display: flex;
		justify-content: space-between;
		align-items: center;
		background: transparent;
		border: none;
		cursor: pointer;
		text-align: left;
	}

	.subsection-header:hover {
		background: var(--bg-tertiary, #2d2d2d);
	}

	.subsection-title {
		font-weight: 500;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.subsection-toggle {
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
	}

	.subsection-content {
		padding: 10px;
		border-top: 1px solid var(--border-color, #404040);
	}

	.defaults-toggle {
		display: flex;
		align-items: center;
		gap: 8px;
		margin-bottom: 10px;
		cursor: pointer;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}

	.defaults-toggle input[type='checkbox'] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}

	.defaults-toggle:hover {
		color: var(--text-primary, #fff);
	}

	.add-btn {
		width: 100%;
		padding: 6px;
		margin-top: 6px;
		font-size: 11px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
	}

	.add-btn:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}
</style>
