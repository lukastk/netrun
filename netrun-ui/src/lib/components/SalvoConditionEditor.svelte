<script lang="ts">
	import type {
		SalvoConditionConfig,
		SalvoConditionUIState,
		SalvoConditionPreset,
	} from '$lib/types/salvoConditions';
	import {
		createTermTrue,
		createAllPortsReadyTerm,
	} from '$lib/types/salvoConditions';
	import { parseTermExpression, termToExpression } from '$lib/utils/salvoParser';
	import { configToUIState } from '$lib/utils/salvoSerializer';

	interface Props {
		name: string;
		config: SalvoConditionConfig;
		portNames: string[];
		isOutput: boolean;
		onChange?: (config: SalvoConditionConfig) => void;
		onRemove?: () => void;
		onRename?: (newName: string) => void;
		readOnly?: boolean;
	}

	let { name, config, portNames, isOutput, onChange, onRemove, onRename, readOnly = false }: Props = $props();

	// Convert config to UI state
	let uiState = $state<SalvoConditionUIState | null>(null);
	let parseError = $state<string | undefined>(undefined);

	// Local state for editing - initialized from props
	let currentPreset = $state<SalvoConditionPreset>('all_ports_ready');
	let customTermText = $state<string>('');
	let maxSalvosType = $state<'one' | 'infinite' | 'custom'>('one');
	let maxSalvosCustomValue = $state<number>(1);
	let isEditingName = $state(false);
	let editedName = $state('');

	// Flag to skip effect re-sync when we emitted the change ourselves
	let selfEmitted = false;

	// Port selection state
	interface PortUIState {
		enabled: boolean;
		countType: 'all' | 'count';
		count: number;
	}
	let portsState = $state<Record<string, PortUIState>>({});

	// Derive preset from a config object
	function determinePresetFromConfig(cfg: SalvoConditionConfig, ports: string[]): SalvoConditionPreset {
		if (cfg.term.type === 'true') {
			return 'always';
		}

		// Check if it matches "all ports ready" pattern
		const expectedTerm = createAllPortsReadyTerm(ports);
		const currentExpr = termToExpression(cfg.term);
		const expectedExpr = termToExpression(expectedTerm);

		if (currentExpr === expectedExpr) {
			return 'all_ports_ready';
		}

		return 'custom';
	}

	// Build ports state from config
	function buildPortsState(cfg: SalvoConditionConfig, ports: string[]): Record<string, PortUIState> {
		const result: Record<string, PortUIState> = {};
		for (const portName of ports) {
			const portConfig = cfg.ports[portName];
			if (portConfig) {
				result[portName] = {
					enabled: true,
					countType: portConfig.type === 'all' ? 'all' : 'count',
					count: portConfig.type === 'count' ? portConfig.count : 1,
				};
			} else {
				result[portName] = { enabled: false, countType: 'all', count: 1 };
			}
		}
		return result;
	}

	// Initialize and sync state from props (skip when change originated from this component)
	$effect(() => {
		// Read dependencies to track them
		const _config = config;
		const _name = name;
		const _portNames = portNames;

		if (selfEmitted) {
			selfEmitted = false;
			return;
		}

		// Update UI state whenever config/name/portNames change externally
		uiState = configToUIState(_name, _config, _portNames);

		// Sync local editing state
		const preset = determinePresetFromConfig(_config, _portNames);
		currentPreset = preset;
		customTermText = preset === 'custom' ? termToExpression(_config.term) : '';

		maxSalvosType = _config.max_salvos.type === 'infinite'
			? 'infinite'
			: _config.max_salvos.max === 1
				? 'one'
				: 'custom';
		maxSalvosCustomValue = _config.max_salvos.type === 'finite' ? _config.max_salvos.max : 1;

		editedName = _name;
		portsState = buildPortsState(_config, _portNames);
		parseError = undefined;
	});

	function handleNameBlur() {
		isEditingName = false;
		const trimmed = editedName.trim();
		if (trimmed && trimmed !== name && !trimmed.startsWith('__control_') && !trimmed.startsWith('__signal_')) {
			onRename?.(trimmed);
		} else {
			editedName = name;
		}
	}

	function handlePresetChange(newPreset: SalvoConditionPreset) {
		currentPreset = newPreset;
		parseError = undefined;

		if (newPreset === 'always') {
			customTermText = '';
			rebuildAndEmit();
		} else if (newPreset === 'all_ports_ready') {
			customTermText = '';
			rebuildAndEmit();
		} else if (newPreset === 'custom') {
			// Set initial custom text from current term
			customTermText = termToExpression(config.term);
		}
	}

	function handleCustomTermChange(text: string) {
		customTermText = text;

		// Validate the expression
		if (!text.trim()) {
			parseError = 'Expression cannot be empty';
			return;
		}

		const result = parseTermExpression(text, portNames);
		if (!result.success) {
			parseError = result.error;
		} else {
			parseError = undefined;
			rebuildAndEmit();
		}
	}

	function handleMaxSalvosTypeChange(type: 'one' | 'infinite' | 'custom') {
		maxSalvosType = type;
		if (type === 'one') {
			maxSalvosCustomValue = 1;
		}
		rebuildAndEmit();
	}

	function handleMaxSalvosCustomChange(value: number) {
		maxSalvosCustomValue = Math.max(1, value);
		rebuildAndEmit();
	}

	function handlePortToggle(portName: string, enabled: boolean) {
		portsState[portName] = { ...portsState[portName], enabled };
		rebuildAndEmit();
	}

	function handlePortCountTypeChange(portName: string, countType: 'all' | 'count') {
		portsState[portName] = { ...portsState[portName], countType };
		rebuildAndEmit();
	}

	function handlePortCountChange(portName: string, count: number) {
		portsState[portName] = { ...portsState[portName], count: Math.max(1, count) };
		rebuildAndEmit();
	}

	function rebuildAndEmit() {
		// Build term based on preset
		let term: SalvoConditionConfig['term'];

		if (currentPreset === 'always') {
			term = createTermTrue();
		} else if (currentPreset === 'all_ports_ready') {
			term = createAllPortsReadyTerm(portNames);
		} else {
			// Custom
			const result = parseTermExpression(customTermText, portNames);
			if (!result.success) {
				return; // Don't emit if there's a parse error
			}
			term = result.term;
		}

		// Build max_salvos
		// Input salvo conditions must have max_salvos = 1
		let maxSalvos: SalvoConditionConfig['max_salvos'];
		if (!isOutput) {
			// Input conditions: always max_salvos = 1
			maxSalvos = { type: 'finite', max: 1 };
		} else if (maxSalvosType === 'infinite') {
			maxSalvos = { type: 'infinite' };
		} else {
			maxSalvos = { type: 'finite', max: maxSalvosType === 'one' ? 1 : maxSalvosCustomValue };
		}

		// Build ports
		const ports: Record<string, SalvoConditionConfig['ports'][string]> = {};
		for (const [portName, state] of Object.entries(portsState)) {
			if (state.enabled) {
				if (state.countType === 'all') {
					ports[portName] = { type: 'all' };
				} else {
					ports[portName] = { type: 'count', count: state.count };
				}
			}
		}

		selfEmitted = true;
		onChange?.({ max_salvos: maxSalvos, ports, term });
	}
</script>

<div class="salvo-condition-editor" class:read-only={readOnly}>
	<div class="condition-header">
		{#if readOnly}
			<span class="name-display-readonly">{name}</span>
		{:else if isEditingName}
			<input
				type="text"
				class="name-input"
				bind:value={editedName}
				onblur={handleNameBlur}
				onkeydown={(e) => {
					if (e.key === 'Enter') handleNameBlur();
					if (e.key === 'Escape') {
						editedName = name;
						isEditingName = false;
					}
				}}
			/>
		{:else}
			<button class="name-display" onclick={() => { isEditingName = true; editedName = name; }}>
				{name}
			</button>
		{/if}
		{#if !readOnly}
			<button class="remove-btn" onclick={() => onRemove?.()} title="Remove condition">×</button>
		{/if}
	</div>

	<div class="condition-body">
		<!-- Max Salvos (only configurable for output conditions) -->
		{#if isOutput}
			<div class="field">
				<label>Max Salvos</label>
				<div class="max-salvos-row">
					<select
						value={maxSalvosType}
						onchange={(e) => handleMaxSalvosTypeChange((e.target as HTMLSelectElement).value as 'one' | 'infinite' | 'custom')}
					>
						<option value="one">1 (standard)</option>
						<option value="infinite">Infinite</option>
						<option value="custom">Custom...</option>
					</select>
					{#if maxSalvosType === 'custom'}
						<input
							type="number"
							class="custom-number"
							min="1"
							value={maxSalvosCustomValue}
							onchange={(e) => handleMaxSalvosCustomChange(parseInt((e.target as HTMLInputElement).value, 10))}
						/>
					{/if}
				</div>
			</div>
		{/if}

		<!-- Trigger Condition -->
		<div class="field">
			<label>Trigger Condition</label>
			<select
				value={currentPreset}
				onchange={(e) => handlePresetChange((e.target as HTMLSelectElement).value as SalvoConditionPreset)}
			>
				{#if isOutput}
					<option value="always">Always send (true)</option>
					<option value="all_ports_ready">All ports ready</option>
				{:else}
					<option value="all_ports_ready">All ports ready</option>
					<option value="always">Always trigger (true)</option>
				{/if}
				<option value="custom">Custom...</option>
			</select>
		</div>

		{#if currentPreset === 'custom'}
			<div class="field">
				<label>Term Expression</label>
				<input
					type="text"
					class="term-input mono"
					class:error={!!parseError}
					value={customTermText}
					placeholder="in1.non_empty and in2.non_empty"
					oninput={(e) => handleCustomTermChange((e.target as HTMLInputElement).value)}
				/>
				{#if parseError}
					<div class="parse-error">{parseError}</div>
				{/if}
				<div class="term-help">
					<details>
						<summary>Syntax help</summary>
						<ul>
							<li><code>port.non_empty</code> - port has packets</li>
							<li><code>port.empty</code> - port is empty</li>
							<li><code>port.full</code> - port is full</li>
							<li><code>port &gt;= 2</code> - port has at least 2</li>
							<li><code>and</code>, <code>or</code>, <code>not</code> - boolean ops</li>
							<li><code>true</code>, <code>false</code> - constants</li>
						</ul>
					</details>
				</div>
			</div>
		{/if}

		<!-- Ports to Include -->
		<div class="field">
			<label>Ports to Include</label>
			<div class="ports-list">
				{#if portNames.length === 0}
					<div class="empty-hint">No ports defined</div>
				{:else}
					{#each portNames as portName}
						{@const portState = portsState[portName] || { enabled: false, countType: 'all', count: 1 }}
						<div class="port-row">
							<label class="port-checkbox">
								<input
									type="checkbox"
									checked={portState.enabled}
									onchange={(e) => handlePortToggle(portName, (e.target as HTMLInputElement).checked)}
								/>
								<span class="port-name">{portName}</span>
							</label>
							{#if portState.enabled}
								<div class="port-count">
									<select
										value={portState.countType}
										onchange={(e) => handlePortCountTypeChange(portName, (e.target as HTMLSelectElement).value as 'all' | 'count')}
									>
										<option value="all">All</option>
										<option value="count">Count...</option>
									</select>
									{#if portState.countType === 'count'}
										<input
											type="number"
											class="count-input"
											min="1"
											value={portState.count}
											onchange={(e) => handlePortCountChange(portName, parseInt((e.target as HTMLInputElement).value, 10))}
										/>
									{/if}
								</div>
							{/if}
						</div>
					{/each}
				{/if}
			</div>
		</div>
	</div>
</div>

<style>
	.salvo-condition-editor.read-only {
		opacity: 0.6;
		pointer-events: none;
	}

	.salvo-condition-editor {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		margin-bottom: 8px;
		overflow: hidden;
	}

	.condition-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 8px 10px;
		background: var(--bg-tertiary, #2d2d2d);
	}

	.name-display {
		font-weight: 500;
		font-size: 12px;
		color: var(--text-primary, #fff);
		background: transparent;
		border: none;
		padding: 0;
		cursor: pointer;
		text-align: left;
	}

	.name-display:hover {
		color: var(--accent-color, #3b82f6);
	}

	.name-display-readonly {
		font-weight: 500;
		font-size: 12px;
		color: var(--text-primary, #fff);
	}

	.name-input {
		font-size: 12px;
		padding: 2px 6px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--accent-color, #3b82f6);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		width: 120px;
	}

	.name-input:focus {
		outline: none;
	}

	.remove-btn {
		background: transparent;
		color: var(--text-secondary, #a0a0a0);
		padding: 4px 8px;
		font-size: 16px;
		line-height: 1;
		border: none;
		cursor: pointer;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
	}

	.condition-body {
		padding: 10px;
	}

	.field {
		margin-bottom: 10px;
	}

	.field:last-child {
		margin-bottom: 0;
	}

	.field label {
		display: block;
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 4px;
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.field select,
	.field input[type='text'],
	.field input[type='number'] {
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
	}

	.field select {
		width: 100%;
		cursor: pointer;
	}

	.field select:focus,
	.field input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.max-salvos-row {
		display: flex;
		gap: 6px;
		align-items: center;
	}

	.max-salvos-row select {
		flex: 1;
	}

	.custom-number {
		width: 60px;
	}

	.term-input {
		width: 100%;
	}

	.term-input.mono {
		font-size: 11px;
	}

	.term-input.error {
		border-color: var(--error-color, #ef4444);
		background: rgba(239, 68, 68, 0.05);
	}

	.parse-error {
		margin-top: 4px;
		font-size: 10px;
		color: var(--error-color, #ef4444);
	}

	.term-help {
		margin-top: 6px;
	}

	.term-help details {
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
	}

	.term-help summary {
		cursor: pointer;
		user-select: none;
	}

	.term-help summary:hover {
		color: var(--accent-color, #3b82f6);
	}

	.term-help ul {
		margin: 6px 0 0 0;
		padding-left: 16px;
	}

	.term-help li {
		margin-bottom: 2px;
	}

	.term-help code {
		background: var(--bg-tertiary, #2d2d2d);
		padding: 1px 4px;
		border-radius: 2px;
	}

	.ports-list {
		display: flex;
		flex-direction: column;
		gap: 6px;
	}

	.port-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
	}

	.port-checkbox {
		display: flex;
		align-items: center;
		gap: 6px;
		cursor: pointer;
	}

	.port-checkbox input[type='checkbox'] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}

	.port-name {
		font-size: 11px;
		color: var(--text-primary, #fff);
	}

	.port-count {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.port-count select {
		font-size: 10px;
		padding: 3px 6px;
	}

	.count-input {
		width: 50px;
		font-size: 10px;
		padding: 3px 6px;
	}

	.empty-hint {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		font-style: italic;
	}
</style>
