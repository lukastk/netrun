<script lang="ts">
	import {
		selectedNode,
		selectedNodeIds,
		updateNodeData,
		updateNodeDataLive,
		renameNode,
		updateNodeActions,
		updateNodeSalvoConditions,
		getNodeSalvoConditions,
		updateNodeExecutionConfig,
		getNodeExecutionConfig,
		updateFactoryNodePreview,
		deleteNodes,
		pushHistory,
		activeTab,
		isNewFile,
		extraData,
		graphExtra,
		updateExtraDataLive,
		updateGraphExtraLive,
		type NetrunNodeData,
		type PortConfig
	} from '$lib/stores/flowStore';
	import SalvoConditionsSection from './SalvoConditionsSection.svelte';
	import PoolsSection from './PoolsSection.svelte';
	import NodeExecutionSection from './NodeExecutionSection.svelte';
	import NetSettingsSection from './NetSettingsSection.svelte';
	import type { SalvoConditionConfig } from '$lib/types/salvoConditions';
	import { parseSalvoConditionsFromJSON, salvoConditionsToJSON } from '$lib/utils/salvoSerializer';
	import { api, type FactoryParameter } from '$lib/api';
	import ActionsPanel from './ActionsPanel.svelte';
	import ProjectSettings from './ProjectSettings.svelte';
	import ActionEditor from './ActionEditor.svelte';
	import TextInputModal from './TextInputModal.svelte';
	import {
		projectActions,
		nodeActions,
		addProjectAction,
		updateProjectAction,
		removeProjectAction,
		generateActionId,
		type Action,
	} from '$lib/stores/actionsStore';
	import {
		projectNodeVars,
		nodeNodeVars,
		allNodesVars,
		updateProjectNodeVars,
		updateNodeNodeVars,
	} from '$lib/stores/variablesStore';
	import NodeVariablesSection from './NodeVariablesSection.svelte';
	import AllNodeVariablesSection from './AllNodeVariablesSection.svelte';
	import { unregisteredFields } from '$lib/stores/schemaStore';

	// Loading state for factory preview
	let isRefreshing = $state(false);

	// Factory signature state
	let factoryParams = $state<FactoryParameter[]>([]);
	let factorySignatureLoading = $state(false);
	let lastFactoryPath = $state<string | null>(null);

	// Modal state for actions
	let showProjectSettings = $state(false);
	let showActionEditor = $state(false);
	let editingAction = $state<Action | null>(null);
	let editingNodeAction = $state(false); // true = node-level, false = project-level

	// Modal state for factory creation
	let showAddFactoryModal = $state(false);

	// Handle action editor save
	function handleSaveAction(action: Action) {
		if (editingNodeAction && $selectedNode) {
			const current = [...$nodeActions];
			if (editingAction) {
				const idx = current.findIndex(a => a.id === editingAction!.id);
				if (idx >= 0) current[idx] = action;
				else current.push(action);
			} else {
				current.push({ ...action, id: generateActionId() });
			}
			updateNodeActions($selectedNode.id, current);
			pushHistory();
		} else {
			if (editingAction) {
				updateProjectAction(action.id, action);
			} else {
				addProjectAction({ ...action, id: generateActionId() });
			}
		}
		showActionEditor = false;
		editingAction = null;
		editingNodeAction = false;
	}

	// Handle action delete
	function handleDeleteAction() {
		if (editingAction) {
			if (editingNodeAction && $selectedNode) {
				const current = $nodeActions.filter(a => a.id !== editingAction!.id);
				updateNodeActions($selectedNode.id, current.length > 0 ? current : undefined);
				pushHistory();
			} else {
				removeProjectAction(editingAction.id);
			}
		}
		showActionEditor = false;
		editingAction = null;
		editingNodeAction = false;
	}

	// Collapsible sections state
	let sectionsOpen = $state({
		general: true,
		inPorts: true,
		outPorts: true,
		factory: true,
		subgraph: true,
		salvoConditions: false,
		execution: false,
		nodeVariables: false,
		// Net-level sections
		graphSettings: true,
		pools: true,
		netSettings: false,
		factories: true,
		uiSettings: false,
		netNodeVariables: false,
		allNodeVariables: false,
	});

	// Sidebar resize state
	const MIN_WIDTH = 200;
	const MAX_WIDTH = 600;
	const DEFAULT_WIDTH = 300;
	let sidebarWidth = $state(DEFAULT_WIDTH);
	let isResizing = $state(false);

	function startResize(e: MouseEvent) {
		e.preventDefault();
		isResizing = true;
		document.addEventListener('mousemove', handleResize);
		document.addEventListener('mouseup', stopResize);
		document.body.style.cursor = 'ew-resize';
		document.body.style.userSelect = 'none';
	}

	function handleResize(e: MouseEvent) {
		if (!isResizing) return;
		// Sidebar is on the right, so width = window width - mouse X
		const newWidth = window.innerWidth - e.clientX;
		sidebarWidth = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, newWidth));
	}

	function stopResize() {
		isResizing = false;
		document.removeEventListener('mousemove', handleResize);
		document.removeEventListener('mouseup', stopResize);
		document.body.style.cursor = '';
		document.body.style.userSelect = '';
	}

	function toggleSection(section: keyof typeof sectionsOpen) {
		sectionsOpen[section] = !sectionsOpen[section];
	}

	// Local state for the name input (to avoid changing node ID on every keystroke)
	let localNodeName = $state('');
	let lastSyncedNodeId = $state<string | null>(null);

	// Sync local name from selected node when selection changes
	$effect(() => {
		if ($selectedNode && $selectedNode.id !== lastSyncedNodeId) {
			localNodeName = $selectedNode.data.label;
			lastSyncedNodeId = $selectedNode.id;
		} else if (!$selectedNode) {
			localNodeName = '';
			lastSyncedNodeId = null;
		}
	});

	// Update handlers
	function updateLabel(event: Event) {
		const target = event.target as HTMLInputElement;
		localNodeName = target.value;
	}

	function commitNodeRename() {
		if (!$selectedNode) return;
		const oldName = $selectedNode.id;
		const newName = localNodeName.trim();

		if (!newName || newName === oldName) {
			// Revert to current name if empty
			localNodeName = $selectedNode.data.label;
			return;
		}

		pushHistory();
		const success = renameNode(oldName, newName);
		if (!success) {
			// Duplicate or invalid - revert
			localNodeName = $selectedNode.data.label;
		} else {
			// Update lastSyncedNodeId to the new name so $effect doesn't overwrite
			lastSyncedNodeId = newName;
		}
	}

	function onFieldBlur() {
		// Push history when user finishes editing a field
		pushHistory();
	}

	function updatePortName(
		portType: 'inPorts' | 'outPorts',
		index: number,
		newName: string
	) {
		if (!$selectedNode) return;
		const ports = [...$selectedNode.data[portType]];
		ports[index] = { ...ports[index], name: newName };
		updateNodeDataLive($selectedNode.id, { [portType]: ports });
	}

	function updatePortType(
		portType: 'inPorts' | 'outPorts',
		index: number,
		newType: string
	) {
		if (!$selectedNode) return;
		const ports = [...$selectedNode.data[portType]];
		ports[index] = { ...ports[index], type: newType || undefined };
		updateNodeDataLive($selectedNode.id, { [portType]: ports });
	}

	function addPort(portType: 'inPorts' | 'outPorts') {
		if (!$selectedNode) return;
		const ports = [...$selectedNode.data[portType]];
		const newPort: PortConfig = {
			name: `port_${ports.length}`,
			type: 'any'
		};
		updateNodeData($selectedNode.id, { [portType]: [...ports, newPort] });
	}

	function removePort(portType: 'inPorts' | 'outPorts', index: number) {
		if (!$selectedNode) return;
		const ports = [...$selectedNode.data[portType]];
		ports.splice(index, 1);
		updateNodeData($selectedNode.id, { [portType]: ports });
	}

	function updateFactoryArg(key: string, value: string | boolean) {
		if (!$selectedNode) return;
		const factoryArgs: Record<string, unknown> = { ...($selectedNode.data.factoryArgs || {}) };

		// Find the parameter definition to determine type conversion
		const param = factoryParams.find(p => p.name === key);
		if (param) {
			const converted = convertFactoryArgValue(param, value);
			if (converted === undefined) {
				delete factoryArgs[key];
			} else {
				factoryArgs[key] = converted;
			}
		} else {
			// No param info, store as-is
			factoryArgs[key] = value;
		}

		updateNodeDataLive($selectedNode.id, { factoryArgs });
	}

	async function refreshFactoryPreview() {
		if (!$selectedNode || $selectedNode.data.nodeType !== 'factory') return;

		isRefreshing = true;
		try {
			await updateFactoryNodePreview($selectedNode.id);
		} catch (error) {
			console.error('Failed to refresh factory preview:', error);
		} finally {
			isRefreshing = false;
		}
	}

	async function resetFactoryArgsToDefaults() {
		if (!$selectedNode) return;
		pushHistory();
		// Clear all factory args so defaults are used
		updateNodeData($selectedNode.id, { factoryArgs: {} });
		await refreshFactoryPreview();
	}

	function selectDottedSegment(e: MouseEvent) {
		if (e.detail < 2) return;
		const input = e.target as HTMLInputElement;
		const val = input.value;
		// Determine which separators are present
		const hasPathSeps = val.includes('/') || val.includes('::');
		const hasDots = val.includes('.');
		if (!hasDots && !hasPathSeps) return;
		const isSep = (ch: string) => ch === '.' || ch === '/' || (ch === ':' && val.includes('::'));
		const pos = input.selectionStart ?? 0;
		let start = pos;
		let end = pos;
		while (start > 0 && !isSep(val[start - 1])) start--;
		while (end < val.length && !isSep(val[end])) end++;
		input.setSelectionRange(start, end);
		e.preventDefault();
	}

	function updateFactoryPath(event: Event) {
		const target = event.target as HTMLInputElement;
		if ($selectedNode) {
			updateNodeDataLive($selectedNode.id, { factory: target.value });
		}
	}

	// Fetch factory signature when factory path changes
	async function loadFactorySignature(factoryPath: string) {
		if (!factoryPath || factoryPath === lastFactoryPath) return;

		factorySignatureLoading = true;
		lastFactoryPath = factoryPath;

		try {
			const projectRoot = ($extraData as Record<string, unknown>)?.project_root as string | undefined;
			const response = await api.getFactorySignature(factoryPath, projectRoot);
			factoryParams = response.parameters;
		} catch (e) {
			console.warn('Failed to load factory signature:', e);
			factoryParams = [];
		} finally {
			factorySignatureLoading = false;
		}
	}

	// Watch for factory path changes
	$effect(() => {
		if ($selectedNode?.data.nodeType === 'factory' && $selectedNode.data.factory) {
			loadFactorySignature($selectedNode.data.factory);
		} else {
			factoryParams = [];
			lastFactoryPath = null;
		}
	});

	// Determine if a parameter type represents an import path (non-primitive)
	function isImportPathParam(param: FactoryParameter): boolean {
		if (!param.type) return false;
		const primitiveTypes = ['str', 'int', 'float', 'bool', 'None', 'NoneType'];
		// Check if it's a primitive type or optional primitive
		const typeStr = param.type.toLowerCase();
		return !primitiveTypes.some(t => typeStr === t || typeStr === `${t} | none` || typeStr === `none | ${t}`);
	}

	// Check if parameter is a boolean type
	function isBoolParam(param: FactoryParameter): boolean {
		if (!param.type) return false;
		const typeStr = param.type.toLowerCase();
		return typeStr === 'bool' || typeStr === 'bool | none' || typeStr === 'none | bool';
	}

	// Check if parameter is an integer type
	function isIntParam(param: FactoryParameter): boolean {
		if (!param.type) return false;
		const typeStr = param.type.toLowerCase();
		return typeStr === 'int' || typeStr === 'int | none' || typeStr === 'none | int';
	}

	// Check if parameter is a float type
	function isFloatParam(param: FactoryParameter): boolean {
		if (!param.type) return false;
		const typeStr = param.type.toLowerCase();
		return typeStr === 'float' || typeStr === 'float | none' || typeStr === 'none | float';
	}

	// Check if parameter has enum options
	function isEnumParam(param: FactoryParameter): boolean {
		return Array.isArray(param.enum_options) && param.enum_options.length > 0;
	}

	// Convert a string value to the appropriate type based on parameter type
	function convertFactoryArgValue(param: FactoryParameter, value: string | boolean): unknown {
		// If it's already a boolean (from checkbox), return as-is
		if (typeof value === 'boolean') return value;

		const strValue = value.trim();

		// Handle empty string - return undefined to clear the value
		if (strValue === '') return undefined;

		if (isBoolParam(param)) {
			// Convert string to boolean
			return strValue.toLowerCase() === 'true' || strValue === '1';
		}

		if (isIntParam(param)) {
			const parsed = parseInt(strValue, 10);
			return isNaN(parsed) ? strValue : parsed;
		}

		if (isFloatParam(param)) {
			const parsed = parseFloat(strValue);
			return isNaN(parsed) ? strValue : parsed;
		}

		// For strings and other types, keep as string
		return strValue;
	}

	// Get placeholder text for a parameter
	function getParamPlaceholder(param: FactoryParameter): string {
		if (isImportPathParam(param)) {
			return 'module.path.to.object';
		}
		if (param.has_default && param.default !== null) {
			return String(param.default);
		}
		return '';
	}

	// Get label hint for parameter type
	function getParamTypeHint(param: FactoryParameter): string {
		if (isImportPathParam(param)) {
			return '(import path)';
		}
		return param.type ? `(${param.type})` : '';
	}

	// Validate factory arguments against required parameters
	function validateFactoryArgs(
		params: FactoryParameter[],
		args: Record<string, unknown> | undefined
	): string[] {
		const errors: string[] = [];
		for (const param of params) {
			// Check if parameter is required (no default)
			if (!param.has_default) {
				const value = args?.[param.name];
				// Value is missing if undefined, null, or empty string
				if (value === undefined || value === null || value === '') {
					errors.push(`Required argument '${param.name}' is missing`);
				}
			}
		}
		return errors;
	}

	// Run validation when factory params or args change
	$effect(() => {
		if (!$selectedNode || $selectedNode.data.nodeType !== 'factory') return;
		if (factorySignatureLoading) return; // Wait for params to load

		// If we have factory params, validate against current args
		if (factoryParams.length > 0) {
			const errors = validateFactoryArgs(factoryParams, $selectedNode.data.factoryArgs);

			// Update node validation state if there are missing required args
			if (errors.length > 0) {
				// Only update if the validation state actually needs to change
				const currentErrors = $selectedNode.data.validationErrors || [];
				const newErrorSet = new Set(errors);
				const currentErrorSet = new Set(currentErrors);

				// Check if errors have changed
				const errorsChanged = errors.length !== currentErrors.length ||
					errors.some(e => !currentErrorSet.has(e));

				if (errorsChanged || $selectedNode.data.isValid !== false) {
					updateNodeDataLive($selectedNode.id, {
						isValid: false,
						validationErrors: errors,
					});
				}
			} else if ($selectedNode.data.isValid === false &&
				$selectedNode.data.validationErrors?.some(e => e.startsWith("Required argument '"))) {
				// Clear validation errors if all required args are now filled
				// (but only clear our own "Required argument" errors, not other validation errors)
				const remainingErrors = ($selectedNode.data.validationErrors || [])
					.filter(e => !e.startsWith("Required argument '"));

				updateNodeDataLive($selectedNode.id, {
					isValid: remainingErrors.length === 0,
					validationErrors: remainingErrors.length > 0 ? remainingErrors : undefined,
				});
			}
		}
	});
</script>

{#if $activeTab}
<aside class="sidebar" style="width: {sidebarWidth}px">
	<!-- Resize handle -->
	<div
		class="resize-handle"
		class:resizing={isResizing}
		onmousedown={startResize}
		role="separator"
		aria-orientation="vertical"
		tabindex="0"
	></div>

	<div class="sidebar-header">
		<h2>Properties</h2>
	</div>

	{#if $unregisteredFields.length > 0}
		<div class="unregistered-warning">
			<span class="warning-badge">DEV</span>
			Unregistered fields: {$unregisteredFields.join(', ')}
		</div>
	{/if}

	<div class="sidebar-content">
		{#if $selectedNode}
			<!-- General Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('general')}
				>
					<span class="section-title">General</span>
					<span class="section-toggle">{sectionsOpen.general ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.general}
					<div class="section-content">
						<div class="field">
							<label for="node-label">Name</label>
							<input
								id="node-label"
								type="text"
								value={localNodeName}
								oninput={updateLabel}
								onblur={commitNodeRename}
							/>
						</div>
						<div class="field">
							<label>Type</label>
							<div class="readonly-value">
								{#if $selectedNode.data.nodeType === 'factory'}
									Factory Node
								{:else if $selectedNode.data.nodeType === 'subgraph'}
									Subgraph Node
								{:else}
									Regular Node
								{/if}
							</div>
						</div>
						<button
							class="delete-node-btn"
							onclick={() => {
								if ($selectedNode) {
									deleteNodes([$selectedNode.id]);
									selectedNodeIds.set(new Set());
								}
							}}
						>
							Delete Node
						</button>
					</div>
				{/if}
			</section>

			<!-- Factory Section (only for factory nodes) -->
			{#if $selectedNode.data.nodeType === 'factory'}
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('factory')}
					>
						<span class="section-title">Factory</span>
						<span class="section-toggle">{sectionsOpen.factory ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.factory}
						<div class="section-content">
							<div class="field">
								<label for="factory-path">Factory Path</label>
								<input
									id="factory-path"
									type="text"
									value={$selectedNode.data.factory || ''}
									oninput={updateFactoryPath}
									onblur={() => { pushHistory(); refreshFactoryPreview(); }}
									onmousedown={selectDottedSegment}
									placeholder="module.path or ./file.py"
									class="mono"
								/>
							</div>
							{#if factorySignatureLoading}
								<div class="loading-hint">Loading parameters...</div>
							{:else if factoryParams.length > 0}
								<div class="field">
									<label>Arguments</label>
									<div class="factory-args">
										{#each factoryParams as param}
											<div class="factory-arg">
												<div class="arg-header">
													<span class="arg-key">{param.name}</span>
													<span class="arg-type">{getParamTypeHint(param)}</span>
													{#if !param.has_default}
														<span class="arg-required">*</span>
													{/if}
												</div>
												{#if isBoolParam(param)}
													{@const storedValue = $selectedNode.data.factoryArgs?.[param.name]}
													{@const effectiveValue = storedValue !== undefined ? storedValue === true : param.default === true}
													<label class="checkbox-label">
														<input
															type="checkbox"
															checked={effectiveValue}
															onchange={(e) => {
																updateFactoryArg(param.name, (e.target as HTMLInputElement).checked);
																pushHistory();
																refreshFactoryPreview();
															}}
														/>
														<span class="checkbox-text">{effectiveValue ? 'True' : 'False'}{storedValue === undefined ? ' (default)' : ''}</span>
													</label>
												{:else if isEnumParam(param)}
													<!-- Enum: dropdown -->
													<select
														value={String($selectedNode.data.factoryArgs?.[param.name] ?? param.default ?? '')}
														onchange={(e) => {
															updateFactoryArg(param.name, (e.target as HTMLSelectElement).value);
															pushHistory();
															refreshFactoryPreview();
														}}
													>
														{#if !param.has_default && !$selectedNode.data.factoryArgs?.[param.name]}
															<option value="" disabled selected>Select...</option>
														{/if}
														{#each param.enum_options as option}
															<option value={option}>{option}</option>
														{/each}
													</select>
												{:else if isIntParam(param) || isFloatParam(param)}
													<input
														type="number"
														step={isIntParam(param) ? '1' : 'any'}
														value={$selectedNode.data.factoryArgs?.[param.name] ?? ''}
														placeholder={getParamPlaceholder(param)}
														oninput={(e) => updateFactoryArg(param.name, (e.target as HTMLInputElement).value)}
														onblur={() => { pushHistory(); refreshFactoryPreview(); }}
														class:required-missing={!param.has_default && $selectedNode.data.factoryArgs?.[param.name] === undefined}
													/>
												{:else}
													<input
														type="text"
														value={String($selectedNode.data.factoryArgs?.[param.name] ?? '')}
														placeholder={getParamPlaceholder(param)}
														oninput={(e) => updateFactoryArg(param.name, (e.target as HTMLInputElement).value)}
														onblur={() => { pushHistory(); refreshFactoryPreview(); }}
														onmousedown={selectDottedSegment}
														class:import-path={isImportPathParam(param)}
														class:required-missing={!param.has_default && !$selectedNode.data.factoryArgs?.[param.name]}
													/>
												{/if}
											</div>
										{/each}
									</div>
								</div>
							{/if}
							<div class="factory-buttons">
								{#if factoryParams.length > 0}
									<button
										class="reset-defaults-btn"
										onclick={resetFactoryArgsToDefaults}
										disabled={isRefreshing || !$selectedNode.data.factory}
									>
										Reset to Defaults
									</button>
								{/if}
								<button
									class="refresh-btn"
									onclick={refreshFactoryPreview}
									disabled={isRefreshing || !$selectedNode.data.factory}
								>
									{isRefreshing ? 'Refreshing...' : 'Refresh'}
								</button>
							</div>
							{#if $selectedNode.data.isValid === false && $selectedNode.data.validationErrors}
								<div class="factory-errors">
									{#each $selectedNode.data.validationErrors as error}
										<div class="error-message">{error}</div>
									{/each}
								</div>
							{/if}
						</div>
					{/if}
				</section>
			{/if}

			<!-- Subgraph Section (only for subgraph nodes) -->
			{#if $selectedNode.data.nodeType === 'subgraph'}
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('subgraph')}
					>
						<span class="section-title">Subgraph</span>
						<span class="section-toggle">{sectionsOpen.subgraph ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.subgraph}
						<div class="section-content">
							<div class="field">
								<label>Source</label>
								<div class="readonly-value mono">
									{$selectedNode.data.source || 'Inline'}
								</div>
							</div>
							{#if $selectedNode.data.nodeCount !== undefined}
								<div class="field">
									<label>Node Count</label>
									<div class="readonly-value">
										{$selectedNode.data.nodeCount} node{$selectedNode.data.nodeCount !== 1 ? 's' : ''}
									</div>
								</div>
							{/if}
							<div class="subgraph-hint">
								Double-click the node to edit its contents
							</div>
						</div>
					{/if}
				</section>
			{/if}

			<!-- Input Ports Section (only for regular nodes) -->
			{#if $selectedNode.data.nodeType === 'regular'}
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('inPorts')}
					>
						<span class="section-title">Input Ports</span>
						<span class="section-toggle">{sectionsOpen.inPorts ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.inPorts}
						<div class="section-content">
							{#each $selectedNode.data.inPorts as port, i}
								<div class="port-editor">
									<div class="port-fields">
										<input
											type="text"
											value={port.name}
											placeholder="name"
											oninput={(e) => updatePortName('inPorts', i, (e.target as HTMLInputElement).value)}
										/>
										<input
											type="text"
											value={port.type || ''}
											placeholder="type"
											oninput={(e) => updatePortType('inPorts', i, (e.target as HTMLInputElement).value)}
										/>
									</div>
									<button
										class="remove-btn"
										onclick={() => removePort('inPorts', i)}
										title="Remove port"
									>
										&times;
									</button>
								</div>
							{/each}
							<button class="add-btn" onclick={() => addPort('inPorts')}>
								+ Add Input Port
							</button>
						</div>
					{/if}
				</section>

				<!-- Output Ports Section -->
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('outPorts')}
					>
						<span class="section-title">Output Ports</span>
						<span class="section-toggle">{sectionsOpen.outPorts ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.outPorts}
						<div class="section-content">
							{#each $selectedNode.data.outPorts as port, i}
								<div class="port-editor">
									<div class="port-fields">
										<input
											type="text"
											value={port.name}
											placeholder="name"
											oninput={(e) => updatePortName('outPorts', i, (e.target as HTMLInputElement).value)}
										/>
										<input
											type="text"
											value={port.type || ''}
											placeholder="type"
											oninput={(e) => updatePortType('outPorts', i, (e.target as HTMLInputElement).value)}
										/>
									</div>
									<button
										class="remove-btn"
										onclick={() => removePort('outPorts', i)}
										title="Remove port"
									>
										&times;
									</button>
								</div>
							{/each}
							<button class="add-btn" onclick={() => addPort('outPorts')}>
								+ Add Output Port
							</button>
						</div>
					{/if}
				</section>
			{/if}

			<!-- Salvo Conditions Section (only for regular nodes, not factory or subgraph) -->
			{#if $selectedNode.data.nodeType === 'regular'}
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('salvoConditions')}
					>
						<span class="section-title">Salvo Conditions</span>
						<span class="section-toggle">{sectionsOpen.salvoConditions ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.salvoConditions}
						{@const inConditionsRaw = getNodeSalvoConditions($selectedNode, 'in')}
						{@const outConditionsRaw = getNodeSalvoConditions($selectedNode, 'out')}
						{@const inConditions = inConditionsRaw ? parseSalvoConditionsFromJSON(inConditionsRaw) : null}
						{@const outConditions = outConditionsRaw ? parseSalvoConditionsFromJSON(outConditionsRaw) : null}
						<div class="section-content">
							<SalvoConditionsSection
								{inConditions}
								{outConditions}
								inPortNames={$selectedNode.data.inPorts.map(p => p.name)}
								outPortNames={$selectedNode.data.outPorts.map(p => p.name)}
								onUpdateIn={(conditions) => {
									const json = conditions ? salvoConditionsToJSON(conditions) : null;
									updateNodeSalvoConditions($selectedNode.id, 'in', json);
								}}
								onUpdateOut={(conditions) => {
									const json = conditions ? salvoConditionsToJSON(conditions) : null;
									updateNodeSalvoConditions($selectedNode.id, 'out', json);
								}}
							/>
						</div>
					{/if}
				</section>
			{/if}

			<!-- Execution Section (for regular and factory nodes, not subgraph) -->
			{#if $selectedNode.data.nodeType !== 'subgraph'}
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('execution')}
					>
						<span class="section-title">Execution</span>
						<span class="section-toggle">{sectionsOpen.execution ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.execution}
						{@const executionConfig = getNodeExecutionConfig($selectedNode)}
						{@const poolsData = ($extraData as Record<string, unknown>)?.pools as Record<string, unknown> | null | undefined}
						{@const availablePools = poolsData ? Object.keys(poolsData) : ['main']}
						<div class="section-content">
							<NodeExecutionSection
								{executionConfig}
								{availablePools}
								isFactory={$selectedNode.data.nodeType === 'factory'}
								onUpdate={(config) => {
									updateNodeExecutionConfig($selectedNode.id, config);
									pushHistory();
								}}
							/>
						</div>
					{/if}
				</section>
			{/if}

			<!-- Node Variables Section (for regular and factory nodes, not subgraph) -->
			{#if $selectedNode.data.nodeType !== 'subgraph'}
				<section class="section">
					<button
						class="section-header"
						onclick={() => toggleSection('nodeVariables')}
					>
						<span class="section-title">Node Variables</span>
						<span class="section-toggle">{sectionsOpen.nodeVariables ? '−' : '+'}</span>
					</button>
					{#if sectionsOpen.nodeVariables}
						<div class="section-content">
							<NodeVariablesSection
								variables={$nodeNodeVars}
								inheritedVariables={$projectNodeVars}
								level="node"
								onUpdate={(vars) => {
									updateNodeNodeVars($selectedNode.id, vars);
									pushHistory();
								}}
							/>
						</div>
					{/if}
				</section>
			{/if}

			<!-- Actions Panel (for all node types) -->
			<ActionsPanel
				onOpenSettings={() => showProjectSettings = true}
				onAddAction={() => { editingAction = null; editingNodeAction = true; showActionEditor = true; }}
				onEditAction={(action) => { editingAction = action; editingNodeAction = !$projectActions.some(a => a.id === action.id); showActionEditor = true; }}
			/>
		{:else if $activeTab}
			<!-- Net-level settings when no node is selected -->

			<!-- Graph Settings Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('graphSettings')}
				>
					<span class="section-title">Graph Settings</span>
					<span class="section-toggle">{sectionsOpen.graphSettings ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.graphSettings}
					<div class="section-content">
						<div class="field">
							<label for="graph-name">Name</label>
							<input
								id="graph-name"
								type="text"
								value={($graphExtra as Record<string, unknown>)?.name ?? ''}
								oninput={(e) => updateGraphExtraLive({ name: (e.target as HTMLInputElement).value })}
								onblur={() => pushHistory()}
								placeholder="Graph name"
							/>
						</div>
						<div class="field">
							<label for="graph-description">Description</label>
							<textarea
								id="graph-description"
								value={String(($graphExtra as Record<string, unknown>)?.description ?? '')}
								oninput={(e) => updateGraphExtraLive({ description: (e.target as HTMLTextAreaElement).value })}
								onblur={() => pushHistory()}
								placeholder="Graph description"
								rows="3"
							></textarea>
						</div>
						<div class="field">
							<label for="project-root">Project Root</label>
							<input
								id="project-root"
								type="text"
								value={String(($extraData as Record<string, unknown>)?.project_root ?? '')}
								oninput={(e) => updateExtraDataLive({ project_root: (e.target as HTMLInputElement).value || undefined })}
								onblur={() => pushHistory()}
								placeholder="./ (relative to net file)"
								class="mono"
							/>
							<div class="field-hint">
								Base path for file references. Relative paths resolve from the net file location.
							</div>
						</div>
					</div>
				{/if}
			</section>

			<!-- Pools Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('pools')}
				>
					<span class="section-title">Pools</span>
					<span class="section-toggle">{sectionsOpen.pools ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.pools}
					<div class="section-content">
						<PoolsSection
							pools={($extraData as Record<string, unknown>)?.pools as Record<string, unknown> | null | undefined}
							onUpdate={(pools) => {
								updateExtraDataLive({ pools });
							}}
						/>
					</div>
				{/if}
			</section>

			<!-- Net Settings Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('netSettings')}
				>
					<span class="section-title">Net Settings</span>
					<span class="section-toggle">{sectionsOpen.netSettings ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.netSettings}
					<div class="section-content">
						<NetSettingsSection
							extraData={$extraData as Record<string, unknown> | null}
							onUpdate={(updates) => {
								updateExtraDataLive(updates);
							}}
						/>
					</div>
				{/if}
			</section>

			<!-- Net-level Node Variables Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('netNodeVariables')}
				>
					<span class="section-title">Global Node Variables</span>
					<span class="section-toggle">{sectionsOpen.netNodeVariables ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.netNodeVariables}
					<div class="section-content">
						<NodeVariablesSection
							variables={$projectNodeVars}
							level="net"
							onUpdate={(vars) => {
								updateProjectNodeVars(vars);
								pushHistory();
							}}
						/>
					</div>
				{/if}
			</section>

			<!-- All Node Variables Overview -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('allNodeVariables')}
				>
					<span class="section-title">Local Node Variables</span>
					<span class="section-toggle">{sectionsOpen.allNodeVariables ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.allNodeVariables}
					<div class="section-content">
						<AllNodeVariablesSection allNodesVars={$allNodesVars} />
					</div>
				{/if}
			</section>

				<!-- Default Factories Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('factories')}
				>
					<span class="section-title">Default Factories</span>
					<span class="section-toggle">{sectionsOpen.factories ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.factories}
					{@const factories = (($extraData as Record<string, unknown>)?.factories as string[]) || []}
					<div class="section-content">
						{#if factories.length > 0}
							{#each factories as factory, index}
								<div class="factory-item">
									<span class="factory-path" title={factory}>
										{factory.includes('/') || factory.includes('\\')
											? (factory.split('/').pop()?.replace('.py', '') || factory)
											: (factory.split('.').pop() || factory)}
									</span>
									<span class="factory-full-path">{factory}</span>
									<button
										class="remove-btn"
										onclick={() => {
											const newFactories = factories.filter((_, i) => i !== index);
											updateExtraDataLive({ factories: newFactories });
											pushHistory();
										}}
										title="Remove factory"
									>
										&times;
									</button>
								</div>
							{/each}
						{:else}
							<p class="empty-hint">No default factories configured</p>
						{/if}
						<button
							class="add-btn"
							onclick={() => showAddFactoryModal = true}
						>
							+ Add Factory
						</button>
					</div>
				{/if}
			</section>

			<!-- UI Settings Section -->
			<section class="section">
				<button
					class="section-header"
					onclick={() => toggleSection('uiSettings')}
				>
					<span class="section-title">UI Settings</span>
					<span class="section-toggle">{sectionsOpen.uiSettings ? '−' : '+'}</span>
				</button>
				{#if sectionsOpen.uiSettings}
					{@const uiMeta = (($graphExtra as Record<string, unknown>)?.ui || {}) as Record<string, unknown>}
					<div class="section-content">
						<div class="field">
							<label for="edge-style">Edge Style</label>
							<select
								id="edge-style"
								value={uiMeta.edgeStyle ?? 'smoothstep'}
								onchange={(e) => {
									updateGraphExtraLive({
										ui: { ...uiMeta, edgeStyle: (e.target as HTMLSelectElement).value }
									});
									pushHistory();
								}}
							>
								<option value="smoothstep">Smooth Step</option>
								<option value="straight">Straight</option>
								<option value="step">Step</option>
								<option value="default">Bezier</option>
							</select>
						</div>
						<div class="field">
							<label for="edge-markers">Edge Markers</label>
							<select
								id="edge-markers"
								value={uiMeta.edgeMarkers ?? 'arrow-end'}
								onchange={(e) => {
									updateGraphExtraLive({
										ui: { ...uiMeta, edgeMarkers: (e.target as HTMLSelectElement).value }
									});
									pushHistory();
								}}
							>
								<option value="arrow-end">Arrow (end)</option>
								<option value="arrow-start">Arrow (start)</option>
								<option value="arrow-both">Arrow (both ends)</option>
								<option value="none">None</option>
							</select>
						</div>
					</div>
				{/if}
			</section>

			<!-- Actions bottom panel (matches node-level ActionsPanel) -->
			<div class="actions-footer">
				<div class="actions-footer-header">
					<span class="actions-footer-title">Default Actions</span>
					<button
						class="actions-footer-gear"
						onclick={() => showProjectSettings = true}
						title="Action Settings"
					>
						<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
							<circle cx="12" cy="12" r="3"/>
							<path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/>
						</svg>
					</button>
				</div>

				<div class="actions-footer-list">
					{#if $projectActions.length === 0}
						<div class="actions-footer-empty">
							No actions defined.
							<button class="actions-footer-link" onclick={() => showProjectSettings = true}>
								Configure in settings
							</button>
						</div>
					{:else}
						{#each $projectActions as action (action.id)}
							<div class="actions-footer-item">
								<span class="actions-footer-item-label">{action.label}</span>
								<button
									class="actions-footer-edit-btn"
									onclick={() => { editingAction = action; editingNodeAction = false; showActionEditor = true; }}
									title="Edit action"
								>
									<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
										<path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
									</svg>
								</button>
							</div>
						{/each}
					{/if}
				</div>

				<button
					class="actions-footer-add-btn"
					onclick={() => { editingAction = null; editingNodeAction = false; showActionEditor = true; }}
				>
					<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<line x1="12" y1="5" x2="12" y2="19"/>
						<line x1="5" y1="12" x2="19" y2="12"/>
					</svg>
					Add Action
				</button>
			</div>
		{/if}
	</div>
</aside>
{/if}

<!-- Modals -->
{#if showProjectSettings}
	<ProjectSettings onClose={() => showProjectSettings = false} />
{/if}

{#if showActionEditor}
	<ActionEditor
		action={editingAction}
		onSave={handleSaveAction}
		onCancel={() => { showActionEditor = false; editingAction = null; }}
		onDelete={editingAction ? handleDeleteAction : undefined}
	/>
{/if}

{#if showAddFactoryModal}
	<TextInputModal
		title="Add Default Factory"
		label="Factory Import Path"
		placeholder="module.path or ./factory.py"
		submitLabel="Add"
		onSubmit={(factoryPath) => {
			const factories = (($extraData as Record<string, unknown>)?.factories as string[]) || [];
			updateExtraDataLive({
				factories: [...factories, factoryPath]
			});
			pushHistory();
			showAddFactoryModal = false;
		}}
		onCancel={() => showAddFactoryModal = false}
	/>
{/if}

<style>
	.sidebar {
		position: relative;
		height: 100%;
		background: var(--bg-secondary, #242424);
		border-left: 1px solid var(--border-color, #404040);
		display: flex;
		flex-direction: column;
		overflow: hidden;
		flex-shrink: 0;
	}

	.resize-handle {
		position: absolute;
		left: 0;
		top: 0;
		bottom: 0;
		width: 4px;
		cursor: ew-resize;
		background: transparent;
		z-index: 10;
		transition: background-color 0.15s ease;
	}

	.resize-handle:hover,
	.resize-handle.resizing {
		background: var(--accent-color, #3b82f6);
	}

	.sidebar-header {
		padding: 16px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.sidebar-header h2 {
		font-size: 14px;
		font-weight: 600;
		color: var(--text-primary, #fff);
		margin: 0;
	}

	/* Net-level actions footer (matches ActionsPanel) */
	.actions-footer {
		border-top: 1px solid var(--border-color, #404040);
		padding: 12px;
	}

	.actions-footer-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
	}

	.actions-footer-title {
		font-size: 11px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.5px;
		color: var(--text-secondary, #a0a0a0);
	}

	.actions-footer-gear {
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

	.actions-footer-gear:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.actions-footer-list {
		display: flex;
		flex-direction: column;
		gap: 4px;
		margin-top: 12px;
	}

	.actions-footer-empty {
		font-size: 12px;
		color: var(--text-secondary, #666);
		text-align: center;
		padding: 16px 8px;
	}

	.actions-footer-link {
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

	.actions-footer-link:hover {
		color: var(--accent-hover, #2563eb);
	}

	.actions-footer-item {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.actions-footer-item-label {
		flex: 1;
		padding: 8px 10px;
		background: var(--bg-tertiary, #2d2d2d);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.actions-footer-edit-btn {
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

	.actions-footer-item:hover .actions-footer-edit-btn {
		opacity: 1;
	}

	.actions-footer-edit-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.actions-footer-add-btn {
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

	.actions-footer-add-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.unregistered-warning {
		padding: 6px 12px;
		background: rgba(239, 146, 68, 0.15);
		border-bottom: 1px solid rgba(239, 146, 68, 0.3);
		color: #ef9244;
		font-size: 10px;
		line-height: 1.4;
		word-break: break-all;
	}

	.warning-badge {
		display: inline-block;
		padding: 1px 4px;
		background: #ef9244;
		color: #000;
		font-size: 9px;
		font-weight: 700;
		border-radius: 2px;
		margin-right: 4px;
		vertical-align: middle;
	}

	.sidebar-content {
		flex: 1;
		overflow-y: auto;
		padding: 8px;
	}

	.section {
		margin-bottom: 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border-radius: 6px;
		overflow: hidden;
	}

	.section-header {
		width: 100%;
		padding: 10px 12px;
		display: flex;
		justify-content: space-between;
		align-items: center;
		background: transparent;
		border: none;
		cursor: pointer;
		text-align: left;
	}

	.section-header:hover {
		background: var(--border-color, #404040);
	}

	.section-title {
		font-weight: 500;
		font-size: 12px;
		color: var(--text-primary, #fff);
	}

	.section-toggle {
		color: var(--text-secondary, #a0a0a0);
		font-size: 14px;
	}

	.section-content {
		padding: 12px;
		border-top: 1px solid var(--border-color, #404040);
	}

	.field {
		margin-bottom: 12px;
	}

	.field:last-child {
		margin-bottom: 0;
	}

	.field label {
		display: block;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 4px;
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.field input {
		width: 100%;
	}

	.readonly-value {
		color: var(--text-primary, #fff);
		font-size: 13px;
		padding: 6px 0;
	}

	.readonly-value.mono {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}

	.port-editor {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.port-fields {
		flex: 1;
		min-width: 0;
		display: flex;
		gap: 4px;
	}

	.port-fields input {
		flex: 1;
		min-width: 0;
	}

	.remove-btn {
		background: transparent;
		color: var(--text-secondary, #a0a0a0);
		padding: 4px 8px;
		font-size: 16px;
		line-height: 1;
		flex-shrink: 0;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
		background: transparent;
	}

	.add-btn {
		width: 100%;
		padding: 8px;
		font-size: 12px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		color: var(--text-secondary, #a0a0a0);
	}

	.add-btn:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
		background: transparent;
	}

	.factory-args {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.factory-arg {
		display: flex;
		flex-direction: column;
		gap: 4px;
	}

	.arg-header {
		display: flex;
		align-items: center;
		gap: 6px;
	}

	.arg-key {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 12px;
		font-weight: 500;
		color: var(--text-primary, #fff);
	}

	.arg-type {
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
	}

	.arg-required {
		color: #ef4444;
		font-weight: bold;
	}

	.factory-arg input,
	.factory-arg select {
		width: 100%;
	}

	.factory-arg input.import-path {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 12px;
	}

	.factory-arg input.required-missing {
		border-color: var(--error-color, #ef4444);
		background: rgba(239, 68, 68, 0.05);
	}

	.factory-arg input.required-missing:focus {
		border-color: var(--error-color, #ef4444);
		box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.2);
	}

	.factory-arg .checkbox-label {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 6px 0;
		cursor: pointer;
	}

	.factory-arg .checkbox-label input[type="checkbox"] {
		width: 16px;
		height: 16px;
		margin: 0;
		cursor: pointer;
		accent-color: var(--accent-color, #3b82f6);
	}

	.factory-arg .checkbox-text {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
	}

	.factory-arg input[type="number"] {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 12px;
	}

	.factory-arg select {
		padding: 8px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		cursor: pointer;
	}

	.factory-arg select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.loading-hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		padding: 8px 0;
	}

	.factory-buttons {
		display: flex;
		gap: 8px;
		margin-top: 12px;
	}

	.factory-buttons button {
		flex: 1;
		padding: 8px;
		font-size: 12px;
		border: none;
		border-radius: 4px;
		cursor: pointer;
	}

	.reset-defaults-btn {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-secondary, #a0a0a0);
	}

	.reset-defaults-btn:hover:not(:disabled) {
		background: var(--bg-hover, #3d3d3d);
		color: var(--text-primary, #fff);
	}

	.reset-defaults-btn:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.refresh-btn {
		background: var(--accent-color, #3b82f6);
		color: white;
	}

	.refresh-btn:hover:not(:disabled) {
		background: var(--accent-hover, #2563eb);
	}

	.refresh-btn:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.factory-errors {
		margin-top: 8px;
		padding: 8px;
		background: rgba(239, 68, 68, 0.1);
		border-radius: 4px;
	}

	.error-message {
		color: var(--error-color, #ef4444);
		font-size: 11px;
	}

	.subgraph-hint {
		margin-top: 12px;
		padding: 8px;
		background: rgba(34, 197, 94, 0.1);
		border-radius: 4px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
		font-style: italic;
		text-align: center;
	}

	input.mono {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
	}

	/* Textarea styling */
	.field textarea {
		width: 100%;
		padding: 8px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 13px;
		font-family: inherit;
		resize: vertical;
	}

	.field textarea:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	/* Select styling */
	.field select {
		width: 100%;
		padding: 8px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 13px;
		cursor: pointer;
	}

	.field select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	/* Pool item styling */
	.pool-item {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		margin-bottom: 8px;
		overflow: hidden;
	}

	.pool-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 8px 10px;
		background: var(--bg-tertiary, #2d2d2d);
	}

	.pool-name {
		font-weight: 500;
		font-size: 12px;
		color: var(--text-primary, #fff);
	}

	.pool-details {
		padding: 8px 10px;
	}

	.pool-field {
		display: flex;
		gap: 8px;
		font-size: 11px;
		margin-bottom: 4px;
	}

	.pool-field:last-child {
		margin-bottom: 0;
	}

	.pool-key {
		color: var(--text-secondary, #a0a0a0);
		font-family: 'SF Mono', Monaco, Consolas, monospace;
	}

	.pool-value {
		color: var(--text-primary, #fff);
		font-family: 'SF Mono', Monaco, Consolas, monospace;
	}

	/* Factory item styling */
	.factory-item {
		display: flex;
		flex-direction: column;
		position: relative;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		margin-bottom: 6px;
		padding: 8px 10px;
		padding-right: 30px;
	}

	.factory-item .factory-path {
		font-weight: 500;
		font-size: 12px;
		color: var(--text-primary, #fff);
	}

	.factory-item .factory-full-path {
		font-size: 10px;
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		color: var(--text-secondary, #a0a0a0);
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.factory-item .remove-btn {
		position: absolute;
		top: 50%;
		right: 6px;
		transform: translateY(-50%);
	}

	.empty-hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		text-align: center;
		padding: 8px;
		margin: 0;
	}

	/* Delete node button */
	.delete-node-btn {
		width: 100%;
		margin-top: 12px;
		padding: 8px;
		font-size: 12px;
		background: transparent;
		border: 1px solid var(--error-color, #ef4444);
		border-radius: 4px;
		color: var(--error-color, #ef4444);
		cursor: pointer;
		transition: background-color 0.15s ease, color 0.15s ease;
	}

	.delete-node-btn:hover {
		background: var(--error-color, #ef4444);
		color: white;
	}

	/* Field hint for small descriptions */
	.field-hint {
		font-size: 10px;
		color: var(--text-secondary, #666);
		margin-top: 4px;
	}

	.field-hint code {
		background: var(--bg-primary, #1a1a1a);
		padding: 1px 3px;
		border-radius: 2px;
		font-size: 10px;
	}

</style>
