<script lang="ts">
	import {
		selectedNode,
		selectedNodeIds,
		updateNodeData,
		updateNodeDataLive,
		updateNodeEnv,
		updateNodeSalvoConditions,
		getNodeSalvoConditions,
		updateNodeExecutionConfig,
		getNodeExecutionConfig,
		updateFactoryNodePreview,
		deleteNodes,
		pushHistory,
		extraData,
		graphMeta,
		updateExtraDataLive,
		updateGraphMetaLive,
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
		addProjectAction,
		updateProjectAction,
		removeProjectAction,
		generateActionId,
		type Action,
	} from '$lib/stores/actionsStore';

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

	// Modal state for factory creation
	let showAddFactoryModal = $state(false);

	// Handle action editor save
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

	// Collapsible sections state
	let sectionsOpen = $state({
		general: true,
		inPorts: true,
		outPorts: true,
		factory: true,
		subgraph: true,
		nodeVariables: false,
		salvoConditions: false,
		execution: false,
		// Net-level sections
		graphSettings: true,
		pools: true,
		netSettings: false,
		factories: true,
		uiSettings: false,
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

	// Node-level env vars - extract from selected node
	function getNodeEnvVars(): Array<{ key: string; value: string }> {
		if (!$selectedNode) return [];
		const config = $selectedNode.data._config as Record<string, unknown> | undefined;
		const meta = config?.meta as Record<string, unknown> | undefined;
		const ui = meta?.ui as Record<string, unknown> | undefined;
		const env = ui?.env as Record<string, string> | undefined;
		if (!env) return [];
		return Object.entries(env).map(([key, value]) => ({ key, value }));
	}

	let nodeEnvVars = $state<Array<{ key: string; value: string }>>(getNodeEnvVars());

	// Update nodeEnvVars when selected node changes
	$effect(() => {
		if ($selectedNode) {
			nodeEnvVars = getNodeEnvVars();
		} else {
			nodeEnvVars = [];
		}
	});

	function saveNodeEnvVars() {
		if (!$selectedNode) return;
		const env: Record<string, string> = {};
		for (const { key, value } of nodeEnvVars) {
			if (key.trim()) {
				env[key.trim()] = value;
			}
		}
		updateNodeEnv($selectedNode.id, Object.keys(env).length > 0 ? env : undefined);
		pushHistory();
	}

	function addNodeEnvVar() {
		nodeEnvVars = [...nodeEnvVars, { key: '', value: '' }];
	}

	function removeNodeEnvVar(index: number) {
		nodeEnvVars = nodeEnvVars.filter((_, i) => i !== index);
		saveNodeEnvVars();
	}

	function toggleSection(section: keyof typeof sectionsOpen) {
		sectionsOpen[section] = !sectionsOpen[section];
	}

	// Update handlers - use "Live" version for typing, push history on blur
	function updateLabel(event: Event) {
		const target = event.target as HTMLInputElement;
		if ($selectedNode) {
			updateNodeDataLive($selectedNode.id, { label: target.value });
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

	function updateFactoryArg(key: string, value: string) {
		if (!$selectedNode) return;
		const factoryArgs: Record<string, unknown> = { ...($selectedNode.data.factoryArgs || {}) };
		factoryArgs[key] = value;
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
			const response = await api.getFactorySignature(factoryPath);
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
								value={$selectedNode.data.label}
								oninput={updateLabel}
								onblur={onFieldBlur}
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
						<div class="field">
							<label>ID</label>
							<div class="readonly-value mono">{$selectedNode.id}</div>
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
												<input
													type="text"
													value={String($selectedNode.data.factoryArgs?.[param.name] ?? '')}
													placeholder={getParamPlaceholder(param)}
													oninput={(e) => updateFactoryArg(param.name, (e.target as HTMLInputElement).value)}
													onblur={() => { pushHistory(); refreshFactoryPreview(); }}
													class:import-path={isImportPathParam(param)}
													class:required-missing={!param.has_default && !$selectedNode.data.factoryArgs?.[param.name]}
												/>
											</div>
										{/each}
									</div>
								</div>
							{/if}
							<button
								class="refresh-btn"
								onclick={refreshFactoryPreview}
								disabled={isRefreshing || !$selectedNode.data.factory}
							>
								{isRefreshing ? 'Refreshing...' : 'Refresh'}
							</button>
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

			<!-- Node Variables Section (for variable overrides) -->
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
						<div class="node-env-list">
							{#if nodeEnvVars.length === 0}
								<div class="empty-hint" style="text-align: left; padding: 0 0 8px 0;">
									Override project variables for this node
								</div>
							{:else}
								{#each nodeEnvVars as envVar, index (index)}
									<div class="env-row">
										<input
											type="text"
											bind:value={envVar.key}
											onblur={saveNodeEnvVars}
											placeholder="VAR_NAME"
											class="env-key"
										/>
										<span class="env-equals">=</span>
										<input
											type="text"
											bind:value={envVar.value}
											onblur={saveNodeEnvVars}
											placeholder="value"
											class="env-value"
										/>
										<button
											class="remove-btn"
											onclick={() => removeNodeEnvVar(index)}
											title="Remove"
										>
											×
										</button>
									</div>
								{/each}
							{/if}
						</div>
						<button class="add-btn" onclick={addNodeEnvVar}>
							+ Add Variable
						</button>
					</div>
				{/if}
			</section>

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
								onUpdate={(config) => {
									updateNodeExecutionConfig($selectedNode.id, config);
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
				onAddAction={() => { editingAction = null; showActionEditor = true; }}
				onEditAction={(action) => { editingAction = action; showActionEditor = true; }}
			/>
		{:else}
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
								value={($graphMeta as Record<string, unknown>)?.name ?? ''}
								oninput={(e) => updateGraphMetaLive({ name: (e.target as HTMLInputElement).value })}
								onblur={() => pushHistory()}
								placeholder="Graph name"
							/>
						</div>
						<div class="field">
							<label for="graph-description">Description</label>
							<textarea
								id="graph-description"
								value={String(($graphMeta as Record<string, unknown>)?.description ?? '')}
								oninput={(e) => updateGraphMetaLive({ description: (e.target as HTMLTextAreaElement).value })}
								onblur={() => pushHistory()}
								placeholder="Graph description"
								rows="3"
							></textarea>
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
										{factory.split('.').pop() || factory}
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
					{@const uiMeta = (($graphMeta as Record<string, unknown>)?.ui || {}) as Record<string, unknown>}
					<div class="section-content">
						<div class="field">
							<label for="edge-style">Edge Style</label>
							<select
								id="edge-style"
								value={uiMeta.edgeStyle ?? 'smoothstep'}
								onchange={(e) => {
									updateGraphMetaLive({
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
									updateGraphMetaLive({
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
		{/if}
	</div>
</aside>

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
		placeholder="mymodule.factories.create_node"
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

	.factory-arg input {
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

	.loading-hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		padding: 8px 0;
	}

	.refresh-btn {
		width: 100%;
		margin-top: 12px;
		padding: 8px;
		font-size: 12px;
		background: var(--accent-color, #3b82f6);
		color: white;
		border: none;
		border-radius: 4px;
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

	/* Node environment variables */
	.node-env-list {
		display: flex;
		flex-direction: column;
		gap: 6px;
		margin-bottom: 8px;
	}

	.env-row {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.env-row input {
		padding: 6px 8px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 11px;
		font-family: 'SF Mono', Monaco, monospace;
	}

	.env-row input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.env-key {
		width: 80px;
		flex-shrink: 0;
	}

	.env-equals {
		color: var(--text-secondary, #666);
		font-family: 'SF Mono', Monaco, monospace;
		font-size: 11px;
	}

	.env-value {
		flex: 1;
		min-width: 0;
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
</style>
