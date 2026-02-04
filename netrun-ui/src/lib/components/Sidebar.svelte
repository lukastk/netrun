<script lang="ts">
	import {
		selectedNode,
		updateNodeData,
		updateNodeDataLive,
		updateFactoryNodePreview,
		pushHistory,
		extraData,
		graphMeta,
		updateExtraDataLive,
		updateGraphMetaLive,
		type NetrunNodeData,
		type PortConfig
	} from '$lib/stores/flowStore';
	import { api } from '$lib/api';
	import ActionsPanel from './ActionsPanel.svelte';
	import ProjectSettings from './ProjectSettings.svelte';
	import ActionEditor from './ActionEditor.svelte';
	import {
		addProjectAction,
		updateProjectAction,
		removeProjectAction,
		generateActionId,
		type Action,
	} from '$lib/stores/actionsStore';

	// Loading state for factory preview
	let isRefreshing = $state(false);

	// Modal state for actions
	let showProjectSettings = $state(false);
	let showActionEditor = $state(false);
	let editingAction = $state<Action | null>(null);

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
		execution: false,
		// Net-level sections
		graphSettings: true,
		pools: true,
		uiSettings: false,
	});

	function toggleSection(section: keyof typeof sectionsOpen) {
		sectionsOpen[section] = !sectionsOpen[section];
	}

	// Pool types for the dropdown
	const poolTypes = [
		{ value: 'ThreadPool', label: 'Thread Pool' },
		{ value: 'MultiprocessPool', label: 'Multiprocess Pool' },
		{ value: 'RemotePoolClient', label: 'Remote Pool' },
	];

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
</script>

<aside class="sidebar">
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
									class="mono"
								/>
							</div>
							{#if $selectedNode.data.factoryArgs && Object.keys($selectedNode.data.factoryArgs).length > 0}
								<div class="field">
									<label>Arguments</label>
									<div class="factory-args">
										{#each Object.entries($selectedNode.data.factoryArgs) as [key, value]}
											<div class="factory-arg">
												<span class="arg-key">{key}:</span>
												<input
													type="text"
													value={String(value)}
													oninput={(e) => updateFactoryArg(key, (e.target as HTMLInputElement).value)}
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
								{isRefreshing ? 'Refreshing...' : 'Refresh Preview'}
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
					{@const pools = ($extraData as Record<string, unknown>)?.pools as Record<string, unknown> | undefined}
					<div class="section-content">
						{#if pools && Object.keys(pools).length > 0}
							{#each Object.entries(pools) as [poolName, poolConfig]}
								<div class="pool-item">
									<div class="pool-header">
										<span class="pool-name">{poolName}</span>
										<button
											class="remove-btn"
											onclick={() => {
												const currentPools = { ...pools };
												delete currentPools[poolName];
												updateExtraDataLive({ pools: currentPools });
												pushHistory();
											}}
											title="Remove pool"
										>
											&times;
										</button>
									</div>
									<div class="pool-details">
										{#if typeof poolConfig === 'object' && poolConfig !== null}
											{#each Object.entries(poolConfig as Record<string, unknown>) as [key, value]}
												<div class="pool-field">
													<span class="pool-key">{key}:</span>
													<span class="pool-value">{JSON.stringify(value)}</span>
												</div>
											{/each}
										{/if}
									</div>
								</div>
							{/each}
						{:else}
							<p class="empty-hint">No pools configured</p>
						{/if}
						<button
							class="add-btn"
							onclick={() => {
								const name = prompt('Pool name:');
								if (name) {
									const currentPools = (pools || {}) as Record<string, unknown>;
									updateExtraDataLive({
										pools: {
											...currentPools,
											[name]: { type: 'ThreadPool', num_workers: 4 }
										}
									});
									pushHistory();
								}
							}}
						>
							+ Add Pool
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
						<p class="empty-hint">More settings coming soon</p>
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

<style>
	.sidebar {
		width: var(--sidebar-width, 300px);
		height: 100%;
		background: var(--bg-secondary, #242424);
		border-left: 1px solid var(--border-color, #404040);
		display: flex;
		flex-direction: column;
		overflow: hidden;
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
		gap: 8px;
	}

	.factory-arg {
		display: flex;
		gap: 8px;
		align-items: center;
	}

	.arg-key {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		min-width: 60px;
	}

	.factory-arg input {
		flex: 1;
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

	.empty-hint {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		text-align: center;
		padding: 8px;
		margin: 0;
	}
</style>
