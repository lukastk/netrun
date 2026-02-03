<script lang="ts">
	import {
		selectedNode,
		updateNodeData,
		updateNodeDataLive,
		updateFactoryNodePreview,
		pushHistory,
		type NetrunNodeData,
		type PortConfig
	} from '$lib/stores/flowStore';
	import { api } from '$lib/api';

	// Loading state for factory preview
	let isRefreshing = $state(false);

	// Collapsible sections state
	let sectionsOpen = $state({
		general: true,
		inPorts: true,
		outPorts: true,
		factory: true,
		execution: false
	});

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
		const factoryArgs = { ...($selectedNode.data.factoryArgs || {}) };
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
								{$selectedNode.data.nodeType === 'factory' ? 'Factory Node' : 'Regular Node'}
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
		{:else}
			<div class="no-selection">
				<p>Select a node to edit its properties</p>
			</div>
		{/if}
	</div>
</aside>

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

	.no-selection {
		padding: 24px 16px;
		text-align: center;
		color: var(--text-secondary, #a0a0a0);
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

	input.mono {
		font-family: 'SF Mono', Monaco, Consolas, monospace;
		font-size: 11px;
	}
</style>
