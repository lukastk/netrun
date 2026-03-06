<script lang="ts">
	import {
		dependencyLabels,
		nodes,
		selectedNodeIds,
		selectedEdgeIds,
		updateNodeDataLive,
		pushHistory,
	} from '$lib/stores/flowStore';
	import { get } from 'svelte/store';
	import { svelteFlowRef } from '$lib/stores/svelteFlowStore';

	// Track editing state per label
	let editingLabel = $state<string | null>(null);
	let editValue = $state('');

	function startEditLabel(label: string) {
		editingLabel = label;
		editValue = label;
	}

	function commitLabelRename() {
		if (editingLabel === null) return;
		const oldLabel = editingLabel;
		const newLabel = editValue.trim();
		editingLabel = null;

		if (!newLabel || newLabel === oldLabel) return;

		// Update all nodes that have this label
		const allNodes = get(nodes);
		pushHistory();
		for (const node of allNodes) {
			const config = (node.data._config || {}) as Record<string, unknown>;
			const depReq = config.dependency_request as Record<string, unknown> | undefined;
			if (depReq && depReq.label === oldLabel) {
				updateNodeDataLive(node.id, {
					_config: {
						...config,
						dependency_request: { ...depReq, label: newLabel },
					},
				});
			}
		}
	}

	function cancelEdit() {
		editingLabel = null;
	}

	function selectNode(nodeName: string) {
		const allNodes = get(nodes);
		const node = allNodes.find(n => n.data.label === nodeName);
		if (!node) return;

		selectedNodeIds.set(new Set([node.id]));
		selectedEdgeIds.set(new Set());

		// Focus node in viewport
		const ref = get(svelteFlowRef);
		if (ref?.fitView) {
			ref.fitView({
				nodes: [{ id: node.id }],
				padding: 0.5,
				maxZoom: 1.5,
				duration: 300,
			});
		}
	}
</script>

<div class="labels-content">
	{#if $dependencyLabels.size === 0}
		<p class="empty-message">No dependency labels in this graph.</p>
	{:else}
		{#each [...$dependencyLabels.entries()] as [label, nodeNames]}
			<div class="label-group">
				<div class="label-header">
					{#if editingLabel === label}
						<input
							class="label-edit"
							type="text"
							bind:value={editValue}
							onblur={commitLabelRename}
							onkeydown={(e) => {
								if (e.key === 'Enter') commitLabelRename();
								if (e.key === 'Escape') cancelEdit();
							}}
							autofocus
						/>
					{:else}
						<!-- svelte-ignore a11y_no_static_element_interactions -->
						<span
							class="label-name"
							class:default-label={!label}
							ondblclick={() => startEditLabel(label)}
							title="Double-click to rename"
						>
							{label || '(default)'}
						</span>
					{/if}
					<span class="label-count">{nodeNames.length}</span>
				</div>
				<div class="label-nodes">
					{#each nodeNames as name}
						<button class="node-link" onclick={() => selectNode(name)}>
							{name}
						</button>
					{/each}
				</div>
			</div>
		{/each}
	{/if}
</div>

<style>
	.empty-message {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin: 0;
	}

	.label-group {
		margin-bottom: 8px;
	}

	.label-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 6px;
		margin-bottom: 4px;
	}

	.label-name {
		font-size: 12px;
		font-weight: 500;
		color: #a78bfa;
		cursor: pointer;
		font-family: monospace;
	}

	.label-name.default-label {
		color: var(--text-secondary, #a0a0a0);
		font-style: italic;
	}

	.label-edit {
		flex: 1;
		padding: 2px 6px;
		background: var(--bg-secondary, #242424);
		border: 1px solid #a78bfa;
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		font-family: monospace;
	}

	.label-count {
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		background: var(--bg-secondary, #242424);
		padding: 1px 5px;
		border-radius: 8px;
	}

	.label-nodes {
		display: flex;
		flex-wrap: wrap;
		gap: 4px;
		padding-left: 4px;
	}

	.node-link {
		background: none;
		border: none;
		color: var(--text-secondary, #a0a0a0);
		font-size: 11px;
		cursor: pointer;
		padding: 1px 4px;
		border-radius: 3px;
		font-family: monospace;
	}

	.node-link:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}
</style>
