<script lang="ts">
	import {
		getNodeShape,
		getNodeColors,
		getNodeVisibility,
		batchUpdateNodeShape,
		batchUpdateNodeColor,
		batchUpdateNodeVisibility,
		NODE_SHAPES,
		type FlowNode,
		type NodeShape,
	} from '$lib/stores/flowStore';

	interface Props {
		nodes: FlowNode[];
	}

	let { nodes }: Props = $props();

	let nodeIds = $derived(nodes.map(n => n.id));

	// Intersection logic: compute shared values across all selected nodes
	let sharedShape = $derived.by(() => {
		const shapes = nodes.map(n => getNodeShape(n.data));
		return shapes.every(s => s === shapes[0]) ? shapes[0] : null;
	});

	let sharedHeaderColor = $derived.by(() => {
		const colors = nodes.map(n => getNodeColors(n.data).headerColor);
		return colors.every(c => c === colors[0]) ? colors[0] : undefined; // undefined = mixed
	});

	let sharedFontColor = $derived.by(() => {
		const colors = nodes.map(n => getNodeColors(n.data).fontColor);
		return colors.every(c => c === colors[0]) ? colors[0] : undefined;
	});

	let sharedHideLabel = $derived.by(() => {
		const vals = nodes.map(n => getNodeVisibility(n.data).hideLabel);
		return vals.every(v => v === vals[0]) ? vals[0] : null; // null = mixed
	});

	let sharedHideDescription = $derived.by(() => {
		const vals = nodes.map(n => getNodeVisibility(n.data).hideDescription);
		return vals.every(v => v === vals[0]) ? vals[0] : null;
	});

	let sharedHidePortNames = $derived.by(() => {
		const vals = nodes.map(n => getNodeVisibility(n.data).hidePortNames);
		return vals.every(v => v === vals[0]) ? vals[0] : null;
	});

	// Whether any node has a custom color set (for showing clear button)
	let anyHasHeaderColor = $derived(nodes.some(n => getNodeColors(n.data).headerColor !== null));
	let anyHasFontColor = $derived(nodes.some(n => getNodeColors(n.data).fontColor !== null));

	// Svelte action for indeterminate checkbox state
	function indeterminate(el: HTMLInputElement, value: boolean) {
		el.indeterminate = value;
		return {
			update(newValue: boolean) {
				el.indeterminate = newValue;
			}
		};
	}
</script>

<section class="section">
	<h3 class="section-title">{nodes.length} Nodes Selected</h3>

	<!-- Shape -->
	<div class="field">
		<label class="field-label">Shape</label>
		<select
			class="field-select"
			value={sharedShape ?? ''}
			onchange={(e) => {
				const val = (e.target as HTMLSelectElement).value as NodeShape;
				if (val) batchUpdateNodeShape(nodeIds, val);
			}}
		>
			{#if sharedShape === null}
				<option value="" disabled>(mixed)</option>
			{/if}
			{#each NODE_SHAPES as s}
				<option value={s.value}>{s.label}</option>
			{/each}
		</select>
	</div>

	<!-- Header Color -->
	<div class="field color-field">
		<label class="field-label">Header Color{#if sharedHeaderColor === undefined} <span class="mixed-label">(mixed)</span>{/if}</label>
		<div class="color-row">
			<input
				type="color"
				class="color-input"
				value={sharedHeaderColor ?? '#3d3d3d'}
				oninput={(e) => batchUpdateNodeColor(nodeIds, 'headerColor', (e.target as HTMLInputElement).value)}
			/>
			<span class="color-value">{sharedHeaderColor ?? 'mixed'}</span>
			{#if anyHasHeaderColor}
				<button
					class="reset-btn"
					onclick={() => batchUpdateNodeColor(nodeIds, 'headerColor', null)}
					title="Reset to default"
				>&times;</button>
			{/if}
		</div>
	</div>

	<!-- Font Color -->
	<div class="field color-field">
		<label class="field-label">Font Color{#if sharedFontColor === undefined} <span class="mixed-label">(mixed)</span>{/if}</label>
		<div class="color-row">
			<input
				type="color"
				class="color-input"
				value={sharedFontColor ?? '#ffffff'}
				oninput={(e) => batchUpdateNodeColor(nodeIds, 'fontColor', (e.target as HTMLInputElement).value)}
			/>
			<span class="color-value">{sharedFontColor ?? 'mixed'}</span>
			{#if anyHasFontColor}
				<button
					class="reset-btn"
					onclick={() => batchUpdateNodeColor(nodeIds, 'fontColor', null)}
					title="Reset to default"
				>&times;</button>
			{/if}
		</div>
	</div>

	<!-- Visibility toggles -->
	<div class="field">
		<label class="checkbox-label">
			<input
				type="checkbox"
				checked={sharedHideLabel === true}
				use:indeterminate={sharedHideLabel === null}
				onchange={(e) => batchUpdateNodeVisibility(nodeIds, 'hideLabel', (e.target as HTMLInputElement).checked)}
			/>
			Hide Label
		</label>
	</div>
	<div class="field">
		<label class="checkbox-label">
			<input
				type="checkbox"
				checked={sharedHideDescription === true}
				use:indeterminate={sharedHideDescription === null}
				onchange={(e) => batchUpdateNodeVisibility(nodeIds, 'hideDescription', (e.target as HTMLInputElement).checked)}
			/>
			Hide Description
		</label>
	</div>
	<div class="field">
		<label class="checkbox-label">
			<input
				type="checkbox"
				checked={sharedHidePortNames === true}
				use:indeterminate={sharedHidePortNames === null}
				onchange={(e) => batchUpdateNodeVisibility(nodeIds, 'hidePortNames', (e.target as HTMLInputElement).checked)}
			/>
			Hide Port Names
		</label>
	</div>
</section>

<style>
	.section {
		padding: 12px 16px;
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.section-title {
		font-size: 11px;
		font-weight: 600;
		text-transform: uppercase;
		color: var(--text-secondary, #a0a0a0);
		margin: 0 0 8px 0;
		letter-spacing: 0.5px;
	}

	.field {
		margin-bottom: 8px;
	}

	.field-label {
		display: block;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		margin-bottom: 4px;
	}

	.field-select {
		width: 100%;
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		font-family: inherit;
		box-sizing: border-box;
	}

	.field-select:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.color-row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.color-input {
		width: 32px;
		height: 24px;
		padding: 0;
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		background: none;
		cursor: pointer;
	}

	.color-input::-webkit-color-swatch-wrapper {
		padding: 2px;
	}

	.color-input::-webkit-color-swatch {
		border: none;
		border-radius: 2px;
	}

	.color-value {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		font-family: monospace;
	}

	.mixed-label {
		font-style: italic;
		color: var(--text-tertiary, #707070);
	}

	.reset-btn {
		padding: 2px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 10px;
		cursor: pointer;
	}

	.reset-btn:hover {
		background: var(--border-color, #404040);
		color: var(--text-primary, #fff);
	}

	.checkbox-label {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		color: var(--text-primary, #fff);
		cursor: pointer;
	}

	.checkbox-label input[type="checkbox"] {
		accent-color: var(--accent-color, #3b82f6);
	}
</style>
