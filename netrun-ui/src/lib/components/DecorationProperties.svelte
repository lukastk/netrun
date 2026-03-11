<script lang="ts">
	import {
		updateNodeDataLive,
		updateNodeDimensions,
		pushHistory,
		deleteNodes,
		getNodeLocked,
		updateNodeLocked,
		DECORATION_TYPES,
		type FlowNode,
		type DecorationNodeData,
		type DecorationType,
	} from '$lib/stores/flowStore';

	interface Props {
		node: FlowNode;
	}

	let { node }: Props = $props();

	let data = $derived(node.data as DecorationNodeData);

	// Property updates
	function updateProp(key: keyof DecorationNodeData, value: unknown) {
		updateNodeDataLive(node.id, { [key]: value } as Partial<DecorationNodeData>);
	}

	function updatePropWithHistory(key: keyof DecorationNodeData, value: unknown) {
		pushHistory();
		updateNodeDataLive(node.id, { [key]: value } as Partial<DecorationNodeData>);
	}

	function handleDelete() {
		deleteNodes([node.id]);
	}

	function resetColor(key: 'fillColor' | 'strokeColor' | 'fontColor') {
		pushHistory();
		updateNodeDataLive(node.id, { [key]: undefined } as Partial<DecorationNodeData>);
	}
</script>

<section class="section">
	<h3 class="section-title">Decoration</h3>

	<!-- Decoration Type -->
	<div class="field">
		<label class="field-label">Type</label>
		<select
			class="field-select"
			value={data.decorationType}
			onchange={(e) => updatePropWithHistory('decorationType', (e.target as HTMLSelectElement).value as DecorationType)}
		>
			{#each DECORATION_TYPES as dt}
				<option value={dt.value}>{dt.label}</option>
			{/each}
		</select>
	</div>

	<!-- Text -->
	<div class="field">
		<label class="field-label">Text</label>
		<textarea
			class="field-textarea"
			value={data.text || ''}
			oninput={(e) => updateProp('text', (e.target as HTMLTextAreaElement).value || undefined)}
			onchange={() => pushHistory()}
			onkeydown={(e) => e.stopPropagation()}
			placeholder="Optional text..."
			rows={data.decorationType === 'textbox' ? 5 : 2}
		></textarea>
	</div>

	<!-- Image Path (only for image type) -->
	{#if data.decorationType === 'image'}
		<div class="field">
			<label class="field-label">Image Path</label>
			<input
				type="text"
				class="field-input"
				value={data.imagePath || ''}
				oninput={(e) => updateProp('imagePath', (e.target as HTMLInputElement).value || undefined)}
				onchange={() => pushHistory()}
				onkeydown={(e) => e.stopPropagation()}
				placeholder="path/to/image.png"
			/>
		</div>
	{/if}

	<!-- Orientation (only for divider type) -->
	{#if data.decorationType === 'divider'}
		<div class="field">
			<label class="field-label">Orientation</label>
			<select
				class="field-select"
				value={data.orientation || 'horizontal'}
				onchange={(e) => {
					const newOrientation = (e.target as HTMLSelectElement).value as 'horizontal' | 'vertical';
					pushHistory();
					updateNodeDataLive(node.id, { orientation: newOrientation } as Partial<DecorationNodeData>);
					const w = node.width ?? 200;
					const h = node.height ?? 4;
					updateNodeDimensions([{ id: node.id, width: h, height: w, position: node.position }]);
				}}
			>
				<option value="horizontal">Horizontal</option>
				<option value="vertical">Vertical</option>
			</select>
		</div>
	{/if}
</section>

<section class="section">
	<h3 class="section-title">Appearance</h3>

	<!-- Fill Color -->
	{#if data.decorationType !== 'label' && data.decorationType !== 'divider'}
		<div class="field color-field">
			<label class="field-label">Fill Color</label>
			<div class="color-row">
				<input
					type="color"
					class="color-input"
					value={data.fillColor || '#374151'}
					onchange={(e) => updatePropWithHistory('fillColor', (e.target as HTMLInputElement).value)}
				/>
				<span class="color-value">{data.fillColor || 'none'}</span>
				{#if data.fillColor}
					<button class="reset-btn" onclick={() => resetColor('fillColor')} title="Reset">x</button>
				{/if}
			</div>
		</div>
	{/if}

	<!-- Stroke Color -->
	{#if data.decorationType !== 'label'}
		<div class="field color-field">
			<label class="field-label">Stroke Color</label>
			<div class="color-row">
				<input
					type="color"
					class="color-input"
					value={data.strokeColor || '#6b7280'}
					onchange={(e) => updatePropWithHistory('strokeColor', (e.target as HTMLInputElement).value)}
				/>
				<span class="color-value">{data.strokeColor || 'none'}</span>
				{#if data.strokeColor}
					<button class="reset-btn" onclick={() => resetColor('strokeColor')} title="Reset">x</button>
				{/if}
			</div>
		</div>

		<!-- Stroke Width -->
		<div class="field">
			<label class="field-label">Stroke Width</label>
			<input
				type="number"
				class="field-input narrow"
				value={data.strokeWidth ?? 1}
				min="0"
				max="20"
				step="1"
				onchange={(e) => updatePropWithHistory('strokeWidth', Number((e.target as HTMLInputElement).value))}
				onkeydown={(e) => e.stopPropagation()}
			/>
		</div>
	{/if}

	<!-- Font Size -->
	<div class="field">
		<label class="field-label">Font Size</label>
		<input
			type="number"
			class="field-input narrow"
			value={data.fontSize ?? 14}
			min="6"
			max="200"
			step="1"
			onchange={(e) => updatePropWithHistory('fontSize', Number((e.target as HTMLInputElement).value))}
			onkeydown={(e) => e.stopPropagation()}
		/>
	</div>

	<!-- Font Color -->
	<div class="field color-field">
		<label class="field-label">Font Color</label>
		<div class="color-row">
			<input
				type="color"
				class="color-input"
				value={data.fontColor || '#ffffff'}
				onchange={(e) => updatePropWithHistory('fontColor', (e.target as HTMLInputElement).value)}
			/>
			<span class="color-value">{data.fontColor || '#ffffff'}</span>
			{#if data.fontColor}
				<button class="reset-btn" onclick={() => resetColor('fontColor')} title="Reset">x</button>
			{/if}
		</div>
	</div>

	<!-- Opacity -->
	<div class="field">
		<label class="field-label">Opacity</label>
		<div class="slider-row">
			<input
				type="range"
				class="field-slider"
				min="0"
				max="1"
				step="0.05"
				value={data.opacity ?? 1}
				oninput={(e) => updateProp('opacity', Number((e.target as HTMLInputElement).value))}
				onchange={() => pushHistory()}
			/>
			<span class="slider-value">{((data.opacity ?? 1) * 100).toFixed(0)}%</span>
		</div>
	</div>
</section>

<section class="section">
	<div class="field">
		<label class="checkbox-row">
			<input
				type="checkbox"
				checked={getNodeLocked(data)}
				onchange={(e) => updateNodeLocked(node.id, (e.target as HTMLInputElement).checked)}
			/>
			<span>Lock position</span>
		</label>
	</div>
	<button class="delete-btn" onclick={handleDelete}>Delete Decoration</button>
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

	.field-input,
	.field-select,
	.field-textarea {
		width: 100%;
		padding: 6px 8px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		color: var(--text-primary, #fff);
		font-size: 12px;
		font-family: inherit;
		box-sizing: border-box;
	}

	.field-input.narrow {
		width: 80px;
	}

	.field-input:focus,
	.field-select:focus,
	.field-textarea:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.field-textarea {
		resize: vertical;
		min-height: 36px;
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

	.reset-btn {
		padding: 2px 6px;
		background: var(--bg-primary, #1a1a1a);
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

	.slider-row {
		display: flex;
		align-items: center;
		gap: 8px;
	}

	.field-slider {
		flex: 1;
		height: 4px;
		accent-color: var(--accent-color, #3b82f6);
	}

	.slider-value {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		min-width: 36px;
		text-align: right;
	}

	.checkbox-row {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
		color: var(--text-primary, #fff);
		cursor: pointer;
	}

	.delete-btn {
		width: 100%;
		padding: 8px;
		background: var(--error-color, #ef4444);
		border: none;
		border-radius: 4px;
		color: #fff;
		font-size: 12px;
		font-weight: 500;
		cursor: pointer;
		transition: background 0.15s ease;
	}

	.delete-btn:hover {
		background: #dc2626;
	}
</style>
