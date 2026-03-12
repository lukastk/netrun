<script lang="ts">
	import { NodeResizer } from '@xyflow/svelte';
	import type { DecorationNodeData } from '../types/nodes.js';

	interface Props {
		id: string;
		data: DecorationNodeData;
		selected?: boolean;
		/** Resolved image URL (the consumer is responsible for resolving relative paths) */
		imageUrl?: string;
		/** Called when the node is resized */
		onResize?: (event: { nodeId: string; width: number; height: number; position: { x: number; y: number } }) => void;
	}

	let { id, data, selected = false, imageUrl = '', onResize }: Props = $props();

	function handleResizeEnd(
		_event: unknown,
		params: { width: number; height: number; x: number; y: number }
	) {
		onResize?.({ nodeId: id, width: params.width, height: params.height, position: { x: params.x, y: params.y } });
	}
</script>

<!-- No Handle components — prevents connections -->
<NodeResizer
	isVisible={selected}
	minWidth={20}
	minHeight={20}
	onResizeEnd={handleResizeEnd}
/>

<div
	class="decoration-node"
	class:selected
	class:type-rectangle={data.decorationType === 'rectangle'}
	class:type-rounded-rectangle={data.decorationType === 'rounded-rectangle'}
	class:type-circle={data.decorationType === 'circle'}
	class:type-triangle={data.decorationType === 'triangle'}
	class:type-divider={data.decorationType === 'divider'}
	class:divider-vertical={data.decorationType === 'divider' && data.orientation === 'vertical'}
	class:type-label={data.decorationType === 'label'}
	class:type-textbox={data.decorationType === 'textbox'}
	class:type-image={data.decorationType === 'image'}
	style:opacity={data.opacity ?? 1}
	style:--fill-color={data.fillColor || 'transparent'}
	style:--stroke-color={data.strokeColor || 'transparent'}
	style:--stroke-width="{data.strokeWidth ?? 1}px"
	style:--font-size="{data.fontSize ?? 14}px"
	style:--font-color={data.fontColor || '#ffffff'}
>
	{#if data.locked}
		<span class="lock-icon" title="Position locked">&#x1F512;</span>
	{/if}
	{#if data.decorationType === 'label'}
		<span class="text-content">{data.text || ''}</span>
	{:else if data.decorationType === 'textbox'}
		<span class="textbox-content">{data.text || ''}</span>
	{:else if data.decorationType === 'image'}
		{#if imageUrl}
			<img src={imageUrl} alt={data.label} class="image-content" />
		{:else}
			<span class="placeholder">No image</span>
		{/if}
		{#if data.text}
			<span class="text-overlay">{data.text}</span>
		{/if}
	{:else if data.decorationType === 'triangle'}
		<div class="triangle-shape"></div>
		{#if data.text}
			<span class="text-overlay">{data.text}</span>
		{/if}
	{:else}
		{#if data.text}
			<span class="text-overlay">{data.text}</span>
		{/if}
	{/if}
</div>

<style>
	.decoration-node {
		width: 100%;
		height: 100%;
		display: flex;
		align-items: center;
		justify-content: center;
		position: relative;
		pointer-events: all;
	}

	/* Rectangle */
	.type-rectangle {
		background: var(--fill-color);
		border: var(--stroke-width) solid var(--stroke-color);
	}

	/* Rounded Rectangle */
	.type-rounded-rectangle {
		background: var(--fill-color);
		border: var(--stroke-width) solid var(--stroke-color);
		border-radius: 12px;
	}

	/* Circle */
	.type-circle {
		background: var(--fill-color);
		border: var(--stroke-width) solid var(--stroke-color);
		border-radius: 50%;
	}

	/* Triangle — uses a child div with clip-path */
	.type-triangle {
		background: transparent;
		border: none;
	}

	.triangle-shape {
		width: 100%;
		height: 100%;
		background: var(--fill-color);
		clip-path: polygon(50% 0%, 0% 100%, 100% 100%);
		position: absolute;
		inset: 0;
	}

	/* Stroke via drop-shadow on the parent when stroke is set */
	.type-triangle {
		filter: drop-shadow(0 0 var(--stroke-width) var(--stroke-color));
	}

	/* Divider (horizontal by default) */
	.type-divider {
		background: transparent;
		border: none;
		min-height: 2px;
		min-width: 2px;
	}

	.type-divider::after {
		content: '';
		position: absolute;
		left: 0;
		right: 0;
		top: 50%;
		transform: translateY(-50%);
		height: var(--stroke-width);
		background: var(--stroke-color);
	}

	/* Divider vertical */
	.type-divider.divider-vertical::after {
		left: 50%;
		right: auto;
		top: 0;
		bottom: 0;
		transform: translateX(-50%);
		height: auto;
		width: var(--stroke-width);
	}

	/* Label */
	.type-label {
		background: transparent;
		border: none;
	}

	/* Textbox */
	.type-textbox {
		background: var(--fill-color);
		border: var(--stroke-width) solid var(--stroke-color);
		border-radius: 4px;
		overflow: hidden;
	}

	/* Image */
	.type-image {
		background: var(--fill-color);
		border: var(--stroke-width) solid var(--stroke-color);
		overflow: hidden;
	}

	.image-content {
		width: 100%;
		height: 100%;
		object-fit: contain;
	}

	.text-content {
		font-size: var(--font-size);
		color: var(--font-color);
		white-space: nowrap;
		text-align: center;
		padding: 4px;
		user-select: none;
	}

	.textbox-content {
		font-size: var(--font-size);
		color: var(--font-color);
		white-space: pre-wrap;
		word-break: break-word;
		text-align: left;
		padding: 8px;
		width: 100%;
		height: 100%;
		overflow: auto;
		user-select: none;
	}

	.text-overlay {
		font-size: var(--font-size);
		color: var(--font-color);
		white-space: pre-wrap;
		word-break: break-word;
		text-align: center;
		position: relative;
		z-index: 1;
		padding: 4px;
		user-select: none;
	}

	.placeholder {
		font-size: 12px;
		color: var(--netrun-text-secondary, #a0a0a0);
		font-style: italic;
		user-select: none;
	}

	.lock-icon {
		position: absolute;
		top: 2px;
		right: 4px;
		font-size: 10px;
		opacity: 0.6;
		z-index: 2;
	}

	/* Selection highlight */
	.selected {
		box-shadow: 0 0 0 2px var(--netrun-accent-color, #3b82f6);
	}

	.type-triangle.selected {
		box-shadow: none;
	}
</style>
