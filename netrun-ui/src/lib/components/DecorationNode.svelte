<script lang="ts">
	import { DecorationNode as VisDecorationNode } from 'netrun-ui-vis/components';
	import type { DecorationNodeData } from '$lib/stores/flowStore';
	import { updateNodeDimensions, pushHistory, getCurrentConfig } from '$lib/stores/flowStore';
	import { getApiBase } from '$lib/api';

	interface Props {
		id: string;
		data: DecorationNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

	// Resolve image URL (previously done inside the component, now done in wrapper)
	let imageUrl = $derived.by(() => {
		if (data.decorationType !== 'image' || !data.imagePath) return '';
		if (data.imagePath.startsWith('http') || data.imagePath.startsWith('data:')) {
			return data.imagePath;
		}
		const config = getCurrentConfig();
		const projectRoot = (config.project_root_path as string) || '';
		return `${getApiBase()}/files/image?path=${encodeURIComponent(data.imagePath)}&project_root=${encodeURIComponent(projectRoot)}`;
	});

	function handleResize(event: { nodeId: string; width: number; height: number; position: { x: number; y: number } }) {
		updateNodeDimensions([{
			id: event.nodeId,
			width: event.width,
			height: event.height,
			position: event.position,
		}]);
		pushHistory();
	}
</script>

<VisDecorationNode
	{id}
	{data}
	{selected}
	{imageUrl}
	onResize={handleResize}
/>
