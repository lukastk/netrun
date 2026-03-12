<script lang="ts">
	import { NetrunNode as VisNetrunNode } from 'netrun-ui-vis/components';
	import type { NetrunNodeData } from '$lib/stores/flowStore';
	import { updateNodeDimensions, pushHistory, toggleNodeDescExpanded, cascadeHighlight, toggleNodePortGroup } from '$lib/stores/flowStore';
	import { signalTypeInfo } from '$lib/stores/signalStore';
	import { controlTypeInfo } from '$lib/stores/controlStore';
	import type { PortTypeConfig } from 'netrun-ui-vis';

	interface Props {
		id: string;
		data: NetrunNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

	// Convert store values to PortTypeConfig
	let signalConfig: PortTypeConfig | undefined = $derived(
		$signalTypeInfo ? { prefix: $signalTypeInfo.portPrefix, suffix: $signalTypeInfo.portSuffix, types: $signalTypeInfo.validTypes } : undefined
	);
	let controlConfig: PortTypeConfig | undefined = $derived(
		$controlTypeInfo ? { prefix: $controlTypeInfo.portPrefix, suffix: $controlTypeInfo.portSuffix, types: $controlTypeInfo.validTypes } : undefined
	);

	function handleResize(event: { nodeId: string; width: number; height: number; position: { x: number; y: number } }) {
		updateNodeDimensions([{
			id: event.nodeId,
			width: event.width,
			height: event.height,
			position: event.position,
		}]);
		pushHistory();
	}

	function handleDescriptionToggle(event: { nodeId: string }) {
		toggleNodeDescExpanded(event.nodeId);
	}

	function handleDoubleClick(event: { nodeId: string; data: NetrunNodeData; metaKey: boolean }) {
		window.dispatchEvent(new CustomEvent('netrun-node-dblclick', { detail: { id: event.nodeId, data: event.data, metaKey: event.metaKey } }));
	}

	function handlePortGroupToggle(event: { nodeId: string; side: 'in' | 'out'; groupPath: string; portCount: number }) {
		toggleNodePortGroup(event.nodeId, event.side, event.groupPath, event.portCount);
	}
</script>

<VisNetrunNode
	{id}
	{data}
	{selected}
	cascadeHighlight={$cascadeHighlight}
	{signalConfig}
	{controlConfig}
	onResize={handleResize}
	onDescriptionToggle={handleDescriptionToggle}
	onDoubleClick={handleDoubleClick}
	onPortGroupToggle={handlePortGroupToggle}
/>
