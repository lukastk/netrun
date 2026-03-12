<script lang="ts">
	import { SubgraphNode as VisSubgraphNode } from 'netrun-ui-vis/components';
	import type { SubgraphNodeData } from '$lib/stores/flowStore';
	import { updateNodeDimensions, pushHistory, toggleNodeDescExpanded, cascadeHighlight, toggleNodePortGroup } from '$lib/stores/flowStore';
	import { openSubgraphTab } from '$lib/stores/tabsStore';
	import { toggleSubgraphExpansion } from '$lib/stores/subgraphExpandStore';
	import { signalTypeInfo } from '$lib/stores/signalStore';
	import { controlTypeInfo } from '$lib/stores/controlStore';
	import type { PortTypeConfig } from 'netrun-ui-vis';

	interface Props {
		id: string;
		data: SubgraphNodeData;
		selected?: boolean;
	}

	let { id, data, selected = false }: Props = $props();

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

	function handleSubgraphOpen(event: { nodeId: string; data: SubgraphNodeData }) {
		openSubgraphTab(event.nodeId, event.data);
	}

	function handleSubgraphToggleExpand(event: { nodeId: string }) {
		toggleSubgraphExpansion(event.nodeId);
	}

	function handlePortGroupToggle(event: { nodeId: string; side: 'in' | 'out'; groupPath: string; portCount: number }) {
		toggleNodePortGroup(event.nodeId, event.side, event.groupPath, event.portCount);
	}
</script>

<VisSubgraphNode
	{id}
	{data}
	{selected}
	cascadeHighlight={$cascadeHighlight}
	{signalConfig}
	{controlConfig}
	onResize={handleResize}
	onDescriptionToggle={handleDescriptionToggle}
	onSubgraphOpen={handleSubgraphOpen}
	onSubgraphToggleExpand={handleSubgraphToggleExpand}
	onPortGroupToggle={handlePortGroupToggle}
/>
