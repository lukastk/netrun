<script lang="ts">
	import { SvelteFlowProvider } from '@xyflow/svelte';
	import Toolbar from '$lib/components/Toolbar.svelte';
	import Sidebar from '$lib/components/Sidebar.svelte';
	import FlowEditor from '$lib/components/FlowEditor.svelte';
	import {
		nodes,
		edges,
		addNode,
		createRegularNode
	} from '$lib/stores/flowStore';

	// Add some demo nodes on mount
	$effect(() => {
		if ($nodes.length === 0) {
			// Create demo nodes
			const node1 = createRegularNode({ x: 100, y: 100 }, 'Input Node');
			node1.data.inPorts = [];
			node1.data.outPorts = [
				{ name: 'data', type: 'int' },
				{ name: 'meta', type: 'dict' }
			];

			const node2 = createRegularNode({ x: 400, y: 100 }, 'Process Node');
			node2.data.inPorts = [
				{ name: 'data', type: 'int' },
				{ name: 'config', type: 'dict' }
			];
			node2.data.outPorts = [
				{ name: 'result', type: 'int' }
			];

			const node3 = createRegularNode({ x: 700, y: 100 }, 'Output Node');
			node3.data.inPorts = [
				{ name: 'value', type: 'any' }
			];
			node3.data.outPorts = [];

			nodes.set([node1, node2, node3]);

			// Create demo edges
			edges.set([
				{
					id: 'e1-2',
					source: node1.id,
					sourceHandle: 'data',
					target: node2.id,
					targetHandle: 'data',
					type: 'smoothstep'
				},
				{
					id: 'e2-3',
					source: node2.id,
					sourceHandle: 'result',
					target: node3.id,
					targetHandle: 'value',
					type: 'smoothstep'
				}
			]);
		}
	});
</script>

<div class="app">
	<Toolbar />
	<div class="main-content">
		<Sidebar />
		<div class="canvas-container">
			<SvelteFlowProvider>
				<FlowEditor />
			</SvelteFlowProvider>
		</div>
	</div>
</div>

<style>
	.app {
		height: 100vh;
		width: 100vw;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.main-content {
		flex: 1;
		display: flex;
		overflow: hidden;
	}

	.canvas-container {
		flex: 1;
		height: 100%;
		position: relative;
	}
</style>
