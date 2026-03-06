<script lang="ts">
	import {
		toggleEdgeDependency,
		pushHistory,
		type NetrunEdge,
		type NetrunEdgeData,
	} from '$lib/stores/flowStore';

	interface Props {
		edge: NetrunEdge;
	}

	let { edge }: Props = $props();

	let isDependency = $derived((edge.data as NetrunEdgeData | undefined)?.dependency ?? false);

	let sourceLabel = $derived(`${edge.source}.${edge.sourceHandle || 'out'}`);
	let targetLabel = $derived(`${edge.target}.${edge.targetHandle || 'in'}`);
</script>

<section class="section">
	<div class="section-header">
		<span class="section-title">Edge Properties</span>
	</div>
	<div class="section-content">
		<div class="field edge-endpoints">
			<span class="endpoint">{sourceLabel}</span>
			<span class="arrow">&rarr;</span>
			<span class="endpoint">{targetLabel}</span>
		</div>

		<div class="field">
			<label class="checkbox-label">
				<input
					type="checkbox"
					checked={isDependency}
					onchange={() => toggleEdgeDependency(edge.id)}
				/>
				Dependency Edge
			</label>
			<p class="help-text">
				Dependency edges participate in backward request cascades.
				Packet requests propagate backward through dependency edges to find source nodes.
			</p>
		</div>
	</div>
</section>

<style>
	.section {
		border-bottom: 1px solid var(--border-color, #404040);
	}

	.section-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 10px 12px;
		background: var(--bg-tertiary, #2d2d2d);
		font-weight: 500;
		font-size: 12px;
		color: var(--text-primary, #fff);
	}

	.section-content {
		padding: 10px 12px;
	}

	.field {
		margin-bottom: 10px;
	}

	.edge-endpoints {
		display: flex;
		align-items: center;
		gap: 8px;
		padding: 6px 8px;
		background: var(--bg-secondary, #242424);
		border-radius: 4px;
		font-family: monospace;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}

	.endpoint {
		color: var(--text-primary, #fff);
	}

	.arrow {
		color: var(--text-secondary, #a0a0a0);
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
		accent-color: #a78bfa;
	}

	.help-text {
		margin: 4px 0 0;
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		line-height: 1.4;
	}
</style>
