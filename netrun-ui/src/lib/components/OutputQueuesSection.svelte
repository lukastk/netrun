<script lang="ts">
	import { pushHistory } from '$lib/stores/flowStore';

	interface Props {
		outputQueues: Record<string, Record<string, unknown>> | null | undefined;
		onUpdate: (queues: Record<string, Record<string, unknown>> | null) => void;
	}

	let { outputQueues, onUpdate }: Props = $props();

	let isAuto = $derived(outputQueues == null);

	function toggleAuto() {
		if (isAuto) {
			onUpdate({});
		} else {
			onUpdate(null);
		}
		pushHistory();
	}

	let queueEntries = $derived(
		outputQueues ? Object.entries(outputQueues) : []
	);

	function addQueue() {
		const current = { ...(outputQueues ?? {}) };
		const name = generateUniqueName(current, 'queue');
		current[name] = { ports: [] };
		onUpdate(current);
		pushHistory();
	}

	function removeQueue(name: string) {
		const current = { ...(outputQueues ?? {}) };
		delete current[name];
		if (Object.keys(current).length === 0) {
			onUpdate({});
		} else {
			onUpdate(current);
		}
		pushHistory();
	}

	function updateQueueName(oldName: string, newName: string) {
		if (oldName === newName || !newName.trim()) return;
		if (outputQueues && newName in outputQueues) return;
		const updated: Record<string, Record<string, unknown>> = {};
		for (const [name, config] of Object.entries(outputQueues ?? {})) {
			updated[name === oldName ? newName : name] = config;
		}
		onUpdate(updated);
		pushHistory();
	}

	// Port pairs: each queue has ports: [[node, port], ...]
	function getPortPairs(queueConfig: Record<string, unknown>): [string, string][] {
		const ports = queueConfig.ports;
		if (Array.isArray(ports)) {
			return ports.map(p => {
				if (Array.isArray(p) && p.length >= 2) return [String(p[0]), String(p[1])];
				return ['', ''];
			});
		}
		return [];
	}

	function updatePorts(queueName: string, ports: [string, string][]) {
		const current = { ...(outputQueues ?? {}) };
		current[queueName] = { ...(current[queueName] ?? {}), ports };
		onUpdate(current);
	}

	function addPort(queueName: string) {
		const pairs = getPortPairs(outputQueues?.[queueName] ?? {});
		updatePorts(queueName, [...pairs, ['', '']]);
		pushHistory();
	}

	function removePort(queueName: string, index: number) {
		const pairs = [...getPortPairs(outputQueues?.[queueName] ?? {})];
		pairs.splice(index, 1);
		updatePorts(queueName, pairs);
		pushHistory();
	}

	function updatePort(queueName: string, index: number, field: 0 | 1, value: string) {
		const pairs = [...getPortPairs(outputQueues?.[queueName] ?? {})];
		pairs[index] = [...pairs[index]] as [string, string];
		pairs[index][field] = value;
		updatePorts(queueName, pairs);
	}

	function generateUniqueName(existing: Record<string, unknown>, base: string): string {
		if (!(base in existing)) return base;
		let i = 1;
		while (`${base}_${i}` in existing) i++;
		return `${base}_${i}`;
	}
</script>

<div class="output-queues-section">
	<label class="defaults-toggle">
		<input type="checkbox" checked={isAuto} onchange={toggleAuto} />
		<span>Auto-generate (from unconnected output ports)</span>
	</label>

	{#if !isAuto}
		<div class="queues-list">
			{#each queueEntries as [name, config]}
				{@const ports = getPortPairs(config)}
				<div class="queue-editor">
					<div class="queue-header">
						<input
							type="text"
							class="queue-name-input"
							value={name}
							onblur={(e) => updateQueueName(name, (e.target as HTMLInputElement).value)}
							onkeydown={(e) => { if (e.key === 'Enter') (e.target as HTMLInputElement).blur(); }}
						/>
						<button
							class="remove-btn"
							onclick={() => removeQueue(name)}
							title="Remove queue"
						>
							&times;
						</button>
					</div>
					<div class="queue-body">
						<div class="ports-label">Ports</div>
						{#each ports as [nodeName, portName], i}
							<div class="port-pair">
								<input
									type="text"
									value={nodeName}
									placeholder="node_name"
									oninput={(e) => updatePort(name, i, 0, (e.target as HTMLInputElement).value)}
									onblur={() => pushHistory()}
								/>
								<span class="port-separator">:</span>
								<input
									type="text"
									value={portName}
									placeholder="port_name"
									oninput={(e) => updatePort(name, i, 1, (e.target as HTMLInputElement).value)}
									onblur={() => pushHistory()}
								/>
								<button
									class="remove-btn"
									onclick={() => removePort(name, i)}
									title="Remove port"
								>
									&times;
								</button>
							</div>
						{/each}
						<button class="add-port-btn" onclick={() => addPort(name)}>+ Add Port</button>
					</div>
				</div>
			{/each}
		</div>

		<button class="add-btn" onclick={addQueue}>+ Add Queue</button>
	{/if}
</div>

<style>
	.output-queues-section {
		display: flex;
		flex-direction: column;
		gap: 10px;
	}

	.defaults-toggle {
		display: flex;
		align-items: center;
		gap: 8px;
		cursor: pointer;
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
	}

	.defaults-toggle input[type='checkbox'] {
		width: 14px;
		height: 14px;
		cursor: pointer;
	}

	.defaults-toggle:hover {
		color: var(--text-primary, #fff);
	}

	.queues-list {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.queue-editor {
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid var(--border-color, #404040);
		border-radius: 4px;
		overflow: hidden;
	}

	.queue-header {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 6px 8px;
		background: var(--bg-tertiary, #2d2d2d);
		gap: 8px;
	}

	.queue-name-input {
		flex: 1;
		font-size: 12px;
		font-weight: 500;
		padding: 4px 6px;
		background: var(--bg-primary, #1a1a1a);
		border: 1px solid transparent;
		border-radius: 3px;
		color: var(--text-primary, #fff);
	}

	.queue-name-input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.queue-body {
		padding: 8px;
		display: flex;
		flex-direction: column;
		gap: 6px;
	}

	.ports-label {
		font-size: 10px;
		color: var(--text-secondary, #a0a0a0);
		text-transform: uppercase;
		letter-spacing: 0.5px;
	}

	.port-pair {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.port-pair input {
		flex: 1;
		min-width: 0;
		padding: 4px 6px;
		background: var(--bg-tertiary, #2d2d2d);
		border: 1px solid var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-primary, #fff);
		font-size: 11px;
	}

	.port-pair input:focus {
		outline: none;
		border-color: var(--accent-color, #3b82f6);
	}

	.port-separator {
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
		flex-shrink: 0;
	}

	.remove-btn {
		background: transparent;
		color: var(--text-secondary, #a0a0a0);
		padding: 2px 6px;
		font-size: 14px;
		line-height: 1;
		border: none;
		cursor: pointer;
		flex-shrink: 0;
	}

	.remove-btn:hover {
		color: var(--error-color, #ef4444);
	}

	.add-port-btn {
		padding: 4px;
		font-size: 10px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
	}

	.add-port-btn:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}

	.add-btn {
		width: 100%;
		padding: 6px;
		font-size: 11px;
		background: transparent;
		border: 1px dashed var(--border-color, #404040);
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		cursor: pointer;
	}

	.add-btn:hover {
		border-color: var(--accent-color, #3b82f6);
		color: var(--accent-color, #3b82f6);
	}
</style>
