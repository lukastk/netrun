import '../src/theme.css';
import { mount } from 'svelte';
import App from './App.svelte';
import { configToGraph } from '../src/utils/configConverter.js';

// Read the embedded config and options
const rawConfig = (window as unknown as { __NETRUN_CONFIG__: unknown }).__NETRUN_CONFIG__;
const options = (window as unknown as { __NETRUN_OPTIONS__?: Record<string, unknown> }).__NETRUN_OPTIONS__ ?? {};
if (!rawConfig || typeof rawConfig === 'string') {
	document.getElementById('app')!.innerHTML = `
		<div style="color: #a0a0a0; padding: 40px; text-align: center;">
			<h2 style="color: #fff; margin-bottom: 12px;">No graph data</h2>
			<p>This file was generated without embedded graph data.</p>
		</div>
	`;
} else {
	const graph = configToGraph(rawConfig as Record<string, unknown>);

	// Update page title from config
	const extra = (rawConfig as Record<string, unknown>).extra as Record<string, unknown> | undefined;
	if (extra?.description) {
		document.title = `Netrun: ${extra.description}`;
	}

	mount(App, {
		target: document.getElementById('app')!,
		props: { graph, showMinimap: !!options.minimap },
	});
}
