import { mount } from 'svelte';
import { setApiBase } from '$lib/api';
import App from './App.svelte';
import '../../../src/app.css';

declare const __APP_VERSION__: string;

// Acquire VS Code webview API
declare function acquireVsCodeApi(): {
	postMessage(message: unknown): void;
	getState(): unknown;
	setState(state: unknown): void;
};

const vscode = acquireVsCodeApi();

let app: Record<string, any> | null = null;

// Listen for messages from extension host
window.addEventListener('message', (event) => {
	const message = event.data;
	if (message.type === 'init') {
		setApiBase(message.apiBase);

		app = mount(App, {
			target: document.getElementById('app')!,
			props: {
				filePath: message.filePath,
				vscode,
			},
		});
	} else if (message.type === 'save') {
		// Triggered by Cmd+S keybinding via extension host
		window.dispatchEvent(new CustomEvent('netrun-vscode-save'));
	}
});

// Tell extension host we're ready
vscode.postMessage({ type: 'ready' });
