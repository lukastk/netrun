import type { RegisteredNet, ObserveState } from './types.js';

/** Fetch the list of registered nets from the dashboard API. */
export async function fetchRegisteredNets(): Promise<RegisteredNet[]> {
	const res = await fetch('/api/nets');
	if (!res.ok) throw new Error(`GET /api/nets failed: ${res.status}`);
	return res.json();
}

/** Manually register a net with the dashboard. */
export async function addNetManually(name: string, url: string): Promise<void> {
	const res = await fetch('/api/register', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ name, url }),
	});
	if (!res.ok) throw new Error(`POST /api/register failed: ${res.status}`);
}

/** Fetch the resolved net config from an ObserveServer. */
export async function fetchNetConfig(observeUrl: string): Promise<Record<string, unknown>> {
	const res = await fetch(`${observeUrl}/config`);
	if (!res.ok) throw new Error(`GET ${observeUrl}/config failed: ${res.status}`);
	return res.json();
}

// --- Control API (sent to ObserveServer) ---

async function postControl(observeUrl: string, path: string, body?: Record<string, unknown>): Promise<{ ok: boolean; message: string }> {
	const res = await fetch(`${observeUrl}${path}`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: body ? JSON.stringify(body) : undefined,
	});
	return res.json();
}

export function enableNode(observeUrl: string, nodeName: string) {
	return postControl(observeUrl, `/nodes/${nodeName}/enable`);
}

export function disableNode(observeUrl: string, nodeName: string) {
	return postControl(observeUrl, `/nodes/${nodeName}/disable`);
}

export function injectData(observeUrl: string, nodeName: string, portName: string, values: unknown[]) {
	return postControl(observeUrl, '/inject', { node_name: nodeName, port_name: portName, values });
}

export function pauseNet(observeUrl: string) {
	return postControl(observeUrl, '/pause');
}

export function resumeNet(observeUrl: string) {
	return postControl(observeUrl, '/resume');
}

/** Connect to an ObserveServer's WebSocket for live state updates.
 *  Returns a function to close the connection.
 */
export function connectWebSocket(
	observeUrl: string,
	onMessage: (state: ObserveState) => void,
	onClose?: () => void,
): () => void {
	const wsUrl = observeUrl.replace(/^http/, 'ws') + '/ws';
	const ws = new WebSocket(wsUrl);

	ws.onmessage = (event) => {
		try {
			const state: ObserveState = JSON.parse(event.data);
			onMessage(state);
		} catch {
			// ignore malformed messages
		}
	};

	ws.onclose = () => {
		onClose?.();
	};

	ws.onerror = () => {
		ws.close();
	};

	return () => {
		ws.close();
	};
}
