import type { RegisteredNet } from '../types.js';
import { fetchRegisteredNets, addNetManually } from '../api.js';

let nets = $state<RegisteredNet[]>([]);
let selectedUrl = $state<string | null>(null);
let _pollInterval: ReturnType<typeof setInterval> | null = null;

async function poll() {
	try {
		nets = await fetchRegisteredNets();
		// If selected net was pruned, deselect
		if (selectedUrl && !nets.some((n) => n.url === selectedUrl)) {
			selectedUrl = null;
		}
	} catch {
		// silent — dashboard API may be unavailable
	}
}

export function startPolling() {
	if (_pollInterval) return;
	poll();
	_pollInterval = setInterval(poll, 3000);
}

export function stopPolling() {
	if (_pollInterval) {
		clearInterval(_pollInterval);
		_pollInterval = null;
	}
}

export function selectNet(url: string) {
	selectedUrl = url;
}

export function deselectNet() {
	selectedUrl = null;
}

export async function addManualNet(name: string, url: string) {
	await addNetManually(name, url);
	await poll();
}

export function getRegistryState() {
	return {
		get nets() {
			return nets;
		},
		get selectedUrl() {
			return selectedUrl;
		},
	};
}
