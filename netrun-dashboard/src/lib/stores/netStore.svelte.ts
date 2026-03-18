import type { ObserveState } from '../types.js';
import { fetchNetConfig, connectWebSocket } from '../api.js';

let config = $state<Record<string, unknown> | null>(null);
let liveState = $state<ObserveState | null>(null);
let connected = $state(false);
let currentUrl = $state<string | null>(null);
let _closeWs: (() => void) | null = null;

export async function connectToNet(url: string) {
	// Disconnect previous if any
	disconnectFromNet();
	currentUrl = url;

	// Fetch config once
	try {
		config = await fetchNetConfig(url);
	} catch {
		config = null;
	}

	// Open WebSocket for live state
	connected = false;
	_closeWs = connectWebSocket(
		url,
		(state) => {
			liveState = state;
			connected = true;
		},
		() => {
			connected = false;
		},
	);
}

export function disconnectFromNet() {
	if (_closeWs) {
		_closeWs();
		_closeWs = null;
	}
	config = null;
	liveState = null;
	connected = false;
	currentUrl = null;
}

export function getNetState() {
	return {
		get config() {
			return config;
		},
		get liveState() {
			return liveState;
		},
		get connected() {
			return connected;
		},
		get currentUrl() {
			return currentUrl;
		},
	};
}
