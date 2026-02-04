import { writable } from 'svelte/store';

export interface Toast {
	id: string;
	message: string;
	type: 'error' | 'success' | 'info' | 'warning';
	duration?: number;
}

function createToastStore() {
	const { subscribe, update } = writable<Toast[]>([]);

	let idCounter = 0;

	function addToast(message: string, type: Toast['type'] = 'info', duration = 5000): string {
		const id = `toast-${++idCounter}`;
		const toast: Toast = { id, message, type, duration };

		update((toasts) => [...toasts, toast]);

		if (duration > 0) {
			setTimeout(() => {
				removeToast(id);
			}, duration);
		}

		return id;
	}

	function removeToast(id: string) {
		update((toasts) => toasts.filter((t) => t.id !== id));
	}

	return {
		subscribe,
		error: (message: string, duration?: number) => addToast(message, 'error', duration),
		success: (message: string, duration?: number) => addToast(message, 'success', duration),
		info: (message: string, duration?: number) => addToast(message, 'info', duration),
		warning: (message: string, duration?: number) => addToast(message, 'warning', duration),
		remove: removeToast,
	};
}

export const toasts = createToastStore();
