/**
 * Modal dialog store
 *
 * Provides Promise-based APIs for showing prompt, alert, and confirm dialogs
 * that replace the native browser dialogs with styled modals.
 */
import { writable, get } from 'svelte/store';

export type ModalType = 'prompt' | 'alert' | 'confirm';

export interface ModalOptions {
	title: string;
	message?: string;
	placeholder?: string;
	defaultValue?: string;
	confirmText?: string;
	cancelText?: string;
	inputType?: 'text' | 'path';
}

export interface ModalState {
	isOpen: boolean;
	type: ModalType;
	title: string;
	message?: string;
	placeholder?: string;
	defaultValue?: string;
	confirmText: string;
	cancelText: string;
	inputType: 'text' | 'path';
	resolve?: (value: string | boolean | null) => void;
}

const defaultState: ModalState = {
	isOpen: false,
	type: 'alert',
	title: '',
	message: undefined,
	placeholder: undefined,
	defaultValue: undefined,
	confirmText: 'OK',
	cancelText: 'Cancel',
	inputType: 'text',
	resolve: undefined,
};

export const modalState = writable<ModalState>(defaultState);

/**
 * Show a prompt dialog and return the entered value (or null if cancelled)
 */
export function showPrompt(options: ModalOptions): Promise<string | null> {
	return new Promise((resolve) => {
		modalState.set({
			isOpen: true,
			type: 'prompt',
			title: options.title,
			message: options.message,
			placeholder: options.placeholder,
			defaultValue: options.defaultValue,
			confirmText: options.confirmText || 'OK',
			cancelText: options.cancelText || 'Cancel',
			inputType: options.inputType || 'text',
			resolve: (value) => resolve(value as string | null),
		});
	});
}

/**
 * Show an alert dialog (informational, single OK button)
 */
export function showAlert(options: ModalOptions): Promise<void> {
	return new Promise((resolve) => {
		modalState.set({
			isOpen: true,
			type: 'alert',
			title: options.title,
			message: options.message,
			placeholder: undefined,
			defaultValue: undefined,
			confirmText: options.confirmText || 'OK',
			cancelText: 'Cancel',
			inputType: 'text',
			resolve: () => resolve(),
		});
	});
}

/**
 * Show a confirm dialog and return true/false
 */
export function showConfirm(options: ModalOptions): Promise<boolean> {
	return new Promise((resolve) => {
		modalState.set({
			isOpen: true,
			type: 'confirm',
			title: options.title,
			message: options.message,
			placeholder: undefined,
			defaultValue: undefined,
			confirmText: options.confirmText || 'OK',
			cancelText: options.cancelText || 'Cancel',
			inputType: 'text',
			resolve: (value) => resolve(value as boolean),
		});
	});
}

/**
 * Close the modal with a result
 */
export function closeModal(result: string | boolean | null): void {
	const state = get(modalState);
	if (state.resolve) {
		state.resolve(result);
	}
	modalState.set(defaultState);
}

/**
 * Cancel the modal (close with null/false)
 */
export function cancelModal(): void {
	const state = get(modalState);
	if (state.type === 'prompt') {
		closeModal(null);
	} else if (state.type === 'confirm') {
		closeModal(false);
	} else {
		closeModal(null);
	}
}
