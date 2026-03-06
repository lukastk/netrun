/**
 * Keyboard shortcut handling store
 *
 * Centralizes keyboard shortcut management and provides utilities
 * for formatting shortcuts for display.
 */
import { writable, get } from 'svelte/store';
import { executeCommand } from './commandStore';

export interface ShortcutBinding {
	key: string; // e.g., "s", "z", "g", "Tab"
	metaKey?: boolean; // Cmd on Mac, Ctrl on Windows
	ctrlKey?: boolean;
	shiftKey?: boolean;
	altKey?: boolean;
	commandId: string;
}

// Registered shortcuts
export const shortcuts = writable<ShortcutBinding[]>([]);

/**
 * Register a keyboard shortcut
 */
export function registerShortcut(binding: ShortcutBinding): void {
	shortcuts.update((s) => {
		// Remove any existing binding for this command
		const filtered = s.filter((b) => b.commandId !== binding.commandId);
		return [...filtered, binding];
	});
}

/**
 * Register multiple shortcuts
 */
export function registerShortcuts(bindings: ShortcutBinding[]): void {
	bindings.forEach(registerShortcut);
}

/**
 * Unregister a shortcut by command ID
 */
export function unregisterShortcut(commandId: string): void {
	shortcuts.update((s) => s.filter((b) => b.commandId !== commandId));
}

/**
 * Check if an input element is focused
 */
function isInputFocused(): boolean {
	const activeElement = document.activeElement;
	return (
		activeElement instanceof HTMLInputElement ||
		activeElement instanceof HTMLTextAreaElement ||
		activeElement?.getAttribute('contenteditable') === 'true'
	);
}

/**
 * Handle a keyboard event
 * Returns true if the event was handled (should prevent default)
 */
export function handleKeyboardEvent(event: KeyboardEvent): boolean {
	const bindings = get(shortcuts);

	for (const binding of bindings) {
		// Check if the binding matches
		if (binding.key.toLowerCase() !== event.key.toLowerCase()) continue;

		// Check modifiers
		const wantsMeta = binding.metaKey ?? false;
		const wantsCtrl = binding.ctrlKey ?? false;
		const wantsShift = binding.shiftKey ?? false;
		const wantsAlt = binding.altKey ?? false;

		// On Mac, metaKey is Cmd; on Windows/Linux, we treat Ctrl as the primary modifier
		const hasPrimaryModifier = event.metaKey || event.ctrlKey;
		const wantsPrimaryModifier = wantsMeta || wantsCtrl;

		// For shortcuts that want meta/ctrl, check if either is pressed
		if (wantsPrimaryModifier && !hasPrimaryModifier) continue;
		if (!wantsPrimaryModifier && hasPrimaryModifier) continue;

		// Check shift
		if (wantsShift !== event.shiftKey) continue;

		// Check alt
		if (wantsAlt !== event.altKey) continue;

		// Skip shortcuts when input is focused (except Tab and copy/paste/cut)
		if (binding.key.toLowerCase() !== 'tab' && isInputFocused()) {
			if (['c', 'v', 'x'].includes(binding.key.toLowerCase()) && (event.metaKey || event.ctrlKey)) {
				// Allow copy/paste/cut to pass through to native input handling
				continue;
			}
			// Skip all other shortcuts when input is focused
			continue;
		}

		// Execute the command
		event.preventDefault();
		executeCommand(binding.commandId);
		return true;
	}

	return false;
}

/**
 * Format a shortcut binding for display
 * Uses Mac-style symbols: ⌘ (Cmd), ⇧ (Shift), ⌥ (Alt), ⌃ (Ctrl)
 */
export function formatShortcut(binding: ShortcutBinding): string {
	const parts: string[] = [];

	// On Mac, use symbols; on Windows, use text
	const isMac = typeof navigator !== 'undefined' && navigator.platform.includes('Mac');

	if (binding.ctrlKey) {
		parts.push(isMac ? '⌃' : 'Ctrl+');
	}
	if (binding.altKey) {
		parts.push(isMac ? '⌥' : 'Alt+');
	}
	if (binding.shiftKey) {
		parts.push(isMac ? '⇧' : 'Shift+');
	}
	if (binding.metaKey) {
		parts.push(isMac ? '⌘' : 'Ctrl+');
	}

	// Format the key
	let keyDisplay = binding.key.toUpperCase();
	if (binding.key === 'Tab') {
		keyDisplay = isMac ? '⇥' : 'Tab';
	} else if (binding.key === ' ') {
		keyDisplay = 'Space';
	} else if (binding.key === 'Backspace') {
		keyDisplay = isMac ? '⌫' : 'Backspace';
	} else if (binding.key === 'Delete') {
		keyDisplay = isMac ? '⌦' : 'Del';
	} else if (binding.key === 'Enter') {
		keyDisplay = isMac ? '↵' : 'Enter';
	} else if (binding.key === 'Escape') {
		keyDisplay = 'Esc';
	}

	parts.push(keyDisplay);

	return parts.join('');
}

/**
 * Create a shortcut display string from a binding
 */
export function shortcutToString(binding: ShortcutBinding): string {
	return formatShortcut(binding);
}

/**
 * Get the shortcut string for a command ID
 */
export function getShortcutForCommand(commandId: string): string | null {
	const binding = get(shortcuts).find((s) => s.commandId === commandId);
	return binding ? formatShortcut(binding) : null;
}
