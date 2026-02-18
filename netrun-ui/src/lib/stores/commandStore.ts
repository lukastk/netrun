/**
 * Command Palette store
 *
 * Manages a registry of commands that can be searched and executed.
 */
import { writable, derived, get } from 'svelte/store';

export type CommandCategory = 'file' | 'edit' | 'view' | 'layout' | 'node' | 'subgraph' | 'decoration' | 'tab' | 'action' | 'recipe';

export interface Command {
	id: string;
	label: string;
	category: CommandCategory;
	keywords?: string[];
	shortcut?: string; // Display string like "⌘S"
	action: () => void | Promise<void>;
	enabled?: () => boolean; // Dynamic enable/disable
}

// Registry of all commands
export const commands = writable<Command[]>([]);

// Command palette open state
export const commandPaletteOpen = writable(false);

// Recently used command IDs (stored in localStorage)
const RECENT_COMMANDS_KEY = 'netrun-ui-recent-commands';
const MAX_RECENT = 5;

function loadRecentCommands(): string[] {
	if (typeof localStorage === 'undefined') return [];
	try {
		const stored = localStorage.getItem(RECENT_COMMANDS_KEY);
		return stored ? JSON.parse(stored) : [];
	} catch {
		return [];
	}
}

function saveRecentCommands(ids: string[]): void {
	if (typeof localStorage === 'undefined') return;
	try {
		localStorage.setItem(RECENT_COMMANDS_KEY, JSON.stringify(ids));
	} catch {
		// Ignore storage errors
	}
}

export const recentCommandIds = writable<string[]>(loadRecentCommands());

// Track recent command usage
function addToRecent(commandId: string): void {
	recentCommandIds.update((ids) => {
		// Remove if already exists
		const filtered = ids.filter((id) => id !== commandId);
		// Add to front
		const updated = [commandId, ...filtered].slice(0, MAX_RECENT);
		saveRecentCommands(updated);
		return updated;
	});
}

/**
 * Register a command
 */
export function registerCommand(command: Command): void {
	commands.update((cmds) => {
		// Replace if exists, otherwise add
		const existing = cmds.findIndex((c) => c.id === command.id);
		if (existing >= 0) {
			const updated = [...cmds];
			updated[existing] = command;
			return updated;
		}
		return [...cmds, command];
	});
}

/**
 * Register multiple commands
 */
export function registerCommands(newCommands: Command[]): void {
	newCommands.forEach(registerCommand);
}

/**
 * Unregister a command
 */
export function unregisterCommand(id: string): void {
	commands.update((cmds) => cmds.filter((c) => c.id !== id));
}

/**
 * Unregister all commands matching a prefix
 */
export function unregisterCommandsByPrefix(prefix: string): void {
	commands.update((cmds) => cmds.filter((c) => !c.id.startsWith(prefix)));
}

/**
 * Execute a command by ID
 */
export async function executeCommand(id: string): Promise<boolean> {
	const cmd = get(commands).find((c) => c.id === id);
	if (!cmd) {
		console.warn(`Command not found: ${id}`);
		return false;
	}

	// Check if enabled
	if (cmd.enabled && !cmd.enabled()) {
		return false;
	}

	try {
		await cmd.action();
		addToRecent(id);
		return true;
	} catch (e) {
		console.error(`Error executing command ${id}:`, e);
		return false;
	}
}

/**
 * Fuzzy search commands
 */
export function searchCommands(query: string): Command[] {
	const cmds = get(commands);
	if (!query.trim()) {
		return cmds;
	}

	const lowerQuery = query.toLowerCase();
	const terms = lowerQuery.split(/\s+/).filter(Boolean);

	return cmds
		.map((cmd) => {
			// Calculate match score
			let score = 0;
			const lowerLabel = cmd.label.toLowerCase();

			// Exact label match
			if (lowerLabel === lowerQuery) {
				score += 100;
			}
			// Label starts with query
			else if (lowerLabel.startsWith(lowerQuery)) {
				score += 50;
			}
			// Label contains query
			else if (lowerLabel.includes(lowerQuery)) {
				score += 30;
			}

			// Check keywords
			if (cmd.keywords) {
				for (const keyword of cmd.keywords) {
					const lowerKeyword = keyword.toLowerCase();
					if (lowerKeyword === lowerQuery) {
						score += 40;
					} else if (lowerKeyword.startsWith(lowerQuery)) {
						score += 20;
					} else if (lowerKeyword.includes(lowerQuery)) {
						score += 10;
					}
				}
			}

			// Check all terms match somewhere
			const allTermsMatch = terms.every((term) => {
				if (lowerLabel.includes(term)) return true;
				if (cmd.keywords?.some((k) => k.toLowerCase().includes(term))) return true;
				if (cmd.category.includes(term)) return true;
				return false;
			});

			if (!allTermsMatch) {
				score = 0;
			}

			return { cmd, score };
		})
		.filter((item) => item.score > 0)
		.sort((a, b) => b.score - a.score)
		.map((item) => item.cmd);
}

/**
 * Get commands grouped by category
 */
export function getCommandsByCategory(): Map<CommandCategory, Command[]> {
	const cmds = get(commands);
	const grouped = new Map<CommandCategory, Command[]>();

	const categoryOrder: CommandCategory[] = ['file', 'edit', 'view', 'layout', 'node', 'subgraph', 'decoration', 'tab', 'action', 'recipe'];

	for (const category of categoryOrder) {
		grouped.set(category, []);
	}

	for (const cmd of cmds) {
		const list = grouped.get(cmd.category) || [];
		list.push(cmd);
		grouped.set(cmd.category, list);
	}

	return grouped;
}

/**
 * Get recent commands
 */
export const recentCommands = derived([commands, recentCommandIds], ([$commands, $recentIds]) => {
	return $recentIds.map((id) => $commands.find((c) => c.id === id)).filter((c): c is Command => !!c);
});

/**
 * Open the command palette
 */
export function openCommandPalette(): void {
	commandPaletteOpen.set(true);
}

/**
 * Close the command palette
 */
export function closeCommandPalette(): void {
	commandPaletteOpen.set(false);
}

/**
 * Toggle the command palette
 */
export function toggleCommandPalette(): void {
	commandPaletteOpen.update((open) => !open);
}

/**
 * Category display names
 */
export const categoryLabels: Record<CommandCategory, string> = {
	file: 'File',
	edit: 'Edit',
	view: 'View',
	layout: 'Layout',
	node: 'Node',
	subgraph: 'Subgraph',
	decoration: 'Decoration',
	tab: 'Tab',
	action: 'Action',
	recipe: 'Recipe',
};
