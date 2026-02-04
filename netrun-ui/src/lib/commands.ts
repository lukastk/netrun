/**
 * Command registration for netrun-ui
 *
 * This module registers all available commands and their keyboard shortcuts.
 * Import this at app initialization to set up the command system.
 */
import {
	registerCommands,
	type Command,
	openCommandPalette,
} from '$lib/stores/commandStore';
import { registerShortcuts, type ShortcutBinding } from '$lib/stores/keyboardStore';
import {
	undo,
	redo,
	history,
	isDirty,
	loadFromFile,
	saveToFile,
	clearFlow,
	createTab,
	closeActiveTab,
	switchToTabIndex,
	switchToNextTab,
	switchToPreviousTab,
	copySelectedNodes,
	pasteNodes,
	cutSelectedNodes,
	selectedNodeIds,
	nodes,
	addNode,
	createRegularNode,
	createFactoryNode,
	updateFactoryNodePreview,
	validateAllNodes,
	createSubgraphFromSelection,
	currentFilePath,
	isInlineSubgraph,
	hasClipboardContent,
} from '$lib/stores/flowStore';
import { get } from 'svelte/store';

// --- File Commands ---

const fileCommands: Command[] = [
	{
		id: 'file.new',
		label: 'New File...',
		category: 'file',
		keywords: ['clear', 'create', 'empty'],
		action: async () => {
			if (get(isDirty)) {
				if (!confirm('You have unsaved changes. Create new file anyway?')) {
					return;
				}
			}
			const path = prompt('Enter full file path (e.g., /path/to/my_flow.netrun.json):');
			if (!path) return;

			// Determine format from extension
			const format = path.endsWith('.toml') ? 'toml' : 'json';
			const fileName = path.split('/').pop() || 'Untitled';

			// Create the new file and save it immediately
			clearFlow(format, fileName);

			try {
				await saveToFile(path);
			} catch (e) {
				alert(`Failed to create file: ${(e as Error).message}`);
			}
		},
	},
	{
		id: 'file.newJson',
		label: 'New JSON File',
		category: 'file',
		keywords: ['clear', 'create', 'empty', 'json'],
		action: async () => {
			if (get(isDirty)) {
				if (!confirm('You have unsaved changes. Create new file anyway?')) {
					return;
				}
			}
			const path = prompt('Enter full file path (e.g., /path/to/my_flow.netrun.json):');
			if (!path) return;

			const fileName = path.split('/').pop() || 'Untitled';
			clearFlow('json', fileName);

			try {
				await saveToFile(path);
			} catch (e) {
				alert(`Failed to create file: ${(e as Error).message}`);
			}
		},
	},
	{
		id: 'file.newToml',
		label: 'New TOML File',
		category: 'file',
		keywords: ['clear', 'create', 'empty', 'toml'],
		action: async () => {
			if (get(isDirty)) {
				if (!confirm('You have unsaved changes. Create new file anyway?')) {
					return;
				}
			}
			const path = prompt('Enter full file path (e.g., /path/to/my_flow.netrun.toml):');
			if (!path) return;

			const fileName = path.split('/').pop() || 'Untitled';
			clearFlow('toml', fileName);

			try {
				await saveToFile(path);
			} catch (e) {
				alert(`Failed to create file: ${(e as Error).message}`);
			}
		},
	},
	{
		id: 'file.newTab',
		label: 'New Tab',
		category: 'file',
		keywords: ['tab', 'create'],
		action: () => {
			createTab();
		},
	},
	{
		id: 'file.open',
		label: 'Open File',
		category: 'file',
		keywords: ['load', 'browse'],
		action: async () => {
			const path = prompt('Enter file path to open:');
			if (path) {
				try {
					await loadFromFile(path);
				} catch (e) {
					alert(`Open failed: ${(e as Error).message}`);
				}
			}
		},
	},
	{
		id: 'file.save',
		label: 'Save',
		category: 'file',
		keywords: ['write', 'store'],
		action: async () => {
			// Handle inline subgraphs
			if (get(isInlineSubgraph)) {
				try {
					await saveToFile();
					alert('Subgraph changes saved to parent. Save the parent file to persist.');
				} catch (e) {
					alert(`Save failed: ${(e as Error).message}`);
				}
				return;
			}

			let path = get(currentFilePath);
			if (!path) {
				path = prompt('Enter file path (e.g., /path/to/file.netrun.json):');
				if (!path) return;
			}

			try {
				await saveToFile(path);
			} catch (e) {
				alert(`Save failed: ${(e as Error).message}`);
			}
		},
		enabled: () => get(isDirty),
	},
	{
		id: 'file.saveAs',
		label: 'Save As...',
		category: 'file',
		keywords: ['write', 'export'],
		action: async () => {
			const path = prompt('Enter file path (e.g., /path/to/file.netrun.json):');
			if (!path) return;

			try {
				await saveToFile(path);
			} catch (e) {
				alert(`Save failed: ${(e as Error).message}`);
			}
		},
	},
	{
		id: 'file.closeTab',
		label: 'Close Tab',
		category: 'file',
		keywords: ['close', 'tab'],
		action: () => {
			closeActiveTab();
		},
	},
];

// --- Edit Commands ---

const editCommands: Command[] = [
	{
		id: 'edit.undo',
		label: 'Undo',
		category: 'edit',
		keywords: ['revert', 'back'],
		action: () => undo(),
		enabled: () => get(history).past.length > 0,
	},
	{
		id: 'edit.redo',
		label: 'Redo',
		category: 'edit',
		keywords: ['forward'],
		action: () => redo(),
		enabled: () => get(history).future.length > 0,
	},
	{
		id: 'edit.copy',
		label: 'Copy',
		category: 'edit',
		keywords: ['clipboard'],
		action: () => copySelectedNodes(),
		enabled: () => get(selectedNodeIds).size > 0,
	},
	{
		id: 'edit.paste',
		label: 'Paste',
		category: 'edit',
		keywords: ['clipboard'],
		action: () => pasteNodes(),
		enabled: () => hasClipboardContent(),
	},
	{
		id: 'edit.cut',
		label: 'Cut',
		category: 'edit',
		keywords: ['clipboard', 'delete'],
		action: () => cutSelectedNodes(),
		enabled: () => get(selectedNodeIds).size > 0,
	},
];

// --- View Commands ---

const viewCommands: Command[] = [
	{
		id: 'view.commandPalette',
		label: 'Command Palette',
		category: 'view',
		keywords: ['search', 'commands', 'menu'],
		action: () => openCommandPalette(),
	},
];

// --- Node Commands ---

const nodeCommands: Command[] = [
	{
		id: 'node.add',
		label: 'Add Node',
		category: 'node',
		keywords: ['create', 'new', 'regular'],
		action: () => {
			const newNode = createRegularNode({ x: 200, y: 200 });
			addNode(newNode);
		},
	},
	{
		id: 'node.addFactory',
		label: 'Add Factory Node',
		category: 'node',
		keywords: ['create', 'new', 'factory'],
		action: async () => {
			const factory = prompt('Enter factory import path:', 'netrun.node_factories.function');
			if (factory) {
				const newNode = createFactoryNode({ x: 200, y: 200 }, factory);
				addNode(newNode);

				try {
					await updateFactoryNodePreview(newNode.id);
				} catch (e) {
					console.warn('Could not get factory preview:', e);
				}
			}
		},
	},
	{
		id: 'node.validate',
		label: 'Validate All Nodes',
		category: 'node',
		keywords: ['check', 'errors'],
		action: () => {
			const result = validateAllNodes();
			if (result.valid) {
				alert('All nodes are valid!');
			} else {
				alert(`Validation found ${result.errorCount} node(s) with errors.`);
			}
		},
	},
];

// --- Subgraph Commands ---

const subgraphCommands: Command[] = [
	{
		id: 'subgraph.create',
		label: 'Create Subgraph from Selection',
		category: 'subgraph',
		keywords: ['group', 'collapse'],
		action: async () => {
			const selected = get(selectedNodeIds);
			if (selected.size < 2) {
				alert('Please select at least 2 nodes to create a subgraph');
				return;
			}

			const name = prompt('Enter subgraph name:', 'MySubgraph');
			if (!name) return;

			try {
				const success = await createSubgraphFromSelection(name);
				if (!success) {
					alert('Failed to create subgraph');
				}
			} catch (e) {
				alert(`Error creating subgraph: ${(e as Error).message}`);
			}
		},
		enabled: () => get(selectedNodeIds).size >= 2,
	},
];

// --- Tab Commands ---

const tabCommands: Command[] = [
	{
		id: 'tab.next',
		label: 'Next Tab',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToNextTab(),
	},
	{
		id: 'tab.previous',
		label: 'Previous Tab',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToPreviousTab(),
	},
	{
		id: 'tab.1',
		label: 'Go to Tab 1',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(0),
	},
	{
		id: 'tab.2',
		label: 'Go to Tab 2',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(1),
	},
	{
		id: 'tab.3',
		label: 'Go to Tab 3',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(2),
	},
	{
		id: 'tab.4',
		label: 'Go to Tab 4',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(3),
	},
	{
		id: 'tab.5',
		label: 'Go to Tab 5',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(4),
	},
	{
		id: 'tab.6',
		label: 'Go to Tab 6',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(5),
	},
	{
		id: 'tab.7',
		label: 'Go to Tab 7',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(6),
	},
	{
		id: 'tab.8',
		label: 'Go to Tab 8',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(7),
	},
	{
		id: 'tab.9',
		label: 'Go to Tab 9',
		category: 'tab',
		keywords: ['switch'],
		action: () => switchToTabIndex(8),
	},
];

// --- Keyboard Shortcuts ---

const keyboardShortcuts: ShortcutBinding[] = [
	// File
	{ key: 'n', metaKey: true, commandId: 'file.new' },
	{ key: 't', metaKey: true, commandId: 'file.newTab' },
	{ key: 'o', metaKey: true, commandId: 'file.open' },
	{ key: 's', metaKey: true, commandId: 'file.save' },
	{ key: 's', metaKey: true, shiftKey: true, commandId: 'file.saveAs' },
	{ key: 'w', metaKey: true, commandId: 'file.closeTab' },

	// Edit
	{ key: 'z', metaKey: true, commandId: 'edit.undo' },
	{ key: 'z', metaKey: true, shiftKey: true, commandId: 'edit.redo' },
	{ key: 'c', metaKey: true, commandId: 'edit.copy' },
	{ key: 'v', metaKey: true, commandId: 'edit.paste' },
	{ key: 'x', metaKey: true, commandId: 'edit.cut' },

	// View
	{ key: 'p', metaKey: true, shiftKey: true, commandId: 'view.commandPalette' },

	// Subgraph
	{ key: 'g', metaKey: true, commandId: 'subgraph.create' },

	// Tab navigation
	{ key: 'Tab', ctrlKey: true, commandId: 'tab.next' },
	{ key: 'Tab', ctrlKey: true, shiftKey: true, commandId: 'tab.previous' },
	{ key: '1', metaKey: true, commandId: 'tab.1' },
	{ key: '2', metaKey: true, commandId: 'tab.2' },
	{ key: '3', metaKey: true, commandId: 'tab.3' },
	{ key: '4', metaKey: true, commandId: 'tab.4' },
	{ key: '5', metaKey: true, commandId: 'tab.5' },
	{ key: '6', metaKey: true, commandId: 'tab.6' },
	{ key: '7', metaKey: true, commandId: 'tab.7' },
	{ key: '8', metaKey: true, commandId: 'tab.8' },
	{ key: '9', metaKey: true, commandId: 'tab.9' },
];

/**
 * Initialize all commands and shortcuts
 */
export function initializeCommands(): void {
	// Register all commands
	registerCommands([
		...fileCommands,
		...editCommands,
		...viewCommands,
		...nodeCommands,
		...subgraphCommands,
		...tabCommands,
	]);

	// Register keyboard shortcuts
	registerShortcuts(keyboardShortcuts);
}
