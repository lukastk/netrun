/**
 * Command registration for netrun-ui
 *
 * This module registers all available commands and their keyboard shortcuts.
 * Import this at app initialization to set up the command system.
 */
import {
	registerCommands,
	unregisterCommandsByPrefix,
	type Command,
	type CommandCategory,
	openCommandPalette,
} from '$lib/stores/commandStore';
import { LAYOUT_ALGORITHMS, computeLayout } from '$lib/utils/autoLayout';
import { svelteFlowRef } from '$lib/stores/svelteFlowStore';
import { toasts } from '$lib/stores/toastStore';
import {
	recipes,
	getRecipePrompts,
	executeRecipe,
	showRecipeModal,
} from '$lib/stores/recipeStore';
import { getCurrentConfig, applyConfig, setAllDescExpanded, setAllPortGroupsCollapsed } from '$lib/stores/flowStore';
import { registerShortcuts, type ShortcutBinding } from '$lib/stores/keyboardStore';
import { availableActions, executeAction } from '$lib/stores/actionsStore';
import {
	undo,
	redo,
	history,
	isDirty,
	loadFromFile,
	saveToFile,
	reloadFile,
	clearFlow,
	createTab,
	closeActiveTab,
	switchToTabIndex,
	switchToNextTab,
	switchToPreviousTab,
	copySelectedNodes,
	pasteNodes,
	cutSelectedNodes,
	selectedNode,
	selectedNodeIds,
	selectedEdgeIds,
	nodes,
	edges,
	pushHistory,
	updateNodePositions,
	addNode,
	createRegularNode,
	createFactoryNode,
	createDecorationNode,
	updateFactoryNodePreview,
	validateAllNodes,
	createSubgraphFromSelection,
	currentFilePath,
	isInlineSubgraph,
	hasClipboardContent,
	extraData,
	updateExtraData,
	isExpandedChildNode,
	type DecorationType,
} from '$lib/stores/flowStore';
import { showFactorySelector } from '$lib/stores/factorySelectorStore';
import { get, derived } from 'svelte/store';
import { resolveFilePath } from '$lib/stores/fileExplorerStore';
import { showPrompt, showAlert, showConfirm } from '$lib/stores/modalStore';
import {
	toggleSubgraphExpansion,
	expandAllSubgraphs,
	collapseAllSubgraphs,
} from '$lib/stores/subgraphExpandStore';

// --- File Commands ---

const fileCommands: Command[] = [
	{
		id: 'file.new',
		label: 'New File...',
		category: 'file',
		keywords: ['clear', 'create', 'empty'],
		action: async () => {
			if (get(isDirty)) {
				const confirmed = await showConfirm({
					title: 'Unsaved Changes',
					message: 'You have unsaved changes. Create new file anyway?',
					confirmText: 'Create New',
					cancelText: 'Cancel',
				});
				if (!confirmed) return;
			}
			const inputPath = await showPrompt({
				title: 'Create New File',
				message: 'Enter filename (relative to current folder) or absolute path',
				placeholder: 'my_flow.netrun.json',
				defaultValue: 'my_flow.netrun.json',
				inputType: 'path',
				confirmText: 'Create',
			});
			if (!inputPath) return;

			const fullPath = resolveFilePath(inputPath);
			const format = fullPath.endsWith('.toml') ? 'toml' : 'json';
			const fileName = fullPath.split('/').pop() || 'Untitled';

			clearFlow(format, fileName);

			try {
				await saveToFile(fullPath);
			} catch (e) {
				await showAlert({
					title: 'Error',
					message: `Failed to create file: ${(e as Error).message}`,
				});
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
				const confirmed = await showConfirm({
					title: 'Unsaved Changes',
					message: 'You have unsaved changes. Create new file anyway?',
					confirmText: 'Create New',
					cancelText: 'Cancel',
				});
				if (!confirmed) return;
			}
			const inputPath = await showPrompt({
				title: 'Create New JSON File',
				message: 'Enter filename (relative to current folder) or absolute path',
				placeholder: 'my_flow.netrun.json',
				defaultValue: 'my_flow.netrun.json',
				inputType: 'path',
				confirmText: 'Create',
			});
			if (!inputPath) return;

			const fullPath = resolveFilePath(inputPath);
			const fileName = fullPath.split('/').pop() || 'Untitled';
			clearFlow('json', fileName);

			try {
				await saveToFile(fullPath);
			} catch (e) {
				await showAlert({
					title: 'Error',
					message: `Failed to create file: ${(e as Error).message}`,
				});
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
				const confirmed = await showConfirm({
					title: 'Unsaved Changes',
					message: 'You have unsaved changes. Create new file anyway?',
					confirmText: 'Create New',
					cancelText: 'Cancel',
				});
				if (!confirmed) return;
			}
			const inputPath = await showPrompt({
				title: 'Create New TOML File',
				message: 'Enter filename (relative to current folder) or absolute path',
				placeholder: 'my_flow.netrun.toml',
				defaultValue: 'my_flow.netrun.toml',
				inputType: 'path',
				confirmText: 'Create',
			});
			if (!inputPath) return;

			const fullPath = resolveFilePath(inputPath);
			const fileName = fullPath.split('/').pop() || 'Untitled';
			clearFlow('toml', fileName);

			try {
				await saveToFile(fullPath);
			} catch (e) {
				await showAlert({
					title: 'Error',
					message: `Failed to create file: ${(e as Error).message}`,
				});
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
			const path = await showPrompt({
				title: 'Open File',
				message: 'Enter file path to open',
				placeholder: '/path/to/file.netrun.json',
				inputType: 'path',
				confirmText: 'Open',
			});
			if (path) {
				try {
					await loadFromFile(path);
				} catch (e) {
					await showAlert({
						title: 'Error',
						message: `Open failed: ${(e as Error).message}`,
					});
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
					await showAlert({
						title: 'Saved',
						message: 'Subgraph changes saved to parent. Save the parent file to persist.',
					});
				} catch (e) {
					await showAlert({
						title: 'Error',
						message: `Save failed: ${(e as Error).message}`,
					});
				}
				return;
			}

			let path = get(currentFilePath);
			if (!path) {
				path = await showPrompt({
					title: 'Save File',
					message: 'Enter file path',
					placeholder: '/path/to/file.netrun.json',
					inputType: 'path',
					confirmText: 'Save',
				});
				if (!path) return;
			}

			try {
				await saveToFile(path);
			} catch (e) {
				await showAlert({
					title: 'Error',
					message: `Save failed: ${(e as Error).message}`,
				});
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
			const path = await showPrompt({
				title: 'Save As',
				message: 'Enter file path',
				placeholder: '/path/to/file.netrun.json',
				inputType: 'path',
				confirmText: 'Save',
			});
			if (!path) return;

			try {
				await saveToFile(path);
			} catch (e) {
				await showAlert({
					title: 'Error',
					message: `Save failed: ${(e as Error).message}`,
				});
			}
		},
	},
	{
		id: 'file.reload',
		label: 'Reload File from Disk',
		category: 'file',
		keywords: ['reload', 'refresh', 'revert', 'discard'],
		action: async () => {
			await reloadFile();
		},
		enabled: () => get(currentFilePath) !== null,
	},
	{
		id: 'file.closeTab',
		label: 'Close Tab',
		category: 'file',
		keywords: ['close', 'tab'],
		action: async () => {
			await closeActiveTab();
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
	{
		id: 'view.collapseDescriptions',
		label: 'Collapse All Descriptions',
		category: 'view',
		keywords: ['collapse', 'descriptions', 'hide'],
		action: () => setAllDescExpanded(false),
		enabled: () => get(nodes).some(n => n.data.description),
	},
	{
		id: 'view.expandDescriptions',
		label: 'Expand All Descriptions',
		category: 'view',
		keywords: ['expand', 'descriptions', 'show'],
		action: () => setAllDescExpanded(true),
		enabled: () => get(nodes).some(n => n.data.description),
	},
	{
		id: 'view.collapsePorts',
		label: 'Collapse All Ports',
		category: 'view',
		keywords: ['collapse', 'ports', 'hide', 'groups'],
		action: () => setAllPortGroupsCollapsed(true),
		enabled: () => get(nodes).length > 0,
	},
	{
		id: 'view.expandPorts',
		label: 'Expand All Ports',
		category: 'view',
		keywords: ['expand', 'ports', 'show', 'groups'],
		action: () => setAllPortGroupsCollapsed(false),
		enabled: () => get(nodes).length > 0,
	},
	{
		id: 'view.collapseAll',
		label: 'Collapse All',
		category: 'view',
		keywords: ['collapse', 'all', 'hide', 'descriptions', 'ports'],
		action: () => {
			setAllDescExpanded(false);
			setAllPortGroupsCollapsed(true);
		},
		enabled: () => get(nodes).length > 0,
	},
	{
		id: 'view.expandAll',
		label: 'Expand All',
		category: 'view',
		keywords: ['expand', 'all', 'show', 'descriptions', 'ports'],
		action: () => {
			setAllDescExpanded(true);
			setAllPortGroupsCollapsed(false);
		},
		enabled: () => get(nodes).length > 0,
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
			const factory = await showFactorySelector();
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
		action: async () => {
			const result = await validateAllNodes();
			if (result.valid) {
				await showAlert({
					title: 'Validation Passed',
					message: 'All nodes are valid!',
				});
			} else {
				const parts: string[] = [];
				if (result.errorCount > 0) {
					parts.push(`${result.errorCount} node(s) with errors (see nodes for details)`);
				}
				for (const err of result.configErrors) {
					parts.push(err);
				}
				await showAlert({
					title: 'Validation Failed',
					message: parts.join('\n'),
				});
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
				await showAlert({
					title: 'Cannot Create Subgraph',
					message: 'Please select at least 2 nodes to create a subgraph.',
				});
				return;
			}

			const name = await showPrompt({
				title: 'Create Subgraph',
				message: 'Enter a name for the subgraph',
				placeholder: 'MySubgraph',
				defaultValue: 'MySubgraph',
				confirmText: 'Create',
			});
			if (!name) return;

			try {
				const success = await createSubgraphFromSelection(name);
				if (!success) {
					await showAlert({
						title: 'Error',
						message: 'Failed to create subgraph.',
					});
				}
			} catch (e) {
				await showAlert({
					title: 'Error',
					message: `Error creating subgraph: ${(e as Error).message}`,
				});
			}
		},
		enabled: () => get(selectedNodeIds).size >= 2,
	},
	{
		id: 'subgraph.toggleExpand',
		label: 'Toggle Subgraph Expansion',
		category: 'subgraph',
		keywords: ['expand', 'collapse', 'inline', 'preview'],
		action: async () => {
			const selected = get(selectedNode);
			if (!selected || selected.data.nodeType !== 'subgraph') {
				await showAlert({
					title: 'No Subgraph Selected',
					message: 'Select a subgraph node to toggle expansion.',
				});
				return;
			}
			await toggleSubgraphExpansion(selected.id);
		},
		enabled: () => {
			const node = get(selectedNode);
			return node !== null && node.data.nodeType === 'subgraph';
		},
	},
	{
		id: 'subgraph.expandAll',
		label: 'Expand All Subgraphs',
		category: 'subgraph',
		keywords: ['expand', 'all', 'inline'],
		action: async () => {
			await expandAllSubgraphs();
		},
		enabled: () => get(nodes).some(n => n.data.nodeType === 'subgraph'),
	},
	{
		id: 'subgraph.collapseAll',
		label: 'Collapse All Subgraphs',
		category: 'subgraph',
		keywords: ['collapse', 'all'],
		action: () => {
			collapseAllSubgraphs();
		},
		enabled: () => get(nodes).some(n => n.data.nodeType === 'subgraph'),
	},
];

// --- Decoration Commands ---

function addDecorationOfType(type: DecorationType) {
	const node = createDecorationNode({ x: 200, y: 200 }, type);
	addNode(node);
	selectedNodeIds.set(new Set([node.id]));
	selectedEdgeIds.set(new Set());
}

const decorationCommands: Command[] = [
	{
		id: 'decoration.addRectangle',
		label: 'Add Rectangle Decoration',
		category: 'decoration',
		keywords: ['shape', 'box', 'decoration'],
		action: () => addDecorationOfType('rectangle'),
	},
	{
		id: 'decoration.addRoundedRectangle',
		label: 'Add Rounded Rectangle Decoration',
		category: 'decoration',
		keywords: ['shape', 'box', 'rounded', 'decoration'],
		action: () => addDecorationOfType('rounded-rectangle'),
	},
	{
		id: 'decoration.addCircle',
		label: 'Add Circle Decoration',
		category: 'decoration',
		keywords: ['shape', 'oval', 'decoration'],
		action: () => addDecorationOfType('circle'),
	},
	{
		id: 'decoration.addTriangle',
		label: 'Add Triangle Decoration',
		category: 'decoration',
		keywords: ['shape', 'decoration'],
		action: () => addDecorationOfType('triangle'),
	},
	{
		id: 'decoration.addDivider',
		label: 'Add Divider',
		category: 'decoration',
		keywords: ['separator', 'divider', 'line', 'decoration'],
		action: () => addDecorationOfType('divider'),
	},
	{
		id: 'decoration.addLabel',
		label: 'Add Label',
		category: 'decoration',
		keywords: ['label', 'annotation', 'decoration', 'text'],
		action: () => addDecorationOfType('label'),
	},
	{
		id: 'decoration.addTextbox',
		label: 'Add Textbox',
		category: 'decoration',
		keywords: ['text', 'multiline', 'paragraph', 'decoration'],
		action: () => addDecorationOfType('textbox'),
	},
	{
		id: 'decoration.addImage',
		label: 'Add Image Decoration',
		category: 'decoration',
		keywords: ['picture', 'photo', 'decoration', 'image'],
		action: () => addDecorationOfType('image'),
	},
];

// --- Recipe Commands ---

const recipeStaticCommands: Command[] = [
	{
		id: 'recipe.add',
		label: 'Add Recipe',
		category: 'recipe',
		keywords: ['create', 'new', 'recipe', 'script'],
		action: async () => {
			// Prompt for recipe name
			const name = await showPrompt({
				title: 'Add Recipe',
				message: 'Enter a name for the recipe (used as identifier)',
				placeholder: 'my_recipe',
				confirmText: 'Next',
			});
			if (!name) return;

			// Validate name (alphanumeric + underscore)
			if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
				await showAlert({
					title: 'Invalid Name',
					message: 'Recipe name must start with a letter or underscore, and contain only letters, numbers, and underscores.',
				});
				return;
			}

			// Check if recipe already exists
			const currentExtra = get(extraData) as Record<string, unknown> | null;
			const existingRecipes = (currentExtra?.recipes as Record<string, unknown>) ?? {};
			if (name in existingRecipes) {
				await showAlert({
					title: 'Recipe Exists',
					message: `A recipe named "${name}" already exists.`,
				});
				return;
			}

			// Prompt for path
			const path = await showPrompt({
				title: 'Recipe Path',
				message: 'Enter the path to the recipe Python file (relative to netrun file)',
				placeholder: './recipes/my_recipe.py',
				defaultValue: `./recipes/${name}.py`,
				inputType: 'path',
				confirmText: 'Next',
			});
			if (!path) return;

			// Prompt for description (optional)
			const description = await showPrompt({
				title: 'Recipe Description',
				message: 'Enter a description (optional, shown in command palette)',
				placeholder: 'Transforms the graph by...',
				confirmText: 'Add Recipe',
			});

			// Add recipe to extraData
			updateExtraData({
				recipes: {
					...existingRecipes,
					[name]: {
						path,
						...(description ? { description } : {}),
					},
				},
			});

			await showAlert({
				title: 'Recipe Added',
				message: `Recipe "${name}" added. Create the Python file at: ${path}`,
			});
		},
		enabled: () => get(currentFilePath) !== null,
	},
];

// --- Layout Commands ---

async function runLayout(algorithmId: string): Promise<void> {
	const currentNodes = get(nodes);
	// Filter out expanded child nodes and decoration nodes - only layout parent-level functional nodes
	const layoutNodes = currentNodes.filter(n => !isExpandedChildNode(n.id) && n.data.nodeType !== 'decoration');
	if (layoutNodes.length < 2) {
		toasts.info(layoutNodes.length === 0 ? 'No nodes to layout.' : 'Need at least 2 nodes to layout.');
		return;
	}

	const flowRef = get(svelteFlowRef);
	if (!flowRef) {
		toasts.error('Flow editor not available.');
		return;
	}

	const measuredNodes = flowRef.getNodes();
	const nodeDimensions = new Map<string, { width: number; height: number }>();
	for (const n of measuredNodes) {
		if (n.measured?.width && n.measured?.height) {
			nodeDimensions.set(n.id, { width: n.measured.width, height: n.measured.height });
		}
	}

	// Filter edges to only include parent-level edges
	const currentEdges = get(edges).filter(e =>
		!isExpandedChildNode(e.source) && !isExpandedChildNode(e.target)
	);
	const result = await computeLayout(layoutNodes, currentEdges, algorithmId, nodeDimensions);

	pushHistory();
	updateNodePositions(result.positions);
	flowRef.fitView({ padding: 0.2, maxZoom: 1.5 });
}

const layoutCommands: Command[] = LAYOUT_ALGORITHMS.map((algo) => ({
	id: `layout.${algo.id}`,
	label: `Layout: ${algo.label}`,
	category: 'layout' as CommandCategory,
	keywords: ['layout', 'arrange', 'auto', 'graph', algo.elkAlgorithm],
	shortcut: algo.id === 'layered-lr' ? '\u21E7\u2318L' : undefined,
	action: () => runLayout(algo.id),
	enabled: () => get(nodes).length >= 2,
}));

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
	{ key: 'r', metaKey: true, shiftKey: true, commandId: 'file.reload' },
	{ key: 'w', metaKey: true, commandId: 'file.closeTab' },

	// Edit
	{ key: 'z', metaKey: true, commandId: 'edit.undo' },
	{ key: 'z', metaKey: true, shiftKey: true, commandId: 'edit.redo' },
	{ key: 'c', metaKey: true, commandId: 'edit.copy' },
	{ key: 'v', metaKey: true, commandId: 'edit.paste' },
	{ key: 'x', metaKey: true, commandId: 'edit.cut' },

	// View
	{ key: 'p', metaKey: true, shiftKey: true, commandId: 'view.commandPalette' },

	// Layout
	{ key: 'l', metaKey: true, shiftKey: true, commandId: 'layout.layered-lr' },

	// Subgraph
	{ key: 'g', metaKey: true, commandId: 'subgraph.create' },
	{ key: 'e', metaKey: true, commandId: 'subgraph.toggleExpand' },

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

const ACTION_CMD_PREFIX = 'action.run.';
const RECIPE_CMD_PREFIX = 'recipe.run.';

/**
 * Derived store that produces action commands for the currently selected node.
 * Returns empty array when no single node is selected.
 */
const actionCommands = derived(
	[selectedNode, availableActions],
	([$selectedNode, $availableActions]): Command[] => {
		if (!$selectedNode) return [];

		const nodeName = $selectedNode.data.label || $selectedNode.id;

		return $availableActions.map((action) => ({
			id: `${ACTION_CMD_PREFIX}${action.id}`,
			label: `Run: ${action.label} on "${nodeName}"`,
			category: 'action' as CommandCategory,
			keywords: ['run', 'execute', 'action', action.label, nodeName],
			action: () => executeAction(action),
		}));
	}
);

/**
 * Run a recipe by name and path.
 * Fetches prompts, shows modal if needed, then executes.
 */
async function runRecipe(name: string, path: string): Promise<void> {
	try {
		const config = getCurrentConfig();
		const prompts = await getRecipePrompts(path, config);

		if (prompts.length === 0) {
			// No prompts - execute immediately
			const newConfig = await executeRecipe(path, config, {});
			applyConfig(newConfig);
		} else {
			// Show modal for input collection
			showRecipeModal(name, prompts, async (inputs) => {
				try {
					const newConfig = await executeRecipe(path, config, inputs);
					applyConfig(newConfig);
				} catch (e) {
					// Error already shown by executeRecipe
				}
			});
		}
	} catch (e) {
		// Error already shown by getRecipePrompts/executeRecipe
	}
}

/**
 * Derived store that produces recipe commands from the current file's extraData.
 */
const recipeCommands = derived(
	recipes,
	($recipes): Command[] => {
		return Object.entries($recipes).map(([name, def]) => ({
			id: `${RECIPE_CMD_PREFIX}${name}`,
			label: `Recipe: ${name}`,
			category: 'recipe' as CommandCategory,
			keywords: ['recipe', 'transform', name, ...(def.description ? [def.description] : [])],
			action: () => runRecipe(name, def.path),
		}));
	}
);

/**
 * Initialize all commands and shortcuts
 */
export function initializeCommands(): void {
	// Register all commands
	registerCommands([
		...fileCommands,
		...editCommands,
		...viewCommands,
		...layoutCommands,
		...nodeCommands,
		...subgraphCommands,
		...decorationCommands,
		...recipeStaticCommands,
		...tabCommands,
	]);

	// Register keyboard shortcuts
	registerShortcuts(keyboardShortcuts);

	// Dynamically register/unregister action commands based on selected node
	actionCommands.subscribe((cmds) => {
		unregisterCommandsByPrefix(ACTION_CMD_PREFIX);
		if (cmds.length > 0) {
			registerCommands(cmds);
		}
	});

	// Dynamically register/unregister recipe commands based on current file's extraData
	recipeCommands.subscribe((cmds) => {
		unregisterCommandsByPrefix(RECIPE_CMD_PREFIX);
		if (cmds.length > 0) {
			registerCommands(cmds);
		}
	});
}
