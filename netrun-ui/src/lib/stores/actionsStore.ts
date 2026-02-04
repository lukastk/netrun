/**
 * Actions Store - Manages node actions and their execution
 *
 * Actions are commands that can be run from the UI with template variables
 * for dynamic paths based on node name, project root, etc.
 */
import { writable, derived, get } from 'svelte/store';
import { api } from '$lib/api';
import { activeTab, graphMeta, updateGraphMetaLive, currentFilePath, selectedNode } from './flowStore';

// Action definition
export interface Action {
	id: string;
	label: string;
	command: string;
	icon?: string;
}

// Project-level action settings stored in meta.ui
export interface ActionSettings {
	projectRoot?: string;
	defaultCmd?: string;
	env?: Record<string, string>;
	actions?: Action[];
}

// Execution state
export interface ActionExecution {
	actionId: string;
	status: 'running' | 'success' | 'error';
	stdout?: string;
	stderr?: string;
	exitCode?: number;
	resolvedCommand?: string;
}

// Store for tracking action execution state
export const actionExecutions = writable<Map<string, ActionExecution>>(new Map());

// Derived: get action settings from graph meta
export const actionSettings = derived(
	graphMeta,
	($graphMeta): ActionSettings => {
		const ui = ($graphMeta as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		return {
			projectRoot: ui?.projectRoot as string | undefined,
			defaultCmd: ui?.defaultCmd as string | undefined,
			env: ui?.env as Record<string, string> | undefined,
			actions: ui?.actions as Action[] | undefined,
		};
	}
);

// Derived: get project-level actions
export const projectActions = derived(
	actionSettings,
	($settings): Action[] => $settings.actions || []
);

// Derived: get node-specific actions for selected node
export const nodeActions = derived(
	selectedNode,
	($selectedNode): Action[] => {
		if (!$selectedNode) return [];
		const config = $selectedNode.data._config as Record<string, unknown> | undefined;
		const meta = config?.meta as Record<string, unknown> | undefined;
		const ui = meta?.ui as Record<string, unknown> | undefined;
		return (ui?.actions as Action[]) || [];
	}
);

// Derived: get node-level environment variable overrides
export const nodeEnv = derived(
	selectedNode,
	($selectedNode): Record<string, string> | undefined => {
		if (!$selectedNode) return undefined;
		const config = $selectedNode.data._config as Record<string, unknown> | undefined;
		const meta = config?.meta as Record<string, unknown> | undefined;
		const ui = meta?.ui as Record<string, unknown> | undefined;
		const env = ui?.env as Record<string, string> | undefined;
		return env && Object.keys(env).length > 0 ? env : undefined;
	}
);

// Derived: all actions available for the selected node
export const availableActions = derived(
	[projectActions, nodeActions],
	([$projectActions, $nodeActions]): Action[] => {
		// Combine project and node actions, with node actions taking precedence
		const actionMap = new Map<string, Action>();

		for (const action of $projectActions) {
			actionMap.set(action.id, action);
		}

		for (const action of $nodeActions) {
			actionMap.set(action.id, action);
		}

		return Array.from(actionMap.values());
	}
);

/**
 * Update project-level action settings
 */
export function updateActionSettings(updates: Partial<ActionSettings>): void {
	const meta = get(graphMeta) || {};
	const ui = (meta as Record<string, unknown>).ui as Record<string, unknown> || {};

	const newUi = {
		...ui,
		...(updates.projectRoot !== undefined && { projectRoot: updates.projectRoot }),
		...(updates.defaultCmd !== undefined && { defaultCmd: updates.defaultCmd }),
		...(updates.env !== undefined && { env: updates.env }),
		...(updates.actions !== undefined && { actions: updates.actions }),
	};

	updateGraphMetaLive({ ui: newUi });
}

/**
 * Add a new project-level action
 */
export function addProjectAction(action: Action): void {
	const settings = get(actionSettings);
	const actions = [...(settings.actions || []), action];
	updateActionSettings({ actions });
}

/**
 * Update a project-level action
 */
export function updateProjectAction(actionId: string, updates: Partial<Action>): void {
	const settings = get(actionSettings);
	const actions = (settings.actions || []).map(a =>
		a.id === actionId ? { ...a, ...updates } : a
	);
	updateActionSettings({ actions });
}

/**
 * Remove a project-level action
 */
export function removeProjectAction(actionId: string): void {
	const settings = get(actionSettings);
	const actions = (settings.actions || []).filter(a => a.id !== actionId);
	updateActionSettings({ actions });
}

/**
 * Generate a unique action ID
 */
export function generateActionId(): string {
	return `action-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Execute an action for the selected node
 */
export async function executeAction(action: Action): Promise<void> {
	const node = get(selectedNode);
	const settings = get(actionSettings);
	const filePath = get(currentFilePath);
	const nodeEnvVars = get(nodeEnv);

	// Mark as running
	actionExecutions.update(map => {
		map.set(action.id, { actionId: action.id, status: 'running' });
		return new Map(map);
	});

	try {
		const result = await api.executeAction({
			command: action.command,
			node_name: node?.data.label,
			node_id: node?.id,
			net_file_path: filePath || undefined,
			project_root: settings.projectRoot,
			default_cmd: settings.defaultCmd,
			env: settings.env,
			node_env: nodeEnvVars,
		});

		actionExecutions.update(map => {
			map.set(action.id, {
				actionId: action.id,
				status: result.success ? 'success' : 'error',
				stdout: result.stdout,
				stderr: result.stderr,
				exitCode: result.exit_code,
				resolvedCommand: result.resolved_command,
			});
			return new Map(map);
		});

		// Clear success status after a delay
		if (result.success) {
			setTimeout(() => {
				actionExecutions.update(map => {
					if (map.get(action.id)?.status === 'success') {
						map.delete(action.id);
					}
					return new Map(map);
				});
			}, 3000);
		}
	} catch (error) {
		actionExecutions.update(map => {
			map.set(action.id, {
				actionId: action.id,
				status: 'error',
				stderr: (error as Error).message,
			});
			return new Map(map);
		});
	}
}

/**
 * Clear execution state for an action
 */
export function clearActionExecution(actionId: string): void {
	actionExecutions.update(map => {
		map.delete(actionId);
		return new Map(map);
	});
}

/**
 * Resolve a command template for preview
 */
export async function resolveCommand(command: string): Promise<string> {
	const node = get(selectedNode);
	const settings = get(actionSettings);
	const filePath = get(currentFilePath);
	const nodeEnvVars = get(nodeEnv);

	try {
		const result = await api.resolveTemplate(command, {
			node_name: node?.data.label,
			node_id: node?.id,
			net_file_path: filePath || undefined,
			project_root: settings.projectRoot,
			default_cmd: settings.defaultCmd,
			env: settings.env,
			node_env: nodeEnvVars,
		});
		return result.resolved;
	} catch {
		return command; // Return original on error
	}
}
