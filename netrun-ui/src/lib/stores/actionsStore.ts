/**
 * Actions Store - Manages node actions and their execution
 *
 * Actions are commands that can be run from the UI with template variables
 * for dynamic paths based on node name, project root, etc.
 */
import { writable, derived, get } from 'svelte/store';
import { api } from '$lib/api';
import { activeTab, graphExtra, updateGraphExtraLive, extraData, updateExtraDataLive, currentFilePath, selectedNode } from './flowStore';
import { toasts } from './toastStore';

// Action definition
export interface Action {
	id: string;
	label: string;
	command: string;
}

// Project-level action settings stored in extra.ui
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

// Derived: get action settings from extraData (project_root) and graph meta (rest)
export const actionSettings = derived(
	[graphExtra, extraData],
	([$graphExtra, $extraData]): ActionSettings => {
		const ui = ($graphExtra as Record<string, unknown>)?.ui as Record<string, unknown> | undefined;
		const extra = $extraData as Record<string, unknown> | null;
		// project_root: prefer extraData.project_root, fall back to legacy graphExtra.ui.projectRoot
		const projectRoot = (extra?.project_root as string | undefined) ?? (ui?.projectRoot as string | undefined);
		return {
			projectRoot,
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
		const extra = config?.extra as Record<string, unknown> | undefined;
		const ui = extra?.ui as Record<string, unknown> | undefined;
		return (ui?.actions as Action[]) || [];
	}
);

// Derived: get node-level environment variable overrides
// Merges node_vars (global then node-level) with legacy ui.env overrides.
// Precedence (lowest to highest): global node_vars < node node_vars < ui.env
export const nodeEnv = derived(
	[selectedNode, extraData],
	([$selectedNode, $extraData]): Record<string, string> | undefined => {
		if (!$selectedNode) return undefined;

		const merged: Record<string, string> = {};

		// Resolve a node_var value to a string.
		// Plain strings pass through; VarRef objects ({"$env": "X", "default": "Y"})
		// use the default value since we can't resolve env vars client-side.
		const resolveVarValue = (value: unknown): string | undefined => {
			if (typeof value === 'string') return value;
			if (value && typeof value === 'object') {
				const ref = value as Record<string, unknown>;
				if (ref.default != null) return String(ref.default);
			}
			return undefined;
		};

		// 1. Global node_vars (from extraData.node_vars)
		const extra = $extraData as Record<string, unknown> | null;
		const globalVars = extra?.node_vars as Record<string, { value: unknown }> | undefined;
		if (globalVars) {
			for (const [key, v] of Object.entries(globalVars)) {
				const resolved = resolveVarValue(v.value);
				if (resolved !== undefined) merged[key] = resolved;
			}
		}

		// 2. Node-level node_vars (from execution_config.node_vars)
		const config = $selectedNode.data._config as Record<string, unknown> | undefined;
		const executionConfig = config?.execution_config as Record<string, unknown> | undefined;
		const nodeVars = executionConfig?.node_vars as Record<string, { value: unknown }> | undefined;
		if (nodeVars) {
			for (const [key, v] of Object.entries(nodeVars)) {
				const resolved = resolveVarValue(v.value);
				if (resolved !== undefined) merged[key] = resolved;
			}
		}

		// 3. Legacy ui.env overrides (highest precedence)
		const nodeExtra = config?.extra as Record<string, unknown> | undefined;
		const ui = nodeExtra?.ui as Record<string, unknown> | undefined;
		const env = ui?.env as Record<string, string> | undefined;
		if (env) {
			Object.assign(merged, env);
		}

		return Object.keys(merged).length > 0 ? merged : undefined;
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
	const graphExtraVal = get(graphExtra) || {};
	const ui = (graphExtraVal as Record<string, unknown>).ui as Record<string, unknown> || {};

	// projectRoot is stored in extraData.project_root (top-level NetConfig field)
	if (updates.projectRoot !== undefined) {
		updateExtraDataLive({ project_root: updates.projectRoot || undefined });
		// Remove legacy key from graphExtra.ui if present
		if ('projectRoot' in ui) {
			const { projectRoot: _, ...restUi } = ui;
			updateGraphExtraLive({ ui: restUi });
		}
	}

	// Other settings remain in graphExtra.ui
	const uiUpdates: Record<string, unknown> = {
		...ui,
		...(updates.defaultCmd !== undefined && { defaultCmd: updates.defaultCmd }),
		...(updates.env !== undefined && { env: updates.env }),
		...(updates.actions !== undefined && { actions: updates.actions }),
	};
	// Remove legacy projectRoot from ui if it got merged in from spread
	delete uiUpdates.projectRoot;

	updateGraphExtraLive({ ui: uiUpdates });
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
		// Serialize node config as JSON for $NODE_CONFIG variable
		const nodeConfig = node ? JSON.stringify({
			name: node.data.label,
			nodeType: node.data.nodeType,
			inPorts: node.data.inPorts,
			outPorts: node.data.outPorts,
			...(node.data.nodeType !== 'subgraph' && (node.data as any).factory ? { factory: (node.data as any).factory } : {}),
			...(node.data.nodeType !== 'subgraph' && (node.data as any).factoryArgs ? { factoryArgs: (node.data as any).factoryArgs } : {}),
			...(node.data.nodeType !== 'subgraph' && (node.data as any)._config ? { _config: (node.data as any)._config } : {}),
		}) : undefined;

		const result = await api.executeAction({
			command: action.command,
			node_name: node?.data.label,
			net_file_path: filePath || undefined,
			project_root: settings.projectRoot,
			default_cmd: settings.defaultCmd,
			env: settings.env,
			node_env: nodeEnvVars,
			node_config: nodeConfig,
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

		// Show stdout as a toast notification
		if (result.stdout?.trim()) {
			toasts.info(result.stdout.trim());
		}

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
		const nodeConfig = node ? JSON.stringify({
			name: node.data.label,
			nodeType: node.data.nodeType,
			inPorts: node.data.inPorts,
			outPorts: node.data.outPorts,
			...(node.data.nodeType !== 'subgraph' && (node.data as any).factory ? { factory: (node.data as any).factory } : {}),
			...(node.data.nodeType !== 'subgraph' && (node.data as any).factoryArgs ? { factoryArgs: (node.data as any).factoryArgs } : {}),
			...(node.data.nodeType !== 'subgraph' && (node.data as any)._config ? { _config: (node.data as any)._config } : {}),
		}) : undefined;

		// For preview, pass $VAR_NAME as fallback so unresolved vars stay visible
		const result = await api.resolveTemplate(command, {
			node_name: node?.data.label || '$NODE_NAME',
			net_file_path: filePath || '$NET_FILE_PATH',
			project_root: settings.projectRoot || '$PROJECT_ROOT',
			default_cmd: settings.defaultCmd || '$DEFAULT_CMD',
			env: settings.env,
			node_env: nodeEnvVars,
			node_config: nodeConfig || '$NODE_CONFIG',
		});
		return result.resolved;
	} catch {
		return command; // Return original on error
	}
}
