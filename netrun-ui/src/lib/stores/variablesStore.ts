/**
 * Variables Store - Manages node variables at net and node levels
 *
 * Node variables are typed key-value pairs accessible via ctx.vars in node functions.
 * Net-level vars serve as defaults; node-level vars override for the same name.
 */
import { derived, get } from 'svelte/store';
import {
	extraData,
	updateExtraDataLive,
	nodes,
	selectedNode,
	activeTab,
} from './flowStore';
import { updateActiveTab } from './tabsStore';

export interface NodeVariable {
	value: string | { $env: string; default?: unknown };
	type?: string; // "str" (default), "int", "float", "bool", "json"
}

/**
 * Get the display string from a variable value.
 * Returns '' for EnvVar values (no string validation needed).
 */
export function getVarValueString(value: string | { $env: string; default?: unknown }): string {
	if (typeof value === 'object' && value !== null && '$env' in value) {
		return '';
	}
	return value;
}

/**
 * Validate a variable value against its declared type.
 * Returns null if valid, or an error message string if invalid.
 * EnvVar objects always pass validation (returns null).
 */
export function validateVarValue(value: string | { $env: string; default?: unknown }, type: string | undefined): string | null {
	if (typeof value === 'object' && value !== null && '$env' in value) return null;
	const t = type || 'str';
	if (value === '') return null; // empty is ok
	switch (t) {
		case 'str':
			return null;
		case 'int':
			if (!/^-?\d+$/.test(value.trim())) return 'Must be an integer';
			return null;
		case 'float':
			if (isNaN(Number(value.trim())) || value.trim() === '') return 'Must be a number';
			return null;
		case 'bool':
			if (!['true', 'false', '1', '0', 'yes', 'no'].includes(value.toLowerCase().trim()))
				return 'Must be true/false, 1/0, or yes/no';
			return null;
		case 'json':
			try { JSON.parse(value); return null; }
			catch { return 'Must be valid JSON'; }
		default:
			return null;
	}
}

// Derived: net-level node variables from extraData.node_vars
export const projectNodeVars = derived(
	extraData,
	($extraData): Record<string, NodeVariable> => {
		const extra = $extraData as Record<string, unknown> | null;
		return (extra?.node_vars as Record<string, NodeVariable>) || {};
	}
);

// Derived: node-level node variables for selected node
export const nodeNodeVars = derived(
	selectedNode,
	($selectedNode): Record<string, NodeVariable> => {
		if (!$selectedNode) return {};
		const config = $selectedNode.data._config as Record<string, unknown> | undefined;
		const executionConfig = config?.execution_config as Record<string, unknown> | undefined;
		return (executionConfig?.node_vars as Record<string, NodeVariable>) || {};
	}
);

// Derived: all nodes' variables overview (for net-level "All Node Variables" section)
export const allNodesVars = derived(
	nodes,
	($nodes): Array<{ nodeId: string; nodeName: string; vars: Record<string, NodeVariable> }> => {
		const result: Array<{ nodeId: string; nodeName: string; vars: Record<string, NodeVariable> }> = [];
		for (const node of $nodes) {
			const config = (node.data._config || {}) as Record<string, unknown>;
			const executionConfig = config.execution_config as Record<string, unknown> | undefined;
			const vars = (executionConfig?.node_vars as Record<string, NodeVariable>) || {};
			if (Object.keys(vars).length > 0) {
				result.push({
					nodeId: node.id,
					nodeName: node.data.label,
					vars,
				});
			}
		}
		return result;
	}
);

/**
 * Update net-level node variables
 */
export function updateProjectNodeVars(vars: Record<string, NodeVariable>): void {
	if (Object.keys(vars).length === 0) {
		// Remove node_vars key entirely when empty
		const extra = get(extraData) as Record<string, unknown> | null;
		if (extra) {
			const { node_vars: _, ...rest } = extra;
			// Can't use updateExtraDataLive for deletion, so set entire extraData
			updateExtraDataLive({ ...rest, node_vars: undefined });
		}
	} else {
		updateExtraDataLive({ node_vars: vars });
	}
}

/**
 * Update node-level node variables for a specific node
 */
export function updateNodeNodeVars(nodeId: string, vars: Record<string, NodeVariable>): void {
	const tab = get(activeTab);
	if (!tab) return;

	updateActiveTab({
		nodes: tab.nodes.map(node => {
			if (node.id !== nodeId) return node;

			const config = (node.data._config || {}) as Record<string, unknown>;
			const executionConfig = (config.execution_config || {}) as Record<string, unknown>;

			let newExecutionConfig: Record<string, unknown>;
			if (Object.keys(vars).length === 0) {
				const { node_vars: _, ...restExec } = executionConfig;
				newExecutionConfig = restExec;
			} else {
				newExecutionConfig = { ...executionConfig, node_vars: vars };
			}

			let newConfig: Record<string, unknown>;
			if (Object.keys(newExecutionConfig).length === 0) {
				const { execution_config: _, ...restConfig } = config;
				newConfig = restConfig;
			} else {
				newConfig = { ...config, execution_config: newExecutionConfig };
			}

			return {
				...node,
				data: {
					...node.data,
					_config: Object.keys(newConfig).length > 0 ? newConfig : undefined,
				},
			};
		}),
		isDirty: true,
	});
}
