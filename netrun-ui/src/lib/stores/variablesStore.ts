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
	value?: string | number | boolean | { $env: string; default?: unknown };
	type?: string; // "str" (default), "int", "float", "bool", "json"
	options?: (string | number | boolean)[];
	inherit?: boolean;
	optional?: boolean;
}

/**
 * Get the display string from a variable value.
 * Returns '' for EnvVar values (no string validation needed).
 */
export function getVarValueString(value: string | number | boolean | { $env: string; default?: unknown } | undefined): string {
	if (value === undefined || value === null) return '';
	if (typeof value === 'object' && value !== null && '$env' in value) {
		return '';
	}
	return String(value);
}

/**
 * Validate a variable value against its declared type.
 * Returns null if valid, or an error message string if invalid.
 * EnvVar objects always pass validation (returns null).
 */
export function validateVarValue(
	value: string | number | boolean | { $env: string; default?: unknown } | undefined,
	type: string | undefined,
	options?: (string | number | boolean)[],
): string | null {
	if (value === undefined || value === null) return null; // unset placeholder — valid
	if (typeof value === 'object' && value !== null && '$env' in value) return null;
	const strValue = typeof value === 'string' ? value : String(value);
	const t = type || 'str';
	if (strValue === '') return null; // empty is ok
	switch (t) {
		case 'str':
			break;
		case 'int':
			if (!/^-?\d+$/.test(strValue.trim())) return 'Must be an integer';
			break;
		case 'float':
			if (isNaN(Number(strValue.trim())) || strValue.trim() === '') return 'Must be a number';
			break;
		case 'bool':
			if (!['true', 'false', '1', '0', 'yes', 'no'].includes(strValue.toLowerCase().trim()))
				return 'Must be true/false, 1/0, or yes/no';
			break;
		case 'json':
			try { JSON.parse(strValue); }
			catch { return 'Must be valid JSON'; }
			break;
		default:
			break;
	}

	if (options && options.length > 0) {
		let coerced: string | number | boolean = strValue;
		if (t === 'int') coerced = parseInt(strValue);
		else if (t === 'float') coerced = parseFloat(strValue);
		else if (t === 'bool') coerced = ['true', '1', 'yes'].includes(strValue.toLowerCase().trim());

		if (!options.includes(coerced)) {
			return `Must be one of: ${options.join(', ')}`;
		}
	}
	return null;
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

// Derived: available variable names for the selected node (net + node merged)
export const availableVarNames = derived(
	[projectNodeVars, nodeNodeVars],
	([$projectVars, $nodeVars]): string[] => {
		const names = new Set<string>();
		for (const name of Object.keys($projectVars)) names.add(name);
		for (const name of Object.keys($nodeVars)) names.add(name);
		return Array.from(names).sort();
	}
);

// Derived: net-level variable names only (for net-level config fields)
export const projectVarNames = derived(
	projectNodeVars,
	($projectVars): string[] => Object.keys($projectVars).sort()
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
