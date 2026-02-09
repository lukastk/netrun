/** Utilities for working with VarRef references ($env and $var) in config values. */

/** A VarRef can be either an env var reference or a node var reference */
export type VarRefValue = { $env: string; default?: unknown } | { $var: string; default?: unknown };

/** Check if a value is any kind of VarRef (either $env or $var) */
export function isVarRef(value: unknown): value is VarRefValue {
	return typeof value === 'object' && value !== null && ('$env' in value || '$var' in value);
}

/** Check if a value is an EnvVar reference object ($env) */
export function isEnvVar(value: unknown): value is { $env: string; default?: unknown } {
	return typeof value === 'object' && value !== null && '$env' in value;
}

/** Check if a value is a node var reference object ($var) */
export function isNodeVarRef(value: unknown): value is { $var: string; default?: unknown } {
	return typeof value === 'object' && value !== null && '$var' in value;
}

/** Get the source type of a VarRef: 'env', 'var', or null */
export function getVarRefSource(value: unknown): 'env' | 'var' | null {
	if (isEnvVar(value)) return 'env';
	if (isNodeVarRef(value)) return 'var';
	return null;
}

/** Get the reference name from a VarRef (env var name or node var name) */
export function getVarRefName(value: unknown): string {
	if (isEnvVar(value)) return value.$env;
	if (isNodeVarRef(value)) return value.$var;
	return '';
}

/** Get the default from a VarRef */
export function getVarRefDefault(value: unknown): unknown {
	if (isVarRef(value)) return (value as Record<string, unknown>).default ?? null;
	return null;
}

/** Create an EnvVar object */
export function makeEnvVar(envName: string, defaultValue?: unknown): { $env: string; default?: unknown } {
	if (defaultValue !== undefined && defaultValue !== null) {
		return { $env: envName, default: defaultValue };
	}
	return { $env: envName };
}

/** Create a node var reference object */
export function makeNodeVarRef(varName: string, defaultValue?: unknown): { $var: string; default?: unknown } {
	if (defaultValue !== undefined && defaultValue !== null) {
		return { $var: varName, default: defaultValue };
	}
	return { $var: varName };
}

/** Extract env var name from an EnvVar object (backwards compat) */
export function getEnvVarName(value: unknown): string {
	if (isEnvVar(value)) return value.$env;
	return '';
}

/** Extract default from an EnvVar object (backwards compat) */
export function getEnvVarDefault(value: unknown): unknown {
	if (isEnvVar(value)) return value.default ?? null;
	return null;
}
