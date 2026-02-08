/** Utilities for working with EnvVar references in config values. */

/** Check if a value is an EnvVar reference object */
export function isEnvVar(value: unknown): value is { $env: string; default?: unknown } {
	return typeof value === 'object' && value !== null && '$env' in value;
}

/** Create an EnvVar object */
export function makeEnvVar(envName: string, defaultValue?: unknown): { $env: string; default?: unknown } {
	if (defaultValue !== undefined && defaultValue !== null) {
		return { $env: envName, default: defaultValue };
	}
	return { $env: envName };
}

/** Extract env var name from an EnvVar object */
export function getEnvVarName(value: unknown): string {
	if (isEnvVar(value)) return value.$env;
	return '';
}

/** Extract default from an EnvVar object */
export function getEnvVarDefault(value: unknown): unknown {
	if (isEnvVar(value)) return value.default ?? null;
	return null;
}
