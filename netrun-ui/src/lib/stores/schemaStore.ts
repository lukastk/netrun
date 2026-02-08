/**
 * Schema store: fetches config schemas and tracks field registrations.
 *
 * The field registration system ensures every model field is explicitly
 * accounted for in the UI — either auto-rendered, custom-rendered, or ignored.
 */
import { writable, derived } from 'svelte/store';
import { api, type ConfigSchemaResponse, type ModelSchema } from '$lib/api';

// ---------------------------------------------------------------------------
// Schema data
// ---------------------------------------------------------------------------

export const configSchema = writable<ConfigSchemaResponse | null>(null);

export async function loadConfigSchema(): Promise<void> {
	try {
		const data = await api.getConfigSchema();
		configSchema.set(data);
	} catch (e) {
		console.warn('Failed to load config schema:', e);
	}
}

// ---------------------------------------------------------------------------
// Field registration
// ---------------------------------------------------------------------------

export type FieldRegistration = 'auto' | 'custom' | 'ignore';

interface Registration {
	registration: FieldRegistration;
	component?: string; // informational: which component handles it
}

/** Map of "ModelName.fieldName" → registration info */
const _registrations = new Map<string, Registration>();

/** Reactive trigger so derived stores update when registrations change */
const _registrationVersion = writable(0);

export function registerField(
	modelName: string,
	fieldName: string,
	registration: FieldRegistration,
	component?: string
): void {
	_registrations.set(`${modelName}.${fieldName}`, { registration, component });
	_registrationVersion.update(v => v + 1);
}

export function getFieldRegistration(
	modelName: string,
	fieldName: string
): FieldRegistration | undefined {
	return _registrations.get(`${modelName}.${fieldName}`)?.registration;
}

// ---------------------------------------------------------------------------
// Unregistered field detection
// ---------------------------------------------------------------------------

export const unregisteredFields = derived(
	[configSchema, _registrationVersion],
	([$schema]) => {
		if (!$schema) return [];
		const missing: string[] = [];
		for (const [modelName, modelSchema] of Object.entries($schema.models)) {
			for (const field of modelSchema.fields) {
				const key = `${modelName}.${field.name}`;
				if (!_registrations.has(key)) {
					missing.push(key);
				}
			}
		}
		return missing;
	}
);

// ---------------------------------------------------------------------------
// Helper: get auto fields for a model
// ---------------------------------------------------------------------------

export function getAutoFields(modelName: string, schema: ModelSchema) {
	return schema.fields.filter(f => {
		const reg = _registrations.get(`${modelName}.${f.name}`);
		return reg?.registration === 'auto';
	});
}

// ---------------------------------------------------------------------------
// Helper: get field description for custom components
// ---------------------------------------------------------------------------

/**
 * Look up a field's description from the loaded schema.
 * Returns undefined if schema isn't loaded or field has no description.
 * Designed for use with `title={desc}` on DOM elements.
 */
export function getFieldDescription(
	schema: ConfigSchemaResponse | null,
	modelName: string,
	fieldName: string
): string | undefined {
	if (!schema) return undefined;
	const model = schema.models[modelName];
	if (!model) return undefined;
	const field = model.fields.find(f => f.name === fieldName);
	return field?.description ?? undefined;
}
