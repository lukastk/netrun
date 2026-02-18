/**
 * Recipe Store
 *
 * Manages recipe definitions and execution. Recipes are Python scripts that
 * can transform the NetConfig through the command palette.
 */
import { derived, writable, get } from 'svelte/store';
import { extraData, currentFilePath, updateExtraData, getCurrentConfig } from './flowStore';
import { api, type RecipePrompt } from '$lib/api';
import { toasts } from './toastStore';

export interface RecipeDefinition {
	path: string;
	description?: string;
}

export type { RecipePrompt };

/**
 * Derive recipes from extraData.
 * Recipes are stored in extraData.recipes in the netrun file.
 */
export const recipes = derived(extraData, ($extraData) => {
	const extra = $extraData as Record<string, unknown> | null;
	return (extra?.recipes as Record<string, RecipeDefinition>) ?? {};
});

/**
 * Resolve a recipe path relative to the project root.
 * Falls back to the current file's directory if project_root_path is not set.
 */
export function resolveRecipePath(recipePath: string): string {
	// If path is absolute, return as-is
	if (recipePath.startsWith('/')) {
		return recipePath;
	}

	// Resolve relative to project_root_path
	const config = getCurrentConfig();
	const projectRoot = config.project_root_path as string | undefined;
	if (projectRoot) {
		return `${projectRoot}/${recipePath}`;
	}

	// Fallback: resolve relative to current file's directory
	const filePath = get(currentFilePath);
	if (filePath) {
		const fileDir = filePath.substring(0, filePath.lastIndexOf('/'));
		return `${fileDir}/${recipePath}`;
	}

	return recipePath;
}

/**
 * Get prompts for a recipe.
 * Returns empty array if recipe has no get_prompts function.
 */
export async function getRecipePrompts(
	recipePath: string,
	config: Record<string, unknown>
): Promise<RecipePrompt[]> {
	const absolutePath = resolveRecipePath(recipePath);
	try {
		const response = await api.getRecipePrompts(absolutePath, config);
		return response.prompts;
	} catch (e) {
		toasts.error(`Failed to get recipe prompts: ${(e as Error).message}`);
		throw e;
	}
}

/**
 * Execute a recipe with the given inputs.
 * Validates the returned config before returning it.
 */
export async function executeRecipe(
	recipePath: string,
	config: Record<string, unknown>,
	inputs: Record<string, unknown>
): Promise<Record<string, unknown>> {
	const absolutePath = resolveRecipePath(recipePath);
	let result: Record<string, unknown>;
	let stdout = '';
	try {
		const response = await api.executeRecipe(absolutePath, config, inputs);
		result = response.config;
		stdout = response.stdout ?? '';
	} catch (e) {
		toasts.error(`Failed to execute recipe: ${(e as Error).message}`);
		throw e;
	}

	// Normalize nodes: in the UI format id = label = node name.
	// Recipes may omit id since it's redundant with data.label.
	const nodes = ((result.nodes ?? []) as Record<string, unknown>[]).map(n => {
		if (!n.id && (n.data as Record<string, unknown>)?.label) {
			return { ...n, id: (n.data as Record<string, unknown>).label };
		}
		return n;
	});
	result = { ...result, nodes };

	// Validate the returned config before accepting it
	try {
		const filePath = get(currentFilePath);
		const validation = await api.validateConfig(
			nodes as unknown as Parameters<typeof api.validateConfig>[0],
			(result.edges ?? []) as Parameters<typeof api.validateConfig>[1],
			result.extra as Record<string, unknown> | undefined,
			result.extraData as Record<string, unknown> | undefined,
			filePath ?? undefined,
		);
		if (!validation.valid) {
			const msgs = validation.errors.slice(0, 3).map(e => e.msg).join('; ');
			const suffix = validation.errors.length > 3 ? ` (+${validation.errors.length - 3} more)` : '';
			toasts.error(`Recipe returned invalid config: ${msgs}${suffix}`);
			throw new Error('Recipe returned invalid config');
		}
	} catch (e) {
		if ((e as Error).message === 'Recipe returned invalid config') throw e;
		toasts.error(`Failed to validate recipe output: ${(e as Error).message}`);
		throw e;
	}

	// Show captured stdout as info toast (like actions do)
	if (stdout.trim()) {
		toasts.info(stdout.trim());
	}

	return result;
}

// --- Recipe Modal State ---

interface RecipeModalState {
	show: boolean;
	recipeName: string;
	prompts: RecipePrompt[];
	onSubmit: (inputs: Record<string, unknown>) => void;
	onCancel: () => void;
}

export const recipeModalState = writable<RecipeModalState>({
	show: false,
	recipeName: '',
	prompts: [],
	onSubmit: () => {},
	onCancel: () => {}
});

/**
 * Show the recipe modal to collect user input.
 */
export function showRecipeModal(
	name: string,
	prompts: RecipePrompt[],
	onSubmit: (inputs: Record<string, unknown>) => void
): void {
	recipeModalState.set({
		show: true,
		recipeName: name,
		prompts,
		onSubmit: (inputs) => {
			onSubmit(inputs);
			recipeModalState.update(s => ({ ...s, show: false }));
		},
		onCancel: () => {
			recipeModalState.update(s => ({ ...s, show: false }));
		}
	});
}

/**
 * Close the recipe modal.
 */
export function closeRecipeModal(): void {
	recipeModalState.update(s => ({ ...s, show: false }));
}

// --- Recipe CRUD ---

/**
 * Add a new recipe to extraData.recipes.
 */
export function addRecipe(name: string, definition: RecipeDefinition): void {
	const current = get(recipes);
	updateExtraData({ recipes: { ...current, [name]: definition } });
}

/**
 * Update an existing recipe (supports renaming).
 */
export function updateRecipe(originalName: string, newName: string, definition: RecipeDefinition): void {
	const current = { ...get(recipes) };
	if (newName !== originalName) {
		delete current[originalName];
	}
	current[newName] = definition;
	updateExtraData({ recipes: current });
}

/**
 * Remove a recipe by name.
 */
export function removeRecipe(name: string): void {
	const current = { ...get(recipes) };
	delete current[name];
	updateExtraData({ recipes: current });
}
