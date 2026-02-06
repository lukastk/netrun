/**
 * Recipe Store
 *
 * Manages recipe definitions and execution. Recipes are Python scripts that
 * can transform the NetConfig through the command palette.
 */
import { derived, writable, get } from 'svelte/store';
import { extraData, currentFilePath } from './flowStore';
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
 * Resolve a recipe path relative to the current file's directory.
 */
export function resolveRecipePath(recipePath: string): string {
	const filePath = get(currentFilePath);
	if (!filePath) {
		// No file open, return as-is
		return recipePath;
	}

	// If path is absolute, return as-is
	if (recipePath.startsWith('/')) {
		return recipePath;
	}

	// Resolve relative to current file's directory
	const fileDir = filePath.substring(0, filePath.lastIndexOf('/'));
	return `${fileDir}/${recipePath}`;
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
 */
export async function executeRecipe(
	recipePath: string,
	config: Record<string, unknown>,
	inputs: Record<string, unknown>
): Promise<Record<string, unknown>> {
	const absolutePath = resolveRecipePath(recipePath);
	try {
		const response = await api.executeRecipe(absolutePath, config, inputs);
		return response.config;
	} catch (e) {
		toasts.error(`Failed to execute recipe: ${(e as Error).message}`);
		throw e;
	}
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
