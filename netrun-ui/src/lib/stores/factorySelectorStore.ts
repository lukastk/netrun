/**
 * Factory Selector Modal Store
 *
 * Provides a Promise-based API for showing the factory selector modal
 * from anywhere in the application.
 */
import { writable, get } from 'svelte/store';
import { extraData } from './flowStore';

export interface FactorySelectorState {
	isOpen: boolean;
	resolve?: (value: string | null) => void;
}

const defaultState: FactorySelectorState = {
	isOpen: false,
	resolve: undefined,
};

export const factorySelectorState = writable<FactorySelectorState>(defaultState);

/**
 * Show the factory selector modal and return the selected factory path (or null if cancelled)
 */
export function showFactorySelector(): Promise<string | null> {
	return new Promise((resolve) => {
		factorySelectorState.set({
			isOpen: true,
			resolve: (value) => resolve(value),
		});
	});
}

/**
 * Close the factory selector with a result
 */
export function closeFactorySelector(result: string | null): void {
	const state = get(factorySelectorState);
	if (state.resolve) {
		state.resolve(result);
	}
	factorySelectorState.set(defaultState);
}

/**
 * Get the default factories from extraData
 */
export function getDefaultFactories(): string[] {
	const data = get(extraData) as Record<string, unknown> | null;
	return (data?.factories as string[]) || [];
}
