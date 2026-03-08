/**
 * Recent files store with localStorage persistence
 */
import { writable, get } from 'svelte/store';
const browser = typeof window !== 'undefined';

const STORAGE_KEY = 'netrun-ui-recent-files';
const MAX_RECENT_FILES = 10;

export interface RecentFile {
	path: string;
	name: string;
	lastOpened: number; // timestamp
}

// Initialize from localStorage if available
function loadFromStorage(): RecentFile[] {
	if (!browser) return [];

	try {
		const stored = localStorage.getItem(STORAGE_KEY);
		if (stored) {
			return JSON.parse(stored);
		}
	} catch (e) {
		console.warn('Failed to load recent files from localStorage:', e);
	}
	return [];
}

// Save to localStorage
function saveToStorage(files: RecentFile[]): void {
	if (!browser) return;

	try {
		localStorage.setItem(STORAGE_KEY, JSON.stringify(files));
	} catch (e) {
		console.warn('Failed to save recent files to localStorage:', e);
	}
}

// Create the store
export const recentFiles = writable<RecentFile[]>(loadFromStorage());

// Subscribe to changes and persist
recentFiles.subscribe(files => {
	saveToStorage(files);
});

/**
 * Add a file to recent files list
 */
export function addRecentFile(path: string): void {
	const name = path.split('/').pop() || path;

	recentFiles.update(files => {
		// Remove if already exists
		const filtered = files.filter(f => f.path !== path);

		// Add to front
		const updated = [
			{ path, name, lastOpened: Date.now() },
			...filtered,
		];

		// Limit to max
		return updated.slice(0, MAX_RECENT_FILES);
	});
}

/**
 * Remove a file from recent files list
 */
export function removeRecentFile(path: string): void {
	recentFiles.update(files => files.filter(f => f.path !== path));
}

/**
 * Clear all recent files
 */
export function clearRecentFiles(): void {
	recentFiles.set([]);
}

/**
 * Get recent files (for non-reactive access)
 */
export function getRecentFiles(): RecentFile[] {
	return get(recentFiles);
}
