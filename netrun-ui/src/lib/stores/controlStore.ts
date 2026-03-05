/**
 * Control type store - fetched once from backend (single source of truth).
 *
 * Provides helpers for control port naming, detection, and computation.
 */
import { writable, get } from 'svelte/store';
import { api } from '$lib/api';
import type { PortConfig } from './flowStore';
import { isEnvVar } from '$lib/utils/envvar';

export interface ControlTypeInfo {
	validTypes: string[];
	portPrefix: string;
	portSuffix: string;
}

export const controlTypeInfo = writable<ControlTypeInfo | null>(null);

/** Fetch control types from backend (call once on app init). */
export async function loadControlTypes(): Promise<void> {
	try {
		const resp = await api.getControlTypes();
		controlTypeInfo.set({
			validTypes: resp.valid_types,
			portPrefix: resp.port_prefix,
			portSuffix: resp.port_suffix,
		});
	} catch (e) {
		console.warn('Failed to load control types:', e);
	}
}

/** Compute control port name from a control type, e.g. "pause" -> "__control_pause__". */
export function controlPortName(controlType: string): string {
	const info = get(controlTypeInfo);
	if (!info) return `__control_${controlType}__`;
	return `${info.portPrefix}${controlType}${info.portSuffix}`;
}

/** Check if a port name is a control port. */
export function isControlPort(portName: string): boolean {
	const info = get(controlTypeInfo);
	if (!info) return portName.startsWith('__control_') && portName.endsWith('__');
	return portName.startsWith(info.portPrefix) && portName.endsWith(info.portSuffix);
}

/** Extract control type from port name, e.g. "__control_pause__" -> "pause". */
export function controlTypeFromPort(portName: string): string | null {
	const info = get(controlTypeInfo);
	const prefix = info?.portPrefix ?? '__control_';
	const suffix = info?.portSuffix ?? '__';
	if (!portName.startsWith(prefix) || !portName.endsWith(suffix)) return null;
	return portName.slice(prefix.length, -suffix.length);
}

/**
 * Determine effective controls for a node.
 *
 * Resolution:
 * - nodeControls is null/undefined -> inherit from defaultControls
 * - nodeControls is [] -> opt-out (no controls)
 * - nodeControls is [...] -> use that list
 * - If either is a VarRef (env var), treat as empty for port computation
 */
export function getEffectiveControls(
	nodeControls: unknown,
	defaultControls: unknown
): string[] {
	// Node-level explicit setting takes precedence
	if (nodeControls !== null && nodeControls !== undefined) {
		if (isEnvVar(nodeControls)) return [];
		if (Array.isArray(nodeControls)) return nodeControls;
		return [];
	}
	// Inherit from net default
	if (defaultControls !== null && defaultControls !== undefined) {
		if (isEnvVar(defaultControls)) return [];
		if (Array.isArray(defaultControls)) return defaultControls;
		return [];
	}
	return [];
}

/** Compute control PortConfig[] from a list of control types. */
export function computeControlPorts(controls: string[]): PortConfig[] {
	return controls.map(ctrl => ({
		name: controlPortName(ctrl),
		type: 'control',
		isControl: true,
	}));
}
