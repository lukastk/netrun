/**
 * Signal type store - fetched once from backend (single source of truth).
 *
 * Provides helpers for signal port naming, detection, and computation.
 */
import { writable, get } from 'svelte/store';
import { api } from '$lib/api';
import type { PortConfig } from './flowStore';
import { isEnvVar } from '$lib/utils/envvar';

export interface SignalTypeInfo {
	validTypes: string[];
	portPrefix: string;
	portSuffix: string;
}

export const signalTypeInfo = writable<SignalTypeInfo | null>(null);

/** Fetch signal types from backend (call once on app init). */
export async function loadSignalTypes(): Promise<void> {
	try {
		const resp = await api.getSignalTypes();
		signalTypeInfo.set({
			validTypes: resp.valid_types,
			portPrefix: resp.port_prefix,
			portSuffix: resp.port_suffix,
		});
	} catch (e) {
		console.warn('Failed to load signal types:', e);
	}
}

/** Compute signal port name from a signal type, e.g. "epoch_finished" -> "__signal_epoch_finished__". */
export function signalPortName(signalType: string): string {
	const info = get(signalTypeInfo);
	if (!info) return `__signal_${signalType}__`;
	return `${info.portPrefix}${signalType}${info.portSuffix}`;
}

/** Check if a port name is a signal port. */
export function isSignalPort(portName: string): boolean {
	const info = get(signalTypeInfo);
	if (!info) return portName.startsWith('__signal_') && portName.endsWith('__');
	return portName.startsWith(info.portPrefix) && portName.endsWith(info.portSuffix);
}

/** Extract signal type from port name, e.g. "__signal_epoch_finished__" -> "epoch_finished". */
export function signalTypeFromPort(portName: string): string | null {
	const info = get(signalTypeInfo);
	const prefix = info?.portPrefix ?? '__signal_';
	const suffix = info?.portSuffix ?? '__';
	if (!portName.startsWith(prefix) || !portName.endsWith(suffix)) return null;
	return portName.slice(prefix.length, -suffix.length);
}

/**
 * Determine effective signals for a node.
 *
 * Resolution:
 * - nodeSignals is null/undefined -> inherit from defaultSignals
 * - nodeSignals is [] -> opt-out (no signals)
 * - nodeSignals is [...] -> use that list
 * - If either is a VarRef (env var), treat as empty for port computation
 */
export function getEffectiveSignals(
	nodeSignals: unknown,
	defaultSignals: unknown
): string[] {
	// Node-level explicit setting takes precedence
	if (nodeSignals !== null && nodeSignals !== undefined) {
		if (isEnvVar(nodeSignals)) return [];
		if (Array.isArray(nodeSignals)) return nodeSignals;
		return [];
	}
	// Inherit from net default
	if (defaultSignals !== null && defaultSignals !== undefined) {
		if (isEnvVar(defaultSignals)) return [];
		if (Array.isArray(defaultSignals)) return defaultSignals;
		return [];
	}
	return [];
}

/** Compute signal PortConfig[] from a list of signal types. */
export function computeSignalPorts(signals: string[]): PortConfig[] {
	return signals.map(sig => ({
		name: signalPortName(sig),
		type: 'signal',
		isSignal: true,
	}));
}
