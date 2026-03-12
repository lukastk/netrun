/**
 * Pure functions for detecting signal/control port types from port names.
 *
 * Replaces the store-backed signalTypeFromPort/controlTypeFromPort
 * with a stateless approach that takes configuration as a parameter.
 */
import type { PortTypeConfig } from '../types/events.js';

/**
 * Extract the type name from a port name given a prefix/suffix config.
 *
 * For example, with prefix="__signal_" and suffix="__":
 *   "__signal_epoch_finished__" → "epoch_finished"
 *   "regular_port" → null
 */
export function extractPortTypeName(portName: string, config: PortTypeConfig | undefined): string | null {
	if (!config) return null;
	if (!portName.startsWith(config.prefix) || !portName.endsWith(config.suffix)) return null;
	const end = config.suffix.length > 0 ? -config.suffix.length : undefined;
	return portName.slice(config.prefix.length, end);
}
