/**
 * TypeScript types for Salvo Conditions
 *
 * These types mirror the Python SalvoConditionConfig types from netrun-sim.
 * They are used for client-side parsing, validation, and serialization.
 */

// =============================================================================
// Max Salvos Configuration
// =============================================================================

export interface MaxSalvosInfinite {
  type: 'infinite';
}

export interface MaxSalvosFinite {
  type: 'finite';
  max: number;
}

export type MaxSalvosConfig = MaxSalvosInfinite | MaxSalvosFinite;

// =============================================================================
// Port State Configurations
// =============================================================================

export interface PortStateEmpty {
  type: 'empty';
}

export interface PortStateFull {
  type: 'full';
}

export interface PortStateNonEmpty {
  type: 'non_empty';
}

export interface PortStateNonFull {
  type: 'non_full';
}

export interface PortStateEquals {
  type: 'equals';
  value: number;
}

export interface PortStateLessThan {
  type: 'less_than';
  value: number;
}

export interface PortStateGreaterThan {
  type: 'greater_than';
  value: number;
}

export interface PortStateEqualsOrLessThan {
  type: 'equals_or_less_than';
  value: number;
}

export interface PortStateEqualsOrGreaterThan {
  type: 'equals_or_greater_than';
  value: number;
}

export type PortStateConfig =
  | PortStateEmpty
  | PortStateFull
  | PortStateNonEmpty
  | PortStateNonFull
  | PortStateEquals
  | PortStateLessThan
  | PortStateGreaterThan
  | PortStateEqualsOrLessThan
  | PortStateEqualsOrGreaterThan;

// =============================================================================
// Packet Count Configurations
// =============================================================================

export interface PacketCountAll {
  type: 'all';
}

export interface PacketCountN {
  type: 'count';
  count: number;
}

export type PacketCountConfig = PacketCountAll | PacketCountN;

// =============================================================================
// Salvo Condition Term (Recursive Boolean Expression)
// =============================================================================

export interface TermTrue {
  type: 'true';
}

export interface TermFalse {
  type: 'false';
}

export interface TermPort {
  type: 'port';
  port_name: string;
  state: PortStateConfig;
}

export interface TermAnd {
  type: 'and';
  terms: SalvoConditionTermConfig[];
}

export interface TermOr {
  type: 'or';
  terms: SalvoConditionTermConfig[];
}

export interface TermNot {
  type: 'not';
  term: SalvoConditionTermConfig;
}

export type SalvoConditionTermConfig =
  | TermTrue
  | TermFalse
  | TermPort
  | TermAnd
  | TermOr
  | TermNot;

// =============================================================================
// Complete Salvo Condition Configuration
// =============================================================================

export interface SalvoConditionConfig {
  max_salvos: MaxSalvosConfig;
  ports: Record<string, PacketCountConfig>;
  term: SalvoConditionTermConfig;
}

// =============================================================================
// UI State Types
// =============================================================================

export type SalvoConditionPreset = 'defaults' | 'all_ports_ready' | 'always' | 'custom';

export interface SalvoConditionUIState {
  name: string;
  preset: SalvoConditionPreset;
  customTermText: string;
  maxSalvosType: 'one' | 'infinite' | 'custom';
  maxSalvosCustomValue: number;
  ports: Record<string, { enabled: boolean; countType: 'all' | 'count'; count: number }>;
  parseError?: string;
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create a default MaxSalvosConfig (max_salvos = 1)
 */
export function createDefaultMaxSalvos(): MaxSalvosConfig {
  return { type: 'finite', max: 1 };
}

/**
 * Create a default PacketCountConfig (all packets)
 */
export function createDefaultPacketCount(): PacketCountConfig {
  return { type: 'all' };
}

/**
 * Create a default TermTrue
 */
export function createTermTrue(): TermTrue {
  return { type: 'true' };
}

/**
 * Create a default TermFalse
 */
export function createTermFalse(): TermFalse {
  return { type: 'false' };
}

/**
 * Create a TermPort with non_empty state
 */
export function createTermPortNonEmpty(portName: string): TermPort {
  return {
    type: 'port',
    port_name: portName,
    state: { type: 'non_empty' },
  };
}

/**
 * Create a TermAnd from multiple terms
 */
export function createTermAnd(terms: SalvoConditionTermConfig[]): TermAnd {
  return { type: 'and', terms };
}

/**
 * Create a TermOr from multiple terms
 */
export function createTermOr(terms: SalvoConditionTermConfig[]): TermOr {
  return { type: 'or', terms };
}

/**
 * Create a TermNot
 */
export function createTermNot(term: SalvoConditionTermConfig): TermNot {
  return { type: 'not', term };
}

/**
 * Create an "all ports ready" term (AND of all ports being non_empty)
 */
export function createAllPortsReadyTerm(portNames: string[]): SalvoConditionTermConfig {
  if (portNames.length === 0) {
    return createTermTrue();
  }
  if (portNames.length === 1) {
    return createTermPortNonEmpty(portNames[0]);
  }
  return createTermAnd(portNames.map(createTermPortNonEmpty));
}

/**
 * Create a default salvo condition for the given ports
 */
export function createDefaultSalvoCondition(
  name: string,
  portNames: string[],
  preset: SalvoConditionPreset = 'all_ports_ready'
): SalvoConditionConfig {
  const ports: Record<string, PacketCountConfig> = {};
  for (const portName of portNames) {
    ports[portName] = createDefaultPacketCount();
  }

  let term: SalvoConditionTermConfig;
  switch (preset) {
    case 'always':
      term = createTermTrue();
      break;
    case 'all_ports_ready':
    default:
      term = createAllPortsReadyTerm(portNames);
      break;
  }

  return {
    max_salvos: createDefaultMaxSalvos(),
    ports,
    term,
  };
}

/**
 * Create a default UI state for a new salvo condition
 */
export function createDefaultUIState(
  name: string,
  portNames: string[],
  isOutput: boolean = false
): SalvoConditionUIState {
  const ports: Record<string, { enabled: boolean; countType: 'all' | 'count'; count: number }> = {};
  for (const portName of portNames) {
    ports[portName] = { enabled: true, countType: 'all', count: 1 };
  }

  return {
    name,
    preset: isOutput ? 'always' : 'all_ports_ready',
    customTermText: '',
    maxSalvosType: 'one',
    maxSalvosCustomValue: 1,
    ports,
  };
}
