/**
 * Serialization utilities for Salvo Conditions
 *
 * Converts between SalvoConditionConfig and JSON format used in .netrun.json files.
 */

import type {
  SalvoConditionConfig,
  SalvoConditionTermConfig,
  SalvoConditionUIState,
  MaxSalvosConfig,
  PacketCountConfig,
  PortStateConfig,
} from '../types/salvoConditions.js';
import {
  createDefaultSalvoCondition,
  createTermTrue,
  createAllPortsReadyTerm,
} from '../types/salvoConditions.js';
import { parseTermExpression, termToExpression } from './salvoParser.js';

// =============================================================================
// JSON Type Definitions (for .netrun.json format)
// =============================================================================

/**
 * JSON representation of MaxSalvosConfig
 * - { "type": "infinite" }
 * - { "type": "finite", "max": 1 }
 */
type MaxSalvosJSON = { type: 'infinite' } | { type: 'finite'; max: number };

/**
 * JSON representation of PacketCountConfig
 * - { "type": "all" }
 * - { "type": "count", "count": 5 }
 */
type PacketCountJSON = { type: 'all' } | { type: 'count'; count: number };

/**
 * JSON representation of PortStateConfig
 */
type PortStateJSON =
  | { type: 'empty' }
  | { type: 'full' }
  | { type: 'non_empty' }
  | { type: 'non_full' }
  | { type: 'equals'; value: number }
  | { type: 'less_than'; value: number }
  | { type: 'greater_than'; value: number }
  | { type: 'equals_or_less_than'; value: number }
  | { type: 'equals_or_greater_than'; value: number };

/**
 * JSON representation of SalvoConditionTermConfig
 */
type TermJSON =
  | { type: 'true' }
  | { type: 'false' }
  | { type: 'port'; port_name: string; state: PortStateJSON }
  | { type: 'and'; terms: TermJSON[] }
  | { type: 'or'; terms: TermJSON[] }
  | { type: 'not'; term: TermJSON };

/**
 * JSON representation of SalvoConditionConfig
 */
interface SalvoConditionJSON {
  max_salvos: MaxSalvosJSON;
  ports: Record<string, PacketCountJSON>;
  term: TermJSON;
}

// =============================================================================
// From JSON (Deserialization)
// =============================================================================

/**
 * Parse a salvo condition from JSON
 */
export function parseSalvoConditionFromJSON(json: unknown): SalvoConditionConfig | null {
  if (!json || typeof json !== 'object') return null;

  const obj = json as Record<string, unknown>;

  const maxSalvos = parseMaxSalvosFromJSON(obj.max_salvos);
  if (!maxSalvos) return null;

  const ports = parsePortsFromJSON(obj.ports);
  if (!ports) return null;

  const term = parseTermFromJSON(obj.term);
  if (!term) return null;

  return { max_salvos: maxSalvos, ports, term };
}

function parseMaxSalvosFromJSON(json: unknown): MaxSalvosConfig | null {
  if (!json || typeof json !== 'object') return null;
  const obj = json as Record<string, unknown>;

  if (obj.type === 'infinite') {
    return { type: 'infinite' };
  }
  if (obj.type === 'finite' && typeof obj.max === 'number') {
    return { type: 'finite', max: obj.max };
  }
  return null;
}

function parsePortsFromJSON(json: unknown): Record<string, PacketCountConfig> | null {
  if (!json || typeof json !== 'object') return null;

  const result: Record<string, PacketCountConfig> = {};
  for (const [portName, countJson] of Object.entries(json as Record<string, unknown>)) {
    const count = parsePacketCountFromJSON(countJson);
    if (!count) return null;
    result[portName] = count;
  }
  return result;
}

function parsePacketCountFromJSON(json: unknown): PacketCountConfig | null {
  if (!json || typeof json !== 'object') return null;
  const obj = json as Record<string, unknown>;

  if (obj.type === 'all') {
    return { type: 'all' };
  }
  if (obj.type === 'count' && typeof obj.count === 'number') {
    return { type: 'count', count: obj.count };
  }
  return null;
}

function parseTermFromJSON(json: unknown): SalvoConditionTermConfig | null {
  if (!json || typeof json !== 'object') return null;
  const obj = json as Record<string, unknown>;

  switch (obj.type) {
    case 'true':
      return { type: 'true' };
    case 'false':
      return { type: 'false' };
    case 'port': {
      if (typeof obj.port_name !== 'string') return null;
      const state = parsePortStateFromJSON(obj.state);
      if (!state) return null;
      return { type: 'port', port_name: obj.port_name, state };
    }
    case 'and': {
      if (!Array.isArray(obj.terms)) return null;
      const terms = obj.terms.map(parseTermFromJSON);
      if (terms.some((t) => t === null)) return null;
      return { type: 'and', terms: terms as SalvoConditionTermConfig[] };
    }
    case 'or': {
      if (!Array.isArray(obj.terms)) return null;
      const terms = obj.terms.map(parseTermFromJSON);
      if (terms.some((t) => t === null)) return null;
      return { type: 'or', terms: terms as SalvoConditionTermConfig[] };
    }
    case 'not': {
      const term = parseTermFromJSON(obj.term);
      if (!term) return null;
      return { type: 'not', term };
    }
    default:
      return null;
  }
}

function parsePortStateFromJSON(json: unknown): PortStateConfig | null {
  if (!json || typeof json !== 'object') return null;
  const obj = json as Record<string, unknown>;

  switch (obj.type) {
    case 'empty':
      return { type: 'empty' };
    case 'full':
      return { type: 'full' };
    case 'non_empty':
      return { type: 'non_empty' };
    case 'non_full':
      return { type: 'non_full' };
    case 'equals':
      if (typeof obj.value !== 'number') return null;
      return { type: 'equals', value: obj.value };
    case 'less_than':
      if (typeof obj.value !== 'number') return null;
      return { type: 'less_than', value: obj.value };
    case 'greater_than':
      if (typeof obj.value !== 'number') return null;
      return { type: 'greater_than', value: obj.value };
    case 'equals_or_less_than':
      if (typeof obj.value !== 'number') return null;
      return { type: 'equals_or_less_than', value: obj.value };
    case 'equals_or_greater_than':
      if (typeof obj.value !== 'number') return null;
      return { type: 'equals_or_greater_than', value: obj.value };
    default:
      return null;
  }
}

/**
 * Parse multiple salvo conditions from JSON (for in_salvo_conditions/out_salvo_conditions)
 */
export function parseSalvoConditionsFromJSON(
  json: unknown
): Record<string, SalvoConditionConfig> | null {
  if (json === null || json === undefined) {
    return null; // null means "use defaults"
  }
  if (typeof json !== 'object') {
    return null;
  }

  const result: Record<string, SalvoConditionConfig> = {};
  for (const [name, conditionJson] of Object.entries(json as Record<string, unknown>)) {
    const condition = parseSalvoConditionFromJSON(conditionJson);
    if (!condition) return null;
    result[name] = condition;
  }
  return result;
}

// =============================================================================
// To JSON (Serialization)
// =============================================================================

/**
 * Convert a SalvoConditionConfig to JSON format
 */
export function salvoConditionToJSON(config: SalvoConditionConfig): SalvoConditionJSON {
  return {
    max_salvos: maxSalvosToJSON(config.max_salvos),
    ports: portsToJSON(config.ports),
    term: termToJSON(config.term),
  };
}

function maxSalvosToJSON(config: MaxSalvosConfig): MaxSalvosJSON {
  if (config.type === 'infinite') {
    return { type: 'infinite' };
  }
  return { type: 'finite', max: config.max };
}

function portsToJSON(ports: Record<string, PacketCountConfig>): Record<string, PacketCountJSON> {
  const result: Record<string, PacketCountJSON> = {};
  for (const [portName, count] of Object.entries(ports)) {
    result[portName] = packetCountToJSON(count);
  }
  return result;
}

function packetCountToJSON(config: PacketCountConfig): PacketCountJSON {
  if (config.type === 'all') {
    return { type: 'all' };
  }
  return { type: 'count', count: config.count };
}

function termToJSON(term: SalvoConditionTermConfig): TermJSON {
  switch (term.type) {
    case 'true':
      return { type: 'true' };
    case 'false':
      return { type: 'false' };
    case 'port':
      return {
        type: 'port',
        port_name: term.port_name,
        state: portStateToJSON(term.state),
      };
    case 'and':
      return { type: 'and', terms: term.terms.map(termToJSON) };
    case 'or':
      return { type: 'or', terms: term.terms.map(termToJSON) };
    case 'not':
      return { type: 'not', term: termToJSON(term.term) };
  }
}

function portStateToJSON(state: PortStateConfig): PortStateJSON {
  // All states map directly
  return state as PortStateJSON;
}

/**
 * Convert multiple salvo conditions to JSON format
 */
export function salvoConditionsToJSON(
  conditions: Record<string, SalvoConditionConfig> | null
): Record<string, SalvoConditionJSON> | null {
  if (conditions === null) {
    return null; // null means "use defaults"
  }

  const result: Record<string, SalvoConditionJSON> = {};
  for (const [name, config] of Object.entries(conditions)) {
    result[name] = salvoConditionToJSON(config);
  }
  return result;
}

// =============================================================================
// UI State Conversion
// =============================================================================

/**
 * Convert a SalvoConditionConfig to UI state
 */
export function configToUIState(
  name: string,
  config: SalvoConditionConfig,
  portNames: string[]
): SalvoConditionUIState {
  // Determine preset based on term structure
  let preset: SalvoConditionUIState['preset'] = 'custom';
  let customTermText = termToExpression(config.term);

  // Check if it's "always" (term is true)
  if (config.term.type === 'true') {
    preset = 'always';
    customTermText = '';
  } else {
    // Check if it's "all_ports_ready" (AND of all ports being non_empty)
    const expectedTerm = createAllPortsReadyTerm(portNames);
    if (termsAreEquivalent(config.term, expectedTerm)) {
      preset = 'all_ports_ready';
      customTermText = '';
    }
  }

  // Determine max_salvos UI state
  let maxSalvosType: SalvoConditionUIState['maxSalvosType'] = 'one';
  let maxSalvosCustomValue = 1;
  if (config.max_salvos.type === 'infinite') {
    maxSalvosType = 'infinite';
  } else if (config.max_salvos.max === 1) {
    maxSalvosType = 'one';
  } else {
    maxSalvosType = 'custom';
    maxSalvosCustomValue = config.max_salvos.max;
  }

  // Convert ports
  const ports: SalvoConditionUIState['ports'] = {};
  for (const portName of portNames) {
    const portConfig = config.ports[portName];
    if (portConfig) {
      ports[portName] = {
        enabled: true,
        countType: portConfig.type === 'all' ? 'all' : 'count',
        count: portConfig.type === 'count' ? portConfig.count : 1,
      };
    } else {
      ports[portName] = {
        enabled: false,
        countType: 'all',
        count: 1,
      };
    }
  }

  return {
    name,
    preset,
    customTermText,
    maxSalvosType,
    maxSalvosCustomValue,
    ports,
  };
}

/**
 * Convert UI state to SalvoConditionConfig
 */
export function uiStateToConfig(
  state: SalvoConditionUIState,
  portNames: string[]
): SalvoConditionConfig | { error: string } {
  // Build max_salvos
  let maxSalvos: MaxSalvosConfig;
  switch (state.maxSalvosType) {
    case 'one':
      maxSalvos = { type: 'finite', max: 1 };
      break;
    case 'infinite':
      maxSalvos = { type: 'infinite' };
      break;
    case 'custom':
      maxSalvos = { type: 'finite', max: state.maxSalvosCustomValue };
      break;
  }

  // Build ports
  const ports: Record<string, PacketCountConfig> = {};
  for (const [portName, portState] of Object.entries(state.ports)) {
    if (portState.enabled) {
      if (portState.countType === 'all') {
        ports[portName] = { type: 'all' };
      } else {
        ports[portName] = { type: 'count', count: portState.count };
      }
    }
  }

  // Build term
  let term: SalvoConditionTermConfig;
  switch (state.preset) {
    case 'defaults':
      // This should return null to indicate "use defaults"
      // But since we're returning a config, we'll use the all_ports_ready term
      term = createAllPortsReadyTerm(portNames);
      break;
    case 'always':
      term = createTermTrue();
      break;
    case 'all_ports_ready':
      term = createAllPortsReadyTerm(portNames);
      break;
    case 'custom': {
      const parseResult = parseTermExpression(state.customTermText, portNames);
      if (!parseResult.success) {
        return { error: parseResult.error };
      }
      term = parseResult.term;
      break;
    }
  }

  return { max_salvos: maxSalvos, ports, term };
}

/**
 * Check if two terms are structurally equivalent
 */
function termsAreEquivalent(a: SalvoConditionTermConfig, b: SalvoConditionTermConfig): boolean {
  if (a.type !== b.type) return false;

  switch (a.type) {
    case 'true':
    case 'false':
      return true;
    case 'port': {
      const bPort = b as typeof a;
      if (a.port_name !== bPort.port_name) return false;
      return statesAreEquivalent(a.state, bPort.state);
    }
    case 'and':
    case 'or': {
      const bComposite = b as typeof a;
      if (a.terms.length !== bComposite.terms.length) return false;
      // Check if all terms match (order-independent for commutative ops)
      const bUsed = new Array(bComposite.terms.length).fill(false);
      for (const aTerm of a.terms) {
        let found = false;
        for (let i = 0; i < bComposite.terms.length; i++) {
          if (!bUsed[i] && termsAreEquivalent(aTerm, bComposite.terms[i])) {
            bUsed[i] = true;
            found = true;
            break;
          }
        }
        if (!found) return false;
      }
      return true;
    }
    case 'not': {
      const bNot = b as typeof a;
      return termsAreEquivalent(a.term, bNot.term);
    }
  }
}

function statesAreEquivalent(a: PortStateConfig, b: PortStateConfig): boolean {
  if (a.type !== b.type) return false;
  if ('value' in a && 'value' in b) {
    return a.value === b.value;
  }
  return true;
}
