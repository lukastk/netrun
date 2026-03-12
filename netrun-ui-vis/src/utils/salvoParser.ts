/**
 * Term Expression Parser for Salvo Conditions
 *
 * Parses a readable, Python-like syntax into SalvoConditionTermConfig.
 *
 * Grammar:
 *   expr      → or_expr
 *   or_expr   → and_expr ('or' and_expr)*
 *   and_expr  → not_expr ('and' not_expr)*
 *   not_expr  → 'not' not_expr | primary
 *   primary   → 'true' | 'false' | port_expr | '(' expr ')'
 *   port_expr → IDENT '.' STATE | IDENT CMP NUMBER
 *   STATE     → 'non_empty' | 'empty' | 'full' | 'non_full'
 *   CMP       → '==' | '<' | '>' | '<=' | '>='
 *
 * Examples:
 *   in1.non_empty
 *   in1.non_empty and in2.non_empty
 *   in1 >= 2 or in2.full
 *   not in1.empty
 *   (in1.non_empty or in2.non_empty) and in3.full
 */

import type {
  SalvoConditionTermConfig,
  PortStateConfig,
  TermTrue,
  TermFalse,
  TermPort,
  TermAnd,
  TermOr,
  TermNot,
} from '../types/salvoConditions.js';

// =============================================================================
// Token Types
// =============================================================================

type TokenType =
  | 'IDENT'
  | 'NUMBER'
  | 'DOT'
  | 'LPAREN'
  | 'RPAREN'
  | 'AND'
  | 'OR'
  | 'NOT'
  | 'TRUE'
  | 'FALSE'
  | 'EQ'
  | 'LT'
  | 'GT'
  | 'LE'
  | 'GE'
  | 'EOF';

interface Token {
  type: TokenType;
  value: string;
  position: number;
}

// =============================================================================
// Parse Result
// =============================================================================

export interface ParseSuccess {
  success: true;
  term: SalvoConditionTermConfig;
}

export interface ParseError {
  success: false;
  error: string;
  position?: number;
}

export type ParseResult = ParseSuccess | ParseError;

// =============================================================================
// Tokenizer
// =============================================================================

const KEYWORDS: Record<string, TokenType> = {
  and: 'AND',
  or: 'OR',
  not: 'NOT',
  true: 'TRUE',
  false: 'FALSE',
};

const PORT_STATES = ['non_empty', 'empty', 'full', 'non_full'];

function tokenize(input: string): Token[] | ParseError {
  const tokens: Token[] = [];
  let pos = 0;

  while (pos < input.length) {
    // Skip whitespace
    if (/\s/.test(input[pos])) {
      pos++;
      continue;
    }

    const startPos = pos;

    // Two-character operators
    if (pos < input.length - 1) {
      const twoChar = input.slice(pos, pos + 2);
      if (twoChar === '==') {
        tokens.push({ type: 'EQ', value: '==', position: pos });
        pos += 2;
        continue;
      }
      if (twoChar === '<=') {
        tokens.push({ type: 'LE', value: '<=', position: pos });
        pos += 2;
        continue;
      }
      if (twoChar === '>=') {
        tokens.push({ type: 'GE', value: '>=', position: pos });
        pos += 2;
        continue;
      }
    }

    // Single-character operators
    if (input[pos] === '<') {
      tokens.push({ type: 'LT', value: '<', position: pos });
      pos++;
      continue;
    }
    if (input[pos] === '>') {
      tokens.push({ type: 'GT', value: '>', position: pos });
      pos++;
      continue;
    }
    if (input[pos] === '.') {
      tokens.push({ type: 'DOT', value: '.', position: pos });
      pos++;
      continue;
    }
    if (input[pos] === '(') {
      tokens.push({ type: 'LPAREN', value: '(', position: pos });
      pos++;
      continue;
    }
    if (input[pos] === ')') {
      tokens.push({ type: 'RPAREN', value: ')', position: pos });
      pos++;
      continue;
    }

    // Numbers
    if (/\d/.test(input[pos])) {
      let numStr = '';
      while (pos < input.length && /\d/.test(input[pos])) {
        numStr += input[pos];
        pos++;
      }
      tokens.push({ type: 'NUMBER', value: numStr, position: startPos });
      continue;
    }

    // Identifiers and keywords (including port states with underscores)
    if (/[a-zA-Z_]/.test(input[pos])) {
      let ident = '';
      while (pos < input.length && /[a-zA-Z0-9_]/.test(input[pos])) {
        ident += input[pos];
        pos++;
      }

      const lowerIdent = ident.toLowerCase();
      if (KEYWORDS[lowerIdent]) {
        tokens.push({ type: KEYWORDS[lowerIdent], value: ident, position: startPos });
      } else {
        tokens.push({ type: 'IDENT', value: ident, position: startPos });
      }
      continue;
    }

    // Unknown character
    return {
      success: false,
      error: `Unexpected character '${input[pos]}'`,
      position: pos,
    };
  }

  tokens.push({ type: 'EOF', value: '', position: pos });
  return tokens;
}

// =============================================================================
// Parser
// =============================================================================

class Parser {
  private tokens: Token[];
  private pos: number = 0;
  private validPortNames: string[] | null;

  constructor(tokens: Token[], validPortNames: string[] | null = null) {
    this.tokens = tokens;
    this.validPortNames = validPortNames;
  }

  private current(): Token {
    return this.tokens[this.pos];
  }

  private peek(offset: number = 0): Token {
    const idx = this.pos + offset;
    return idx < this.tokens.length ? this.tokens[idx] : this.tokens[this.tokens.length - 1];
  }

  private advance(): Token {
    const token = this.current();
    if (this.pos < this.tokens.length - 1) {
      this.pos++;
    }
    return token;
  }

  private match(...types: TokenType[]): boolean {
    return types.includes(this.current().type);
  }

  private expect(type: TokenType, message: string): Token | ParseError {
    if (this.current().type === type) {
      return this.advance();
    }
    return {
      success: false,
      error: message,
      position: this.current().position,
    };
  }

  parse(): ParseResult {
    if (this.current().type === 'EOF') {
      return {
        success: false,
        error: 'Empty expression',
        position: 0,
      };
    }

    const result = this.parseOrExpr();
    if (!result.success) {
      return result;
    }

    if (this.current().type !== 'EOF') {
      return {
        success: false,
        error: `Unexpected token '${this.current().value}'`,
        position: this.current().position,
      };
    }

    return result;
  }

  private parseOrExpr(): ParseResult {
    const terms: SalvoConditionTermConfig[] = [];
    const first = this.parseAndExpr();
    if (!first.success) return first;
    terms.push(first.term);

    while (this.match('OR')) {
      this.advance(); // consume 'or'
      const next = this.parseAndExpr();
      if (!next.success) return next;
      terms.push(next.term);
    }

    if (terms.length === 1) {
      return { success: true, term: terms[0] };
    }

    return {
      success: true,
      term: { type: 'or', terms } as TermOr,
    };
  }

  private parseAndExpr(): ParseResult {
    const terms: SalvoConditionTermConfig[] = [];
    const first = this.parseNotExpr();
    if (!first.success) return first;
    terms.push(first.term);

    while (this.match('AND')) {
      this.advance(); // consume 'and'
      const next = this.parseNotExpr();
      if (!next.success) return next;
      terms.push(next.term);
    }

    if (terms.length === 1) {
      return { success: true, term: terms[0] };
    }

    return {
      success: true,
      term: { type: 'and', terms } as TermAnd,
    };
  }

  private parseNotExpr(): ParseResult {
    if (this.match('NOT')) {
      this.advance(); // consume 'not'
      const inner = this.parseNotExpr();
      if (!inner.success) return inner;
      return {
        success: true,
        term: { type: 'not', term: inner.term } as TermNot,
      };
    }

    return this.parsePrimary();
  }

  private parsePrimary(): ParseResult {
    // Boolean literals
    if (this.match('TRUE')) {
      this.advance();
      return { success: true, term: { type: 'true' } as TermTrue };
    }

    if (this.match('FALSE')) {
      this.advance();
      return { success: true, term: { type: 'false' } as TermFalse };
    }

    // Parenthesized expression
    if (this.match('LPAREN')) {
      this.advance(); // consume '('
      const inner = this.parseOrExpr();
      if (!inner.success) return inner;

      const closeParen = this.expect('RPAREN', "Expected ')' after expression");
      if ('error' in closeParen) return closeParen;

      return inner;
    }

    // Port expression
    if (this.match('IDENT')) {
      return this.parsePortExpr();
    }

    return {
      success: false,
      error: `Unexpected token '${this.current().value || 'end of input'}'`,
      position: this.current().position,
    };
  }

  private parsePortExpr(): ParseResult {
    const portToken = this.advance();
    const portName = portToken.value;

    // Validate port name if we have valid port names
    if (this.validPortNames && !this.validPortNames.includes(portName)) {
      return {
        success: false,
        error: `Unknown port '${portName}'. Valid ports: ${this.validPortNames.join(', ')}`,
        position: portToken.position,
      };
    }

    // Check for dot-notation state (port.state)
    if (this.match('DOT')) {
      this.advance(); // consume '.'

      if (!this.match('IDENT')) {
        return {
          success: false,
          error: 'Expected state name after dot',
          position: this.current().position,
        };
      }

      const stateToken = this.advance();
      const stateName = stateToken.value.toLowerCase();

      if (!PORT_STATES.includes(stateName)) {
        return {
          success: false,
          error: `Unknown port state '${stateName}'. Valid states: ${PORT_STATES.join(', ')}`,
          position: stateToken.position,
        };
      }

      const state: PortStateConfig = { type: stateName as 'empty' | 'full' | 'non_empty' | 'non_full' };
      return {
        success: true,
        term: { type: 'port', port_name: portName, state } as TermPort,
      };
    }

    // Check for comparison operators (port >= value)
    if (this.match('EQ', 'LT', 'GT', 'LE', 'GE')) {
      const opToken = this.advance();

      if (!this.match('NUMBER')) {
        return {
          success: false,
          error: 'Expected number after comparison operator',
          position: this.current().position,
        };
      }

      const numToken = this.advance();
      const value = parseInt(numToken.value, 10);

      let stateType: PortStateConfig['type'];
      switch (opToken.type) {
        case 'EQ':
          stateType = 'equals';
          break;
        case 'LT':
          stateType = 'less_than';
          break;
        case 'GT':
          stateType = 'greater_than';
          break;
        case 'LE':
          stateType = 'equals_or_less_than';
          break;
        case 'GE':
          stateType = 'equals_or_greater_than';
          break;
        default:
          return {
            success: false,
            error: `Unknown operator '${opToken.value}'`,
            position: opToken.position,
          };
      }

      const state: PortStateConfig = { type: stateType, value } as PortStateConfig;
      return {
        success: true,
        term: { type: 'port', port_name: portName, state } as TermPort,
      };
    }

    return {
      success: false,
      error: `Expected '.' or comparison operator after port name '${portName}'`,
      position: this.current().position,
    };
  }
}

// =============================================================================
// Public API
// =============================================================================

/**
 * Parse a term expression string into a SalvoConditionTermConfig
 *
 * @param input - The expression string to parse
 * @param validPortNames - Optional list of valid port names for validation
 * @returns ParseResult with either the parsed term or an error
 *
 * @example
 * parseTermExpression('in1.non_empty and in2.non_empty', ['in1', 'in2'])
 * parseTermExpression('in1 >= 2 or in2.full')
 * parseTermExpression('not in1.empty')
 */
export function parseTermExpression(
  input: string,
  validPortNames: string[] | null = null
): ParseResult {
  const trimmed = input.trim();
  if (!trimmed) {
    return {
      success: false,
      error: 'Empty expression',
      position: 0,
    };
  }

  const tokensOrError = tokenize(trimmed);
  if (!Array.isArray(tokensOrError)) {
    return tokensOrError;
  }

  const parser = new Parser(tokensOrError, validPortNames);
  return parser.parse();
}

/**
 * Convert a SalvoConditionTermConfig back to an expression string
 *
 * @param term - The term configuration to serialize
 * @returns A string representation of the term
 */
export function termToExpression(term: SalvoConditionTermConfig): string {
  switch (term.type) {
    case 'true':
      return 'true';
    case 'false':
      return 'false';
    case 'port':
      return portTermToExpression(term);
    case 'and':
      return term.terms.map((t) => wrapIfNeeded(t, 'and')).join(' and ');
    case 'or':
      return term.terms.map((t) => wrapIfNeeded(t, 'or')).join(' or ');
    case 'not':
      return `not ${wrapIfNeeded(term.term, 'not')}`;
  }
}

function portTermToExpression(term: TermPort): string {
  const { port_name, state } = term;

  switch (state.type) {
    case 'empty':
    case 'full':
    case 'non_empty':
    case 'non_full':
      return `${port_name}.${state.type}`;
    case 'equals':
      return `${port_name} == ${state.value}`;
    case 'less_than':
      return `${port_name} < ${state.value}`;
    case 'greater_than':
      return `${port_name} > ${state.value}`;
    case 'equals_or_less_than':
      return `${port_name} <= ${state.value}`;
    case 'equals_or_greater_than':
      return `${port_name} >= ${state.value}`;
  }
}

function wrapIfNeeded(term: SalvoConditionTermConfig, parentOp: 'and' | 'or' | 'not'): string {
  // Determine if we need parentheses based on precedence
  // Precedence (lowest to highest): or, and, not, primary
  const expr = termToExpression(term);

  if (parentOp === 'not') {
    // 'not' binds tightly, so we need parens for 'and' and 'or'
    if (term.type === 'and' || term.type === 'or') {
      return `(${expr})`;
    }
  } else if (parentOp === 'and') {
    // 'and' has higher precedence than 'or', so wrap 'or'
    if (term.type === 'or') {
      return `(${expr})`;
    }
  }
  // 'or' has lowest precedence, no wrapping needed

  return expr;
}

/**
 * Extract all port names referenced in a term
 *
 * @param term - The term configuration
 * @returns Array of unique port names
 */
export function getReferencedPorts(term: SalvoConditionTermConfig): string[] {
  const ports = new Set<string>();
  collectPorts(term, ports);
  return Array.from(ports);
}

function collectPorts(term: SalvoConditionTermConfig, ports: Set<string>): void {
  switch (term.type) {
    case 'true':
    case 'false':
      break;
    case 'port':
      ports.add(term.port_name);
      break;
    case 'and':
    case 'or':
      for (const t of term.terms) {
        collectPorts(t, ports);
      }
      break;
    case 'not':
      collectPorts(term.term, ports);
      break;
  }
}

/**
 * Validate a term against available port names
 *
 * @param term - The term configuration
 * @param validPortNames - List of valid port names
 * @returns Array of error messages (empty if valid)
 */
export function validateTermPorts(
  term: SalvoConditionTermConfig,
  validPortNames: string[]
): string[] {
  const referencedPorts = getReferencedPorts(term);
  const errors: string[] = [];

  for (const port of referencedPorts) {
    if (!validPortNames.includes(port)) {
      errors.push(`Unknown port '${port}'. Valid ports: ${validPortNames.join(', ')}`);
    }
  }

  return errors;
}
