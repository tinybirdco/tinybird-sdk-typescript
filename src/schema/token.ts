/**
 * Token definition for Tinybird
 * Define reusable static tokens as TypeScript with full type safety
 */

// Symbol for brand typing - use Symbol.for() for global registry
// This ensures the same symbol is used across module instances
const TOKEN_BRAND = Symbol.for("tinybird.token");

/**
 * Token scope for datasources
 */
export type DatasourceTokenScope = "READ" | "APPEND";

/**
 * Token scope for pipes (READ only)
 */
export type PipeTokenScope = "READ";

/**
 * A token definition
 */
export interface TokenDefinition {
  readonly [TOKEN_BRAND]: true;
  /** Token name */
  readonly _name: string;
  /** Type marker for inference */
  readonly _type: "token";
}

/**
 * Define a static token
 *
 * @param name - The token name (must be valid identifier)
 * @returns A token definition that can be referenced in datasources and pipes
 *
 * @example
 * ```ts
 * import { defineToken } from '@tinybirdco/sdk';
 *
 * export const appToken = defineToken('app_read');
 *
 * // Use in datasource
 * const events = defineDatasource('events', {
 *   schema: { id: t.string() },
 *   tokens: [{ token: appToken, scope: 'READ' }],
 * });
 * ```
 */
export function defineToken(name: string): TokenDefinition {
  // Validate name is a valid identifier
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(
      `Invalid token name: "${name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.`
    );
  }

  return {
    [TOKEN_BRAND]: true,
    _name: name,
    _type: "token",
  };
}

/**
 * Check if a value is a token definition
 */
export function isTokenDefinition(value: unknown): value is TokenDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    TOKEN_BRAND in value &&
    (value as Record<symbol, unknown>)[TOKEN_BRAND] === true
  );
}
