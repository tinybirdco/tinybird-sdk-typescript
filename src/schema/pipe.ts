/**
 * Pipe definition for Tinybird
 * Define SQL transformations and endpoints as TypeScript with full type safety
 */

import type { AnyTypeValidator } from "./types.js";
import type { AnyParamValidator } from "./params.js";
import type { DatasourceDefinition } from "./datasource.js";

// Symbol for brand typing
const PIPE_BRAND = Symbol("tinybird.pipe");
const NODE_BRAND = Symbol("tinybird.node");

/**
 * Parameter definition for a pipe
 */
export type ParamsDefinition = Record<string, AnyParamValidator>;

/**
 * Output schema definition for a pipe
 */
export type OutputDefinition = Record<string, AnyTypeValidator>;

/**
 * Node configuration options
 */
export interface NodeOptions {
  /** Node name (must be valid identifier) */
  name: string;
  /** SQL query for this node */
  sql: string;
  /** Human-readable description */
  description?: string;
}

/**
 * A node definition within a pipe
 */
export interface NodeDefinition {
  readonly [NODE_BRAND]: true;
  /** Node name */
  readonly _name: string;
  /** Type marker for inference */
  readonly _type: "node";
  /** SQL query */
  readonly sql: string;
  /** Description */
  readonly description?: string;
}

/**
 * Create a node within a pipe
 *
 * @param options - Node configuration
 * @returns A node definition
 *
 * @example
 * ```ts
 * import { node } from '@tinybird/sdk';
 *
 * const filteredNode = node({
 *   name: 'filtered',
 *   sql: `
 *     SELECT *
 *     FROM events
 *     WHERE timestamp >= {{DateTime(start_date)}}
 *   `,
 * });
 * ```
 */
export function node(options: NodeOptions): NodeDefinition {
  // Validate name is a valid identifier
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(options.name)) {
    throw new Error(
      `Invalid node name: "${options.name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.`
    );
  }

  return {
    [NODE_BRAND]: true,
    _name: options.name,
    _type: "node",
    sql: options.sql,
    description: options.description,
  };
}

/**
 * Check if a value is a node definition
 */
export function isNodeDefinition(value: unknown): value is NodeDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    NODE_BRAND in value &&
    (value as Record<symbol, unknown>)[NODE_BRAND] === true
  );
}

/**
 * Endpoint configuration for a pipe
 */
export interface EndpointConfig {
  /** Whether this pipe is exposed as an API endpoint */
  enabled: boolean;
  /** Cache configuration */
  cache?: {
    /** Whether caching is enabled */
    enabled: boolean;
    /** Cache TTL in seconds */
    ttl?: number;
  };
}

/**
 * Token configuration for pipe access
 */
export interface PipeTokenConfig {
  /** Token name */
  name: string;
}

/**
 * Options for defining a pipe
 */
export interface PipeOptions<
  TParams extends ParamsDefinition,
  TOutput extends OutputDefinition
> {
  /** Human-readable description of the pipe */
  description?: string;
  /** Parameter definitions for query inputs */
  params?: TParams;
  /** Nodes in the transformation pipeline */
  nodes: readonly NodeDefinition[];
  /** Output schema (required for type safety) */
  output: TOutput;
  /** Whether this pipe is an API endpoint (shorthand for { enabled: true }) */
  endpoint?: boolean | EndpointConfig;
  /** Access tokens for this pipe */
  tokens?: readonly PipeTokenConfig[];
}

/**
 * A pipe definition with full type information
 */
export interface PipeDefinition<
  TParams extends ParamsDefinition = ParamsDefinition,
  TOutput extends OutputDefinition = OutputDefinition
> {
  readonly [PIPE_BRAND]: true;
  /** Pipe name */
  readonly _name: string;
  /** Type marker for inference */
  readonly _type: "pipe";
  /** Parameter definitions */
  readonly _params: TParams;
  /** Output schema */
  readonly _output: TOutput;
  /** Full options */
  readonly options: PipeOptions<TParams, TOutput>;
}

/**
 * Define a Tinybird pipe
 *
 * @param name - The pipe name (must be valid identifier)
 * @param options - Pipe configuration including params, nodes, and output schema
 * @returns A pipe definition that can be used in a project
 *
 * @example
 * ```ts
 * import { definePipe, node, p, t } from '@tinybird/sdk';
 *
 * export const topEvents = definePipe('top_events', {
 *   description: 'Get top events by count',
 *   params: {
 *     start_date: p.dateTime(),
 *     end_date: p.dateTime(),
 *     limit: p.int32().optional(10),
 *   },
 *   nodes: [
 *     node({
 *       name: 'filtered',
 *       sql: `
 *         SELECT *
 *         FROM events
 *         WHERE timestamp BETWEEN {{DateTime(start_date)}} AND {{DateTime(end_date)}}
 *       `,
 *     }),
 *     node({
 *       name: 'aggregated',
 *       sql: `
 *         SELECT
 *           event_type,
 *           count() as event_count,
 *           uniqExact(user_id) as unique_users
 *         FROM filtered
 *         GROUP BY event_type
 *         ORDER BY event_count DESC
 *         LIMIT {{Int32(limit, 10)}}
 *       `,
 *     }),
 *   ],
 *   output: {
 *     event_type: t.string(),
 *     event_count: t.uint64(),
 *     unique_users: t.uint64(),
 *   },
 *   endpoint: true,
 * });
 * ```
 */
export function definePipe<
  TParams extends ParamsDefinition,
  TOutput extends OutputDefinition
>(
  name: string,
  options: PipeOptions<TParams, TOutput>
): PipeDefinition<TParams, TOutput> {
  // Validate name is a valid identifier
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(
      `Invalid pipe name: "${name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.`
    );
  }

  // Validate at least one node
  if (!options.nodes || options.nodes.length === 0) {
    throw new Error(`Pipe "${name}" must have at least one node.`);
  }

  // Validate output is provided (required for type safety)
  if (!options.output || Object.keys(options.output).length === 0) {
    throw new Error(
      `Pipe "${name}" must have an output schema defined for type safety.`
    );
  }

  const params = (options.params ?? {}) as TParams;

  return {
    [PIPE_BRAND]: true,
    _name: name,
    _type: "pipe",
    _params: params,
    _output: options.output,
    options: {
      ...options,
      params,
    },
  };
}

/**
 * Check if a value is a pipe definition
 */
export function isPipeDefinition(value: unknown): value is PipeDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    PIPE_BRAND in value &&
    (value as Record<symbol, unknown>)[PIPE_BRAND] === true
  );
}

/**
 * Get the endpoint configuration from a pipe
 */
export function getEndpointConfig(pipe: PipeDefinition): EndpointConfig | null {
  const { endpoint } = pipe.options;

  if (!endpoint) {
    return null;
  }

  if (typeof endpoint === "boolean") {
    return endpoint ? { enabled: true } : null;
  }

  return endpoint.enabled ? endpoint : null;
}

/**
 * Get all node names from a pipe
 */
export function getNodeNames(pipe: PipeDefinition): string[] {
  return pipe.options.nodes.map((n) => n._name);
}

/**
 * Get a specific node by name
 */
export function getNode(pipe: PipeDefinition, name: string): NodeDefinition | undefined {
  return pipe.options.nodes.find((n) => n._name === name);
}

/**
 * Helper type to extract params from a pipe definition
 */
export type ExtractParams<T> = T extends PipeDefinition<infer P, OutputDefinition> ? P : never;

/**
 * Helper type to extract output from a pipe definition
 */
export type ExtractOutput<T> = T extends PipeDefinition<ParamsDefinition, infer O> ? O : never;

/**
 * SQL template helper for referencing datasources and other nodes
 * This is a simple helper - for complex templating, use raw strings
 *
 * @example
 * ```ts
 * import { sql, events } from './datasources/events';
 *
 * const query = sql`SELECT * FROM ${events} WHERE id = 1`;
 * // Results in: "SELECT * FROM events WHERE id = 1"
 * ```
 */
export function sql(
  strings: TemplateStringsArray,
  ...values: (DatasourceDefinition | NodeDefinition | string | number)[]
): string {
  return strings.reduce((result, str, i) => {
    const value = values[i];
    if (value === undefined) {
      return result + str;
    }

    if (typeof value === "string" || typeof value === "number") {
      return result + str + String(value);
    }

    if ("_name" in value) {
      return result + str + value._name;
    }

    return result + str;
  }, "");
}
