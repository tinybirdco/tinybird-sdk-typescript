/**
 * Datasource definition for Tinybird
 * Define table schemas as TypeScript with full type safety
 */

import type { AnyTypeValidator } from "./types.js";
import type { EngineConfig } from "./engines.js";
import type { KafkaConnectionDefinition } from "./connection.js";

// Symbol for brand typing - use Symbol.for() for global registry
// This ensures the same symbol is used across module instances
const DATASOURCE_BRAND = Symbol.for("tinybird.datasource");

/**
 * A column can be defined as just a type validator,
 * or with additional options like JSON path or default value
 */
export interface ColumnDefinition<T extends AnyTypeValidator = AnyTypeValidator> {
  /** The column type */
  type: T;
  /** JSON path for extracting from nested JSON (e.g., '$.user.id') */
  jsonPath?: string;
}

/**
 * Schema definition is a record of column names to type validators or column definitions
 */
export type SchemaDefinition = Record<string, AnyTypeValidator | ColumnDefinition>;

/**
 * Token configuration for datasource access
 */
export interface TokenConfig {
  /** Token name */
  name: string;
  /** Permissions granted to this token */
  permissions: readonly ("READ" | "APPEND")[];
}

/**
 * Kafka ingestion configuration for a datasource
 */
export interface KafkaConfig {
  /** Kafka connection to use */
  connection: KafkaConnectionDefinition;
  /** Kafka topic to consume from */
  topic: string;
  /** Consumer group ID (optional) */
  groupId?: string;
  /** Where to start reading: 'earliest' or 'latest' (default: 'latest') */
  autoOffsetReset?: "earliest" | "latest";
}

/**
 * Options for defining a datasource
 */
export interface DatasourceOptions<TSchema extends SchemaDefinition> {
  /** Human-readable description of the datasource */
  description?: string;
  /** Column schema definition */
  schema: TSchema;
  /** Table engine configuration */
  engine?: EngineConfig;
  /** Access tokens for this datasource */
  tokens?: readonly TokenConfig[];
  /** Workspaces to share this datasource with */
  sharedWith?: readonly string[];
  /**
   * Whether to generate JSON path expressions for columns.
   * Set to false for datasources that are targets of materialized views.
   * Defaults to true.
   */
  jsonPaths?: boolean;
  /**
   * Forward query used to evolve a datasource with incompatible schema changes.
   * This should be the SELECT clause only (no FROM/WHERE).
   */
  forwardQuery?: string;
  /** Kafka ingestion configuration */
  kafka?: KafkaConfig;
}

/**
 * A datasource definition with full type information
 */
export interface DatasourceDefinition<TSchema extends SchemaDefinition = SchemaDefinition> {
  readonly [DATASOURCE_BRAND]: true;
  /** Datasource name */
  readonly _name: string;
  /** Type marker for inference */
  readonly _type: "datasource";
  /** Schema definition */
  readonly _schema: TSchema;
  /** Full options */
  readonly options: DatasourceOptions<TSchema>;
}

/**
 * Define a Tinybird datasource
 *
 * @param name - The datasource name (must be valid identifier)
 * @param options - Datasource configuration including schema and engine
 * @returns A datasource definition that can be used in a project
 *
 * @example
 * ```ts
 * import { defineDatasource, t, engine } from '@tinybirdco/sdk';
 *
 * export const events = defineDatasource('events', {
 *   description: 'User event tracking data',
 *   schema: {
 *     timestamp: t.dateTime(),
 *     event_id: t.uuid(),
 *     user_id: t.string(),
 *     event_type: t.string().lowCardinality(),
 *     properties: t.json(),
 *     session_id: t.string().nullable(),
 *   },
 *   engine: engine.mergeTree({
 *     sortingKey: ['user_id', 'timestamp'],
 *     partitionKey: 'toYYYYMM(timestamp)',
 *     ttl: 'timestamp + INTERVAL 90 DAY',
 *   }),
 * });
 * ```
 */
export function defineDatasource<TSchema extends SchemaDefinition>(
  name: string,
  options: DatasourceOptions<TSchema>
): DatasourceDefinition<TSchema> {
  // Validate name is a valid identifier
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(
      `Invalid datasource name: "${name}". Must start with a letter or underscore and contain only alphanumeric characters and underscores.`
    );
  }

  return {
    [DATASOURCE_BRAND]: true,
    _name: name,
    _type: "datasource",
    _schema: options.schema,
    options,
  };
}

/**
 * Check if a value is a datasource definition
 */
export function isDatasourceDefinition(value: unknown): value is DatasourceDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    DATASOURCE_BRAND in value &&
    (value as Record<symbol, unknown>)[DATASOURCE_BRAND] === true
  );
}

/**
 * Get the column type for a schema entry (handles both raw validators and column definitions)
 */
export function getColumnType(column: AnyTypeValidator | ColumnDefinition): AnyTypeValidator {
  if ("type" in column && typeof column.type === "object") {
    return column.type;
  }
  return column as AnyTypeValidator;
}

/**
 * Get the JSON path for a column if defined
 */
export function getColumnJsonPath(column: AnyTypeValidator | ColumnDefinition): string | undefined {
  if ("jsonPath" in column) {
    return column.jsonPath;
  }
  return undefined;
}

/**
 * Get all column names from a schema
 */
export function getColumnNames<TSchema extends SchemaDefinition>(
  schema: TSchema
): (keyof TSchema)[] {
  return Object.keys(schema) as (keyof TSchema)[];
}

/**
 * Helper type to extract the schema from a datasource definition
 */
export type ExtractSchema<T> = T extends DatasourceDefinition<infer S> ? S : never;

/**
 * Column definition helper for complex column configurations
 *
 * @example
 * ```ts
 * import { defineDatasource, t, column } from '@tinybirdco/sdk';
 *
 * export const events = defineDatasource('events', {
 *   schema: {
 *     // Simple column
 *     id: t.string(),
 *     // Column with JSON extraction
 *     user_id: column(t.string(), { jsonPath: '$.user.id' }),
 *   },
 * });
 * ```
 */
export function column<T extends AnyTypeValidator>(
  type: T,
  options?: Omit<ColumnDefinition<T>, "type">
): ColumnDefinition<T> {
  return {
    type,
    ...options,
  };
}
