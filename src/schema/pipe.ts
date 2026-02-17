/**
 * Pipe definition for Tinybird
 * Define SQL transformations and endpoints as TypeScript with full type safety
 */

import type { AnyTypeValidator } from "./types.js";
import type { AnyParamValidator } from "./params.js";
import type { DatasourceDefinition, SchemaDefinition, ColumnDefinition } from "./datasource.js";
import { getColumnType } from "./datasource.js";
import { getTinybirdType } from "./types.js";
import type { TokenDefinition, PipeTokenScope } from "./token.js";
import type { KafkaConnectionDefinition, S3ConnectionDefinition } from "./connection.js";
import { isKafkaConnectionDefinition, isS3ConnectionDefinition } from "./connection.js";

/** Symbol for brand typing pipes - use Symbol.for() for global registry */
export const PIPE_BRAND = Symbol.for("tinybird.pipe");
/** Symbol for brand typing nodes - use Symbol.for() for global registry */
export const NODE_BRAND = Symbol.for("tinybird.node");

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
 * import { node } from '@tinybirdco/sdk';
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
 * Materialized view configuration for a pipe
 */
export interface MaterializedConfig<
  TDatasource extends DatasourceDefinition<SchemaDefinition> = DatasourceDefinition<SchemaDefinition>
> {
  /** Target datasource where materialized data is written */
  datasource: TDatasource;
  /**
   * Deployment method for materialized views.
   * Use 'alter' to update existing materialized views using ALTER TABLE ... MODIFY QUERY
   * instead of recreating the table. This preserves existing data and reduces deployment time.
   */
  deploymentMethod?: "alter";
}

/**
 * Copy pipe configuration
 */
export interface CopyConfig<
  TDatasource extends DatasourceDefinition<SchemaDefinition> = DatasourceDefinition<SchemaDefinition>
> {
  /** Target datasource where copied data is written */
  datasource: TDatasource;
  /**
   * Copy mode: how data is ingested
   * - 'append': Appends the result to the target data source (default)
   * - 'replace': Every run completely replaces the destination Data Source content
   */
  copy_mode?: "append" | "replace";
  /**
   * Copy schedule: when the copy job runs
   * - A cron expression (e.g., "0 * * * *" for hourly)
   * - "@on-demand" for manual execution only
   * Defaults to "@on-demand" if not specified
   */
  copy_schedule?: string;
}

/**
 * Sink export strategy.
 * - 'append': append exported rows/files
 * - 'replace': replace destination data on each run
 */
export type SinkStrategy = "append" | "replace";

/**
 * Kafka sink configuration
 */
export interface KafkaSinkConfig {
  /** Kafka connection used to publish records */
  connection: KafkaConnectionDefinition;
  /** Destination Kafka topic */
  topic: string;
  /** Sink schedule (for example: @on-demand, @once, cron expression) */
  schedule?: string;
  /** Export strategy */
  strategy?: SinkStrategy;
}

/**
 * S3 sink configuration
 */
export interface S3SinkConfig {
  /** S3 connection used to write exported files */
  connection: S3ConnectionDefinition;
  /** Destination bucket URI (for example: s3://bucket/prefix/) */
  bucketUri: string;
  /** Output filename template (supports Tinybird placeholders) */
  fileTemplate: string;
  /** Output format (for example: csv, ndjson) */
  format?: string;
  /** Sink schedule (for example: @on-demand, @once, cron expression) */
  schedule?: string;
  /** Export strategy */
  strategy?: SinkStrategy;
}

/**
 * Sink pipe configuration (Kafka or S3 only)
 */
export type SinkConfig = KafkaSinkConfig | S3SinkConfig;

/**
 * Inline token configuration for pipe access
 */
export interface InlinePipeTokenConfig {
  /** Token name */
  name: string;
}

/**
 * Token reference with pipe-specific scope
 */
export interface PipeTokenReference {
  /** The token definition */
  token: TokenDefinition;
  /** Scope for this pipe (READ only) */
  scope: PipeTokenScope;
}

/**
 * Token configuration for pipe access.
 * Can be either an inline definition or a reference to a defined token.
 */
export type PipeTokenConfig = InlinePipeTokenConfig | PipeTokenReference;

/**
 * Options for defining a pipe (reusable SQL logic, no endpoint)
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
  /** Output schema (optional for reusable pipes, required for endpoints) */
  output?: TOutput;
  /** Whether this pipe is an API endpoint (shorthand for { enabled: true }). Mutually exclusive with materialized, copy, and sink. */
  endpoint?: boolean | EndpointConfig;
  /** Materialized view configuration. Mutually exclusive with endpoint, copy, and sink. */
  materialized?: MaterializedConfig;
  /** Copy pipe configuration. Mutually exclusive with endpoint, materialized, and sink. */
  copy?: CopyConfig;
  /** Sink configuration (Kafka/S3 export). Mutually exclusive with endpoint, materialized, and copy. */
  sink?: SinkConfig;
  /** Access tokens for this pipe */
  tokens?: readonly PipeTokenConfig[];
}

/**
 * Options for defining an endpoint (API-exposed pipe)
 */
export interface EndpointOptions<
  TParams extends ParamsDefinition,
  TOutput extends OutputDefinition
> {
  /** Human-readable description of the endpoint */
  description?: string;
  /** Parameter definitions for query inputs */
  params?: TParams;
  /** Nodes in the transformation pipeline */
  nodes: readonly NodeDefinition[];
  /** Output schema (required for type safety) */
  output: TOutput;
  /** Cache configuration */
  cache?: {
    /** Whether caching is enabled */
    enabled: boolean;
    /** Cache TTL in seconds */
    ttl?: number;
  };
  /** Access tokens for this endpoint */
  tokens?: readonly PipeTokenConfig[];
}

/**
 * Options for defining a copy pipe
 */
export interface CopyPipeOptions<
  TSchema extends SchemaDefinition,
  TDatasource extends DatasourceDefinition<TSchema>
> {
  /** Human-readable description of the copy pipe */
  description?: string;
  /** Nodes in the transformation pipeline */
  nodes: readonly NodeDefinition[];
  /** Target datasource where copied data is written */
  datasource: TDatasource;
  /**
   * Copy mode: how data is ingested
   * - 'append': Appends the result to the target data source (default)
   * - 'replace': Every run completely replaces the destination Data Source content
   */
  copy_mode?: "append" | "replace";
  /**
   * Copy schedule: when the copy job runs
   * - A cron expression (e.g., "0 * * * *" for hourly)
   * - "@on-demand" for manual execution only
   * Defaults to "@on-demand" if not specified
   */
  copy_schedule?: string;
  /** Access tokens for this copy pipe */
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
  /** Output schema (optional for reusable pipes) */
  readonly _output?: TOutput;
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
 * import { definePipe, node, p, t } from '@tinybirdco/sdk';
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
/**
 * Normalize a Tinybird type for comparison by removing wrappers that don't affect compatibility
 */
function normalizeTypeForComparison(type: string): string {
  // Remove Nullable wrapper for comparison
  let normalized = type.replace(/^Nullable\((.+)\)$/, "$1");
  // Remove LowCardinality wrapper
  normalized = normalized.replace(/^LowCardinality\((.+)\)$/, "$1");
  // Handle LowCardinality(Nullable(...))
  normalized = normalized.replace(/^LowCardinality\(Nullable\((.+)\)\)$/, "$1");
  // Remove timezone from DateTime types
  normalized = normalized.replace(/^DateTime\('[^']+'\)$/, "DateTime");
  normalized = normalized.replace(/^DateTime64\((\d+),\s*'[^']+'\)$/, "DateTime64($1)");
  return normalized;
}

/**
 * Check if two Tinybird types are compatible
 */
function typesAreCompatible(outputType: string, datasourceType: string): boolean {
  const normalizedOutput = normalizeTypeForComparison(outputType);
  const normalizedDatasource = normalizeTypeForComparison(datasourceType);

  // Direct match
  if (normalizedOutput === normalizedDatasource) {
    return true;
  }

  // SimpleAggregateFunction compatibility: output can be the base type
  // e.g., output UInt64 -> datasource SimpleAggregateFunction(sum, UInt64)
  const simpleAggMatch = normalizedDatasource.match(
    /^SimpleAggregateFunction\([^,]+,\s*(.+)\)$/
  );
  if (simpleAggMatch && normalizedOutput === simpleAggMatch[1]) {
    return true;
  }

  // AggregateFunction compatibility
  const aggMatch = normalizedDatasource.match(
    /^AggregateFunction\([^,]+,\s*(.+)\)$/
  );
  if (aggMatch && normalizedOutput === aggMatch[1]) {
    return true;
  }

  return false;
}

/**
 * Validate that the pipe output schema matches the target datasource schema
 */
function validateMaterializedSchema(
  pipeName: string,
  output: OutputDefinition,
  datasource: DatasourceDefinition
): void {
  const outputColumns = Object.keys(output);
  const datasourceSchema = datasource._schema;
  const datasourceColumns = Object.keys(datasourceSchema);

  // Check for missing columns in output
  const missingInOutput = datasourceColumns.filter(
    (col) => !outputColumns.includes(col)
  );
  if (missingInOutput.length > 0) {
    throw new Error(
      `Materialized view "${pipeName}" output schema is missing columns from target datasource "${datasource._name}": ${missingInOutput.join(", ")}`
    );
  }

  // Check for extra columns in output
  const extraInOutput = outputColumns.filter(
    (col) => !datasourceColumns.includes(col)
  );
  if (extraInOutput.length > 0) {
    throw new Error(
      `Materialized view "${pipeName}" output schema has columns not in target datasource "${datasource._name}": ${extraInOutput.join(", ")}`
    );
  }

  // Check type compatibility for each column
  for (const columnName of outputColumns) {
    const outputValidator = output[columnName];
    const datasourceColumn = datasourceSchema[columnName];

    const outputType = getTinybirdType(outputValidator);
    const datasourceValidator = getColumnType(datasourceColumn);
    const datasourceType = getTinybirdType(datasourceValidator);

    if (!typesAreCompatible(outputType, datasourceType)) {
      throw new Error(
        `Materialized view "${pipeName}" column "${columnName}" type mismatch: ` +
          `output has "${outputType}" but target datasource "${datasource._name}" expects "${datasourceType}"`
      );
    }
  }
}

function validateSinkConfig(pipeName: string, sink: SinkConfig): void {
  if ("topic" in sink) {
    if (!isKafkaConnectionDefinition(sink.connection)) {
      throw new Error(
        `Pipe "${pipeName}" sink with topic requires a Kafka connection.`
      );
    }
    if (!sink.topic.trim()) {
      throw new Error(`Pipe "${pipeName}" sink topic cannot be empty.`);
    }
    return;
  }

  if (!isS3ConnectionDefinition(sink.connection)) {
    throw new Error(
      `Pipe "${pipeName}" S3 sink requires an S3 connection.`
    );
  }
  if (!sink.bucketUri.trim()) {
    throw new Error(`Pipe "${pipeName}" sink bucketUri cannot be empty.`);
  }
  if (!sink.fileTemplate.trim()) {
    throw new Error(`Pipe "${pipeName}" sink fileTemplate cannot be empty.`);
  }
}

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

  // Validate output is provided for endpoints and materialized views
  if ((options.endpoint || options.materialized) && (!options.output || Object.keys(options.output).length === 0)) {
    throw new Error(
      `Pipe "${name}" must have an output schema defined when used as an endpoint or materialized view.`
    );
  }

  // Count how many types are configured
  const typeCount = [options.endpoint, options.materialized, options.copy, options.sink]
    .filter(Boolean).length;
  if (typeCount > 1) {
    throw new Error(
      `Pipe "${name}" can only have one of: endpoint, materialized, copy, or sink configuration. ` +
        `A pipe must be at most one type.`
    );
  }

  // Validate materialized view schema compatibility
  if (options.materialized) {
    // output is guaranteed to be defined here because of the earlier validation
    validateMaterializedSchema(name, options.output!, options.materialized.datasource);
  }

  if (options.sink) {
    validateSinkConfig(name, options.sink);
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
 * Options for defining a materialized view
 */
export interface MaterializedViewOptions<
  TDatasource extends DatasourceDefinition<SchemaDefinition>
> {
  /** Human-readable description of the materialized view */
  description?: string;
  /** Nodes in the transformation pipeline */
  nodes: readonly NodeDefinition[];
  /** Target datasource where materialized data is written */
  datasource: TDatasource;
  /**
   * Deployment method for materialized views.
   * Use 'alter' to update existing materialized views using ALTER TABLE ... MODIFY QUERY
   * instead of recreating the table. This preserves existing data and reduces deployment time.
   */
  deploymentMethod?: "alter";
  /** Access tokens for this pipe */
  tokens?: readonly PipeTokenConfig[];
}

/**
 * Helper type to extract the output definition from a datasource schema
 */
type DatasourceSchemaToOutput<TSchema extends SchemaDefinition> = {
  [K in keyof TSchema]: TSchema[K] extends ColumnDefinition<infer V>
    ? V
    : TSchema[K] extends AnyTypeValidator
      ? TSchema[K]
      : never;
};

/**
 * Define a Tinybird materialized view
 *
 * This is a convenience function that simplifies creating materialized views.
 * The output schema is automatically derived from the target datasource, ensuring
 * type safety between the pipe output and the target.
 *
 * @param name - The pipe name (must be valid identifier)
 * @param options - Materialized view configuration
 * @returns A pipe definition configured as a materialized view
 *
 * @example
 * ```ts
 * import { defineDatasource, defineMaterializedView, node, t, engine } from '@tinybirdco/sdk';
 *
 * // Target datasource for aggregated data
 * const salesByHour = defineDatasource('sales_by_hour', {
 *   schema: {
 *     day: t.date(),
 *     country: t.string().lowCardinality(),
 *     total_sales: t.simpleAggregateFunction('sum', t.uint64()),
 *   },
 *   engine: engine.aggregatingMergeTree({
 *     sortingKey: ['day', 'country'],
 *   }),
 * });
 *
 * // Materialized view - output schema is inferred from datasource
 * export const salesByHourMv = defineMaterializedView('sales_by_hour_mv', {
 *   description: 'Aggregate sales per hour',
 *   datasource: salesByHour,
 *   nodes: [
 *     node({
 *       name: 'daily_sales',
 *       sql: `
 *         SELECT
 *           toStartOfDay(starting_date) as day,
 *           country,
 *           sum(sales) as total_sales
 *         FROM teams
 *         GROUP BY day, country
 *       `,
 *     }),
 *   ],
 *   deploymentMethod: 'alter', // optional
 * });
 * ```
 */
export function defineMaterializedView<
  TSchema extends SchemaDefinition,
  TDatasource extends DatasourceDefinition<TSchema>
>(
  name: string,
  options: MaterializedViewOptions<TDatasource>
): PipeDefinition<Record<string, never>, DatasourceSchemaToOutput<TSchema>> {
  // Extract the schema from the datasource to build the output
  const datasourceSchema = options.datasource._schema as TSchema;
  const output: Record<string, AnyTypeValidator> = {};

  for (const [columnName, column] of Object.entries(datasourceSchema)) {
    output[columnName] = getColumnType(column);
  }

  return definePipe(name, {
    description: options.description,
    nodes: options.nodes,
    output: output as DatasourceSchemaToOutput<TSchema>,
    materialized: {
      datasource: options.datasource,
      deploymentMethod: options.deploymentMethod,
    },
    tokens: options.tokens,
  });
}

/**
 * Define a Tinybird endpoint
 *
 * This is a convenience function for creating API endpoints.
 * Endpoints are pipes that are exposed as HTTP API endpoints.
 *
 * @param name - The endpoint name (must be valid identifier)
 * @param options - Endpoint configuration including params, nodes, and output schema
 * @returns A pipe definition configured as an endpoint
 *
 * @example
 * ```ts
 * import { defineEndpoint, node, p, t } from '@tinybirdco/sdk';
 *
 * export const topEvents = defineEndpoint('top_events', {
 *   description: 'Get top events by count',
 *   params: {
 *     start_date: p.dateTime(),
 *     end_date: p.dateTime(),
 *     limit: p.int32().optional(10),
 *   },
 *   nodes: [
 *     node({
 *       name: 'aggregated',
 *       sql: `
 *         SELECT
 *           event_type,
 *           count() as event_count
 *         FROM events
 *         WHERE timestamp BETWEEN {{DateTime(start_date)}} AND {{DateTime(end_date)}}
 *         GROUP BY event_type
 *         ORDER BY event_count DESC
 *         LIMIT {{Int32(limit, 10)}}
 *       `,
 *     }),
 *   ],
 *   output: {
 *     event_type: t.string(),
 *     event_count: t.uint64(),
 *   },
 * });
 * ```
 */
export function defineEndpoint<
  TParams extends ParamsDefinition,
  TOutput extends OutputDefinition
>(
  name: string,
  options: EndpointOptions<TParams, TOutput>
): PipeDefinition<TParams, TOutput> {
  return definePipe(name, {
    description: options.description,
    params: options.params,
    nodes: options.nodes,
    output: options.output,
    endpoint: options.cache ? { enabled: true, cache: options.cache } : true,
    tokens: options.tokens,
  });
}

/**
 * Define a Tinybird copy pipe
 *
 * Copy pipes capture the result of a pipe at a moment in time and write
 * the result into a target data source. They can be run on a schedule,
 * or executed on demand.
 *
 * Unlike materialized views which continuously update as new events are inserted,
 * copy pipes generate a single snapshot at a specific point in time.
 *
 * @param name - The copy pipe name (must be valid identifier)
 * @param options - Copy pipe configuration
 * @returns A pipe definition configured as a copy pipe
 *
 * @example
 * ```ts
 * import { defineCopyPipe, defineDatasource, node, t, engine } from '@tinybirdco/sdk';
 *
 * // Target datasource for daily snapshots
 * const dailySalesSnapshot = defineDatasource('daily_sales_snapshot', {
 *   schema: {
 *     snapshot_date: t.date(),
 *     country: t.string(),
 *     total_sales: t.uint64(),
 *   },
 *   engine: engine.mergeTree({
 *     sortingKey: ['snapshot_date', 'country'],
 *   }),
 * });
 *
 * // Copy pipe that runs daily at midnight
 * export const dailySalesCopy = defineCopyPipe('daily_sales_copy', {
 *   description: 'Daily snapshot of sales by country',
 *   datasource: dailySalesSnapshot,
 *   copy_schedule: '0 0 * * *', // Daily at midnight UTC
 *   copy_mode: 'append',
 *   nodes: [
 *     node({
 *       name: 'snapshot',
 *       sql: `
 *         SELECT
 *           today() AS snapshot_date,
 *           country,
 *           sum(sales) AS total_sales
 *         FROM sales
 *         WHERE date = today() - 1
 *         GROUP BY country
 *       `,
 *     }),
 *   ],
 * });
 * ```
 */
export function defineCopyPipe<
  TSchema extends SchemaDefinition,
  TDatasource extends DatasourceDefinition<TSchema>
>(
  name: string,
  options: CopyPipeOptions<TSchema, TDatasource>
): PipeDefinition<Record<string, never>, DatasourceSchemaToOutput<TSchema>> {
  // Extract the schema from the datasource to build the output
  const datasourceSchema = options.datasource._schema as TSchema;
  const output: Record<string, AnyTypeValidator> = {};

  for (const [columnName, column] of Object.entries(datasourceSchema)) {
    output[columnName] = getColumnType(column);
  }

  return definePipe(name, {
    description: options.description,
    nodes: options.nodes,
    output: output as DatasourceSchemaToOutput<TSchema>,
    copy: {
      datasource: options.datasource,
      copy_mode: options.copy_mode,
      copy_schedule: options.copy_schedule,
    },
    tokens: options.tokens,
  });
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
 * Get the materialized view configuration from a pipe
 */
export function getMaterializedConfig(pipe: PipeDefinition): MaterializedConfig | null {
  return pipe.options.materialized ?? null;
}

/**
 * Check if a pipe is a materialized view
 */
export function isMaterializedView(pipe: PipeDefinition): boolean {
  return pipe.options.materialized !== undefined;
}

/**
 * Get the copy pipe configuration from a pipe
 */
export function getCopyConfig(pipe: PipeDefinition): CopyConfig | null {
  return pipe.options.copy ?? null;
}

/**
 * Check if a pipe is a copy pipe
 */
export function isCopyPipe(pipe: PipeDefinition): boolean {
  return pipe.options.copy !== undefined;
}

/**
 * Get the sink configuration from a pipe
 */
export function getSinkConfig(pipe: PipeDefinition): SinkConfig | null {
  return pipe.options.sink ?? null;
}

/**
 * Check if a pipe is a sink pipe
 */
export function isSinkPipe(pipe: PipeDefinition): boolean {
  return pipe.options.sink !== undefined;
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
