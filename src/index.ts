/**
 * @tinybird/sdk - TypeScript SDK for Tinybird Forward
 *
 * Define datasources and pipes as TypeScript with full type safety.
 *
 * @example
 * ```ts
 * import {
 *   defineDatasource,
 *   definePipe,
 *   defineProject,
 *   node,
 *   t,
 *   p,
 *   engine,
 *   createClient,
 * } from '@tinybird/sdk';
 *
 * // Define a datasource
 * const events = defineDatasource('events', {
 *   schema: {
 *     timestamp: t.dateTime(),
 *     user_id: t.string(),
 *     event_type: t.string().lowCardinality(),
 *   },
 *   engine: engine.mergeTree({
 *     sortingKey: ['user_id', 'timestamp'],
 *   }),
 * });
 *
 * // Define a pipe
 * const topEvents = definePipe('top_events', {
 *   params: {
 *     limit: p.int32().optional(10),
 *   },
 *   nodes: [
 *     node({
 *       name: 'aggregated',
 *       sql: 'SELECT event_type, count() as cnt FROM events GROUP BY event_type LIMIT {{Int32(limit, 10)}}',
 *     }),
 *   ],
 *   output: {
 *     event_type: t.string(),
 *     cnt: t.uint64(),
 *   },
 *   endpoint: true,
 * });
 *
 * // Create project
 * export default defineProject({
 *   datasources: { events },
 *   pipes: { topEvents },
 * });
 * ```
 */

// ============ Schema Types ============
export { t } from "./schema/types.js";
export type {
  TypeValidator,
  AnyTypeValidator,
  TypeModifiers,
  InferType,
  TinybirdType,
} from "./schema/types.js";
export {
  isTypeValidator,
  getTinybirdType,
  getModifiers,
} from "./schema/types.js";

// ============ Parameter Types ============
export { p } from "./schema/params.js";
export type {
  ParamValidator,
  AnyParamValidator,
  InferParamType,
} from "./schema/params.js";
export {
  isParamValidator,
  getParamTinybirdType,
  isParamRequired,
  getParamDefault,
  getParamDescription,
} from "./schema/params.js";

// ============ Engine Configurations ============
export { engine, getEngineClause, getSortingKey, getPrimaryKey } from "./schema/engines.js";
export type {
  EngineConfig,
  BaseMergeTreeConfig,
  MergeTreeConfig,
  ReplacingMergeTreeConfig,
  SummingMergeTreeConfig,
  AggregatingMergeTreeConfig,
  CollapsingMergeTreeConfig,
  VersionedCollapsingMergeTreeConfig,
} from "./schema/engines.js";

// ============ Datasource ============
export { defineDatasource, isDatasourceDefinition, column, getColumnType, getColumnJsonPath, getColumnNames } from "./schema/datasource.js";
export type {
  DatasourceDefinition,
  DatasourceOptions,
  SchemaDefinition,
  ColumnDefinition,
  TokenConfig,
  ExtractSchema,
} from "./schema/datasource.js";

// ============ Pipe ============
export { definePipe, node, isPipeDefinition, isNodeDefinition, getEndpointConfig, getNodeNames, getNode, sql } from "./schema/pipe.js";
export type {
  PipeDefinition,
  PipeOptions,
  NodeDefinition,
  NodeOptions,
  ParamsDefinition,
  OutputDefinition,
  EndpointConfig,
  PipeTokenConfig,
  ExtractParams,
  ExtractOutput,
} from "./schema/pipe.js";

// ============ Project ============
export { defineProject, isProjectDefinition, getDatasourceNames, getPipeNames, getDatasource, getPipe } from "./schema/project.js";
export type {
  ProjectDefinition,
  ProjectConfig,
  DatasourcesDefinition,
  PipesDefinition,
  ExtractDatasources,
  ExtractPipes,
  DataModel,
} from "./schema/project.js";

// ============ Type Inference ============
export type {
  Infer,
  InferRow,
  InferParams,
  InferOutput,
  InferOutputRow,
  InferEvent,
  PartialRow,
  InferSchema,
  InferTinybirdTypes,
} from "./infer/index.js";

// ============ Client ============
export { TinybirdClient, createClient } from "./client/base.js";
export { TinybirdError } from "./client/types.js";
export type {
  ClientConfig,
  QueryResult,
  IngestResult,
  QueryOptions,
  IngestOptions,
  ColumnMeta,
  QueryStatistics,
  TinybirdErrorResponse,
  TypedPipeEndpoint,
  TypedDatasourceIngest,
} from "./client/types.js";
