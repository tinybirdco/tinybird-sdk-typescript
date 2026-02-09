/**
 * @tinybirdco/sdk - TypeScript SDK for Tinybird Forward
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
 * } from '@tinybirdco/sdk';
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
  InlineTokenConfig,
  DatasourceTokenReference,
  ExtractSchema,
  KafkaConfig,
} from "./schema/datasource.js";

// ============ Connection ============
export { createKafkaConnection, isConnectionDefinition, isKafkaConnectionDefinition, getConnectionType } from "./schema/connection.js";
export type {
  ConnectionDefinition,
  KafkaConnectionDefinition,
  KafkaConnectionOptions,
  KafkaSecurityProtocol,
  KafkaSaslMechanism,
} from "./schema/connection.js";

// ============ Token ============
export { defineToken, isTokenDefinition } from "./schema/token.js";
export type {
  TokenDefinition,
  DatasourceTokenScope,
  PipeTokenScope,
} from "./schema/token.js";

// ============ Pipe ============
export {
  definePipe,
  defineEndpoint,
  defineMaterializedView,
  defineCopyPipe,
  node,
  isPipeDefinition,
  isNodeDefinition,
  getEndpointConfig,
  getMaterializedConfig,
  getCopyConfig,
  isMaterializedView,
  isCopyPipe,
  getNodeNames,
  getNode,
  sql,
} from "./schema/pipe.js";
export type {
  PipeDefinition,
  PipeOptions,
  EndpointOptions,
  CopyPipeOptions,
  CopyConfig,
  NodeDefinition,
  NodeOptions,
  ParamsDefinition,
  OutputDefinition,
  EndpointConfig,
  MaterializedConfig,
  MaterializedViewOptions,
  PipeTokenConfig,
  InlinePipeTokenConfig,
  PipeTokenReference,
  ExtractParams,
  ExtractOutput,
} from "./schema/pipe.js";

// ============ Project ============
export { defineProject, isProjectDefinition, getDatasourceNames, getPipeNames, getDatasource, getPipe, createTinybirdClient } from "./schema/project.js";
export type {
  ProjectDefinition,
  ProjectConfig,
  ProjectClient,
  TinybirdClientConfig,
  DatasourcesDefinition,
  PipesDefinition,
  ConnectionsDefinition,
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
  InferMaterializedTarget,
  InferMaterializedTargetRow,
  IsMaterializedPipe,
} from "./infer/index.js";

// ============ Client ============
export { TinybirdClient, createClient } from "./client/base.js";
export { TinybirdError } from "./client/types.js";
export type {
  ClientConfig,
  ClientContext,
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

// ============ Public Tinybird API ============
export {
  TinybirdApi,
  TinybirdApiError,
  createTinybirdApi,
  createTinybirdApiWrapper,
} from "./api/api.js";
export type {
  TinybirdApiConfig,
  TinybirdApiQueryOptions,
  TinybirdApiIngestOptions,
  TinybirdApiRequestInit,
} from "./api/api.js";

// ============ Preview Environment ============
export {
  isPreviewEnvironment,
  getPreviewBranchName,
  resolveToken,
  clearTokenCache,
} from "./client/preview.js";

// ============ Dashboard URL Utilities ============
export {
  parseApiUrl,
  getDashboardUrl,
  getBranchDashboardUrl,
  getLocalDashboardUrl,
} from "./api/dashboard.js";
export type { RegionInfo } from "./api/dashboard.js";
