/**
 * Project definition for Tinybird
 * Aggregates all datasources and pipes into a single schema
 */

import type { DatasourceDefinition, SchemaDefinition } from "./datasource.js";
import type { PipeDefinition, ParamsDefinition, OutputDefinition } from "./pipe.js";
import { getEndpointConfig } from "./pipe.js";
import type { TinybirdClient } from "../client/base.js";
import type { QueryResult } from "../client/types.js";
import type { InferRow, InferParams, InferOutputRow } from "../infer/index.js";

// Symbol for brand typing
const PROJECT_BRAND = Symbol("tinybird.project");

/**
 * Collection of datasource definitions
 */
export type DatasourcesDefinition = Record<string, DatasourceDefinition<SchemaDefinition>>;

/**
 * Collection of pipe definitions
 */
export type PipesDefinition = Record<string, PipeDefinition<ParamsDefinition, OutputDefinition>>;

/**
 * Type for a single query method
 */
type QueryMethod<T extends PipeDefinition<ParamsDefinition, OutputDefinition>> =
  T extends PipeDefinition<infer P, OutputDefinition>
    ? keyof P extends never
      ? () => Promise<QueryResult<InferOutputRow<T>>>
      : (params: InferParams<T>) => Promise<QueryResult<InferOutputRow<T>>>
    : never;

/**
 * Type for query methods object
 * Note: At runtime, only pipes with endpoint: true are included
 */
type QueryMethods<T extends PipesDefinition> = {
  [K in keyof T]: QueryMethod<T[K]>;
};

/**
 * Type for a single ingest method
 */
type IngestMethod<T extends DatasourceDefinition<SchemaDefinition>> = (
  event: InferRow<T>
) => Promise<void>;

/**
 * Type for a batch ingest method
 */
type IngestBatchMethod<T extends DatasourceDefinition<SchemaDefinition>> = (
  events: InferRow<T>[]
) => Promise<void>;

/**
 * Type for ingest methods object
 */
type IngestMethods<T extends DatasourcesDefinition> = {
  [K in keyof T]: IngestMethod<T[K]>;
} & {
  [K in keyof T as `${K & string}Batch`]: IngestBatchMethod<T[K]>;
};

/**
 * Typed client interface for a project
 */
export interface ProjectClient<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
> {
  /** Query endpoint pipes */
  query: QueryMethods<TPipes>;
  /** Ingest events to datasources */
  ingest: IngestMethods<TDatasources>;
  /** Raw client for advanced usage */
  readonly client: TinybirdClient;
}

/**
 * Project configuration
 */
export interface ProjectConfig<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition
> {
  /** All datasources in this project */
  datasources?: TDatasources;
  /** All pipes in this project */
  pipes?: TPipes;
}

/**
 * A project definition with full type information
 */
export interface ProjectDefinition<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition
> {
  readonly [PROJECT_BRAND]: true;
  /** Type marker for inference */
  readonly _type: "project";
  /** All datasources */
  readonly datasources: TDatasources;
  /** All pipes */
  readonly pipes: TPipes;
  /** Typed Tinybird client */
  readonly tinybird: ProjectClient<TDatasources, TPipes>;
}

/**
 * Define a Tinybird project
 *
 * This aggregates all datasources and pipes into a single schema definition
 * that can be used for code generation and type inference.
 *
 * @param config - Project configuration with datasources and pipes
 * @returns A project definition
 *
 * @example
 * ```ts
 * // tinybird/schema.ts
 * import { defineProject } from '@tinybird/sdk';
 * import { events, users } from './datasources';
 * import { topEvents, userActivity } from './pipes';
 *
 * export default defineProject({
 *   datasources: {
 *     events,
 *     users,
 *   },
 *   pipes: {
 *     topEvents,
 *     userActivity,
 *   },
 * });
 * ```
 */
export function defineProject<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
>(
  config: ProjectConfig<TDatasources, TPipes>
): ProjectDefinition<TDatasources, TPipes> {
  const datasources = (config.datasources ?? {}) as TDatasources;
  const pipes = (config.pipes ?? {}) as TPipes;

  // Lazy client initialization
  let _client: TinybirdClient | null = null;

  const getClient = async (): Promise<TinybirdClient> => {
    if (!_client) {
      // Dynamic import to avoid circular dependencies
      const { createClient } = await import("../client/base.js");
      _client = createClient({
        baseUrl: process.env.TINYBIRD_URL ?? "https://api.tinybird.co",
        token: process.env.TINYBIRD_TOKEN!,
        devMode: process.env.NODE_ENV === "development",
      });
    }
    return _client;
  };

  // Build query methods for endpoint pipes
  const queryMethods: Record<string, (params?: unknown) => Promise<unknown>> = {};
  for (const [name, pipe] of Object.entries(pipes)) {
    const endpointConfig = getEndpointConfig(pipe);
    if (!endpointConfig) continue;

    // Use the Tinybird pipe name (snake_case)
    const tinybirdName = pipe._name;
    queryMethods[name] = async (params?: unknown) => {
      const client = await getClient();
      return client.query(tinybirdName, (params ?? {}) as Record<string, unknown>);
    };
  }

  // Build ingest methods for datasources
  const ingestMethods: Record<string, (data: unknown) => Promise<void>> = {};
  for (const [name, datasource] of Object.entries(datasources)) {
    // Use the Tinybird datasource name (snake_case)
    const tinybirdName = datasource._name;

    // Single event ingest
    ingestMethods[name] = async (event: unknown) => {
      const client = await getClient();
      await client.ingest(tinybirdName, event as Record<string, unknown>);
    };

    // Batch ingest
    ingestMethods[`${name}Batch`] = async (events: unknown) => {
      const client = await getClient();
      await client.ingestBatch(tinybirdName, events as Record<string, unknown>[]);
    };
  }

  // Create the typed client object
  const tinybird = {
    query: queryMethods,
    ingest: ingestMethods,
    get client(): TinybirdClient {
      // Synchronous client access - will throw if not initialized
      if (!_client) {
        throw new Error(
          "Client not initialized. Call a query or ingest method first, or access client asynchronously."
        );
      }
      return _client;
    },
  } as ProjectClient<TDatasources, TPipes>;

  return {
    [PROJECT_BRAND]: true,
    _type: "project",
    datasources,
    pipes,
    tinybird,
  };
}

/**
 * Check if a value is a project definition
 */
export function isProjectDefinition(value: unknown): value is ProjectDefinition {
  return (
    typeof value === "object" &&
    value !== null &&
    PROJECT_BRAND in value &&
    (value as Record<symbol, unknown>)[PROJECT_BRAND] === true
  );
}

/**
 * Get all datasource names from a project
 */
export function getDatasourceNames<T extends ProjectDefinition>(
  project: T
): (keyof T["datasources"])[] {
  return Object.keys(project.datasources) as (keyof T["datasources"])[];
}

/**
 * Get all pipe names from a project
 */
export function getPipeNames<T extends ProjectDefinition>(project: T): (keyof T["pipes"])[] {
  return Object.keys(project.pipes) as (keyof T["pipes"])[];
}

/**
 * Get a datasource by name from a project
 */
export function getDatasource<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition,
  K extends keyof TDatasources
>(project: ProjectDefinition<TDatasources, TPipes>, name: K): TDatasources[K] {
  return project.datasources[name];
}

/**
 * Get a pipe by name from a project
 */
export function getPipe<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition,
  K extends keyof TPipes
>(project: ProjectDefinition<TDatasources, TPipes>, name: K): TPipes[K] {
  return project.pipes[name];
}

/**
 * Helper type to extract datasources from a project
 */
export type ExtractDatasources<T> = T extends ProjectDefinition<infer D, PipesDefinition>
  ? D
  : never;

/**
 * Helper type to extract pipes from a project
 */
export type ExtractPipes<T> = T extends ProjectDefinition<DatasourcesDefinition, infer P>
  ? P
  : never;

/**
 * Data model type derived from a project
 * Useful for generating typed clients
 */
export type DataModel<T extends ProjectDefinition> = {
  datasources: {
    [K in keyof T["datasources"]]: T["datasources"][K] extends DatasourceDefinition<infer S>
      ? S
      : never;
  };
  pipes: {
    [K in keyof T["pipes"]]: T["pipes"][K] extends PipeDefinition<infer P, infer O>
      ? { params: P; output: O }
      : never;
  };
};
