/**
 * Project definition for Tinybird
 * Aggregates all datasources and pipes into a single schema
 */

import type { DatasourceDefinition, SchemaDefinition } from "./datasource.js";
import type { PipeDefinition, ParamsDefinition, OutputDefinition } from "./pipe.js";
import type { ConnectionDefinition } from "./connection.js";
import { getEndpointConfig } from "./pipe.js";
import type { TinybirdClient } from "../client/base.js";
import type {
  AppendOptions,
  AppendResult,
  DatasourcesNamespace,
  QueryOptions,
  QueryResult,
} from "../client/types.js";
import type { InferRow, InferParams, InferOutputRow } from "../infer/index.js";
import type { TokensNamespace } from "../client/tokens.js";

// Symbol for brand typing - use Symbol.for() for global registry
// This ensures the same symbol is used across module instances
const PROJECT_BRAND = Symbol.for("tinybird.project");

/**
 * Collection of datasource definitions
 */
export type DatasourcesDefinition = Record<string, DatasourceDefinition<SchemaDefinition>>;

/**
 * Collection of pipe definitions
 */
export type PipesDefinition = Record<string, PipeDefinition<ParamsDefinition, OutputDefinition>>;

/**
 * Collection of connection definitions
 */
export type ConnectionsDefinition = Record<string, ConnectionDefinition>;

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
 * Type for pipe accessors object
 * Note: At runtime, all declared pipes are included. Non-endpoint pipes throw
 * when queried with a clear error message.
 */
type PipeAccessors<T extends PipesDefinition> = {
  [K in keyof T]: {
    query: QueryMethod<T[K]>;
  };
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
 * Type for a datasource accessor with append method
 */
type DatasourceAccessor = {
  /** Append data from a URL or file */
  append(options: AppendOptions): Promise<AppendResult>;
};

/**
 * Type for datasource accessors object
 * Maps each datasource to an accessor with append method
 */
type DatasourceAccessors<T extends DatasourcesDefinition> = {
  [K in keyof T]: DatasourceAccessor;
};

/**
 * Base project client interface
 */
interface ProjectClientBase<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
> {
  /** Query access by pipe name */
  pipes: PipeAccessors<TPipes>;
  /** Ingest events to datasources */
  ingest: IngestMethods<TDatasources>;
  /** Token operations (JWT creation, etc.) */
  readonly tokens: TokensNamespace;
  /** Datasource operations (append from URL/file) */
  readonly datasources: DatasourcesNamespace;
  /** Execute raw SQL queries */
  sql<T = unknown>(sql: string, options?: QueryOptions): Promise<QueryResult<T>>;
  /** Raw client for advanced usage */
  readonly client: TinybirdClient;
}

/**
 * Typed client interface for a project
 * Includes datasource accessors as top-level properties
 */
export type ProjectClient<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
> = ProjectClientBase<TDatasources, TPipes> & DatasourceAccessors<TDatasources>;

/**
 * Configuration for createTinybirdClient
 */
export interface TinybirdClientConfig<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition
> {
  /** All datasources */
  datasources: TDatasources;
  /** All pipes */
  pipes: TPipes;
  /** Tinybird API base URL (defaults to TINYBIRD_URL env var or https://api.tinybird.co) */
  baseUrl?: string;
  /** Tinybird API token (defaults to TINYBIRD_TOKEN env var) */
  token?: string;
  /**
   * Directory to use as the starting point when searching for tinybird.json config.
   * In monorepo setups, this should be set to the directory containing tinybird.json
   * to ensure the config is found regardless of where the application runs from.
   */
  configDir?: string;
  /**
   * Enable development mode for the client.
   * Defaults to `process.env.NODE_ENV === "development"` if not specified.
   */
  devMode?: boolean;
}

/**
 * Project configuration
 */
export interface ProjectConfig<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition,
  TConnections extends ConnectionsDefinition = ConnectionsDefinition
> {
  /** All datasources in this project */
  datasources?: TDatasources;
  /** All pipes in this project */
  pipes?: TPipes;
  /** All connections in this project */
  connections?: TConnections;
}

/**
 * A project definition with full type information
 */
export interface ProjectDefinition<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition,
  TConnections extends ConnectionsDefinition = ConnectionsDefinition
> {
  readonly [PROJECT_BRAND]: true;
  /** Type marker for inference */
  readonly _type: "project";
  /** All datasources */
  readonly datasources: TDatasources;
  /** All pipes */
  readonly pipes: TPipes;
  /** All connections */
  readonly connections: TConnections;
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
 * import { defineProject } from '@tinybirdco/sdk';
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
  TPipes extends PipesDefinition,
  TConnections extends ConnectionsDefinition
>(
  config: ProjectConfig<TDatasources, TPipes, TConnections>
): ProjectDefinition<TDatasources, TPipes, TConnections> {
  const datasources = (config.datasources ?? {}) as TDatasources;
  const pipes = (config.pipes ?? {}) as TPipes;
  const connections = (config.connections ?? {}) as TConnections;

  // Use the shared client builder
  const tinybird = buildProjectClient(datasources, pipes);

  return {
    [PROJECT_BRAND]: true,
    _type: "project",
    datasources,
    pipes,
    connections,
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
 * Build a typed Tinybird client from datasources and pipes
 *
 * This is an internal helper that builds pipe query and datasource ingest methods.
 */
function buildProjectClient<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
>(
  datasources: TDatasources,
  pipes: TPipes,
  options?: { baseUrl?: string; token?: string; configDir?: string; devMode?: boolean }
): ProjectClient<TDatasources, TPipes> {
  // Lazy client initialization
  let _client: TinybirdClient | null = null;

  const getClient = async (): Promise<TinybirdClient> => {
    if (!_client) {
      // Dynamic imports to avoid circular dependencies
      const { createClient } = await import("../client/base.js");
      const { resolveToken } = await import("../client/preview.js");

      // Resolve the token (handles preview environment detection)
      const baseUrl = options?.baseUrl ?? process.env.TINYBIRD_URL ?? "https://api.tinybird.co";
      const token = await resolveToken({ baseUrl, token: options?.token });

      _client = createClient({
        baseUrl,
        token,
        devMode: options?.devMode ?? process.env.NODE_ENV === "development",
        configDir: options?.configDir,
      });
    }
    return _client;
  };

  // Build pipe accessors with query methods
  const pipeAccessors: Record<string, { query: (params?: unknown) => Promise<unknown> }> = {};
  for (const [name, pipe] of Object.entries(pipes)) {
    const endpointConfig = getEndpointConfig(pipe);

    if (!endpointConfig) {
      // Non-endpoint pipes get a stub that throws a clear error
      pipeAccessors[name] = {
        query: async () => {
          throw new Error(
            `Pipe "${name}" is not exposed as an endpoint. ` +
              `Set "endpoint: true" in the pipe definition to enable querying.`
          );
        },
      };
      continue;
    }

    // Use the Tinybird pipe name (snake_case)
    const tinybirdName = pipe._name;
    pipeAccessors[name] = {
      query: async (params?: unknown) => {
        const client = await getClient();
        return client.query(tinybirdName, (params ?? {}) as Record<string, unknown>);
      },
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

  // Build datasource accessors for top-level access
  const datasourceAccessors: Record<string, DatasourceAccessor> = {};
  for (const [name, datasource] of Object.entries(datasources)) {
    const tinybirdName = datasource._name;

    datasourceAccessors[name] = {
      append: async (options: AppendOptions) => {
        const client = await getClient();
        return client.datasources.append(tinybirdName, options);
      },
    };
  }

  // Create the typed client object
  return {
    ...datasourceAccessors,
    pipes: pipeAccessors,
    ingest: ingestMethods,
    sql: async <T = unknown>(sql: string, options: QueryOptions = {}) => {
      const client = await getClient();
      return client.sql<T>(sql, options);
    },
    get tokens(): TokensNamespace {
      // Synchronous access - will throw if not initialized
      if (!_client) {
        throw new Error(
          "Client not initialized. Call a query or ingest method first, or access client asynchronously."
        );
      }
      return _client.tokens;
    },
    get datasources(): DatasourcesNamespace {
      // Synchronous access - will throw if not initialized
      if (!_client) {
        throw new Error(
          "Client not initialized. Call a query or ingest method first, or access client asynchronously."
        );
      }
      return _client.datasources;
    },
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
}

/**
 * Create a typed Tinybird client
 *
 * Creates a client with typed pipe query and datasource ingest methods based on
 * the provided
 * datasources and pipes. This is the recommended way to create a Tinybird client
 * when using the SDK's auto-generated client file.
 *
 * @param config - Client configuration with datasources and pipes
 * @returns A typed client with pipe query and datasource ingest methods
 *
 * @example
 * ```ts
 * import { createTinybirdClient } from '@tinybirdco/sdk';
 * import { pageViews, events } from './datasources';
 * import { topPages } from './pipes';
 *
 * export const tinybird = createTinybirdClient({
 *   datasources: { pageViews, events },
 *   pipes: { topPages },
 * });
 *
 * // Query a pipe (fully typed)
 * const result = await tinybird.pipes.topPages.query({
 *   start_date: new Date('2024-01-01'),
 *   end_date: new Date('2024-01-31'),
 * });
 *
 * // Ingest an event (fully typed)
 * await tinybird.ingest.pageViews({
 *   timestamp: new Date(),
 *   pathname: '/home',
 *   session_id: 'abc123',
 * });
 * ```
 */
export function createTinybirdClient<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
>(
  config: TinybirdClientConfig<TDatasources, TPipes>
): ProjectClient<TDatasources, TPipes> {
  return buildProjectClient(
    config.datasources,
    config.pipes,
    { baseUrl: config.baseUrl, token: config.token, configDir: config.configDir, devMode: config.devMode }
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
