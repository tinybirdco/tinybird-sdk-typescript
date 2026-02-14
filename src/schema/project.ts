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
  DeleteOptions,
  DeleteResult,
  IngestResult,
  QueryOptions,
  QueryResult,
  TruncateOptions,
  TruncateResult,
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
 * Type for pipe entity accessors object
 * Note: At runtime, all declared pipes are included. Non-endpoint pipes throw
 * when queried with a clear error message.
 */
type PipeEntityAccessors<T extends PipesDefinition> = {
  [K in keyof T]: {
    query: QueryMethod<T[K]>;
  };
};

/**
 * Type for a datasource accessor with import/mutation methods
 */
type DatasourceAccessor<T extends DatasourceDefinition<SchemaDefinition>> = {
  /** Ingest a single event row */
  ingest(event: InferRow<T>): Promise<IngestResult>;
  /** Append data from a URL or file */
  append(options: AppendOptions): Promise<AppendResult>;
  /** Replace datasource content from a URL or file */
  replace(options: AppendOptions): Promise<AppendResult>;
  /** Delete rows using a SQL condition */
  delete(options: DeleteOptions): Promise<DeleteResult>;
  /** Truncate all rows */
  truncate(options?: TruncateOptions): Promise<TruncateResult>;
};

/**
 * Type for datasource accessors object
 * Maps each datasource to an accessor with import/mutation methods
 */
type DatasourceAccessors<T extends DatasourcesDefinition> = {
  [K in keyof T]: DatasourceAccessor<T[K]>;
};

/**
 * Base project client interface
 */
interface ProjectClientBase {
  /** Token operations (JWT creation, etc.) */
  readonly tokens: TokensNamespace;
  /** Datasource operations (ingest/append/replace/delete/truncate) */
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
> = ProjectClientBase &
  DatasourceAccessors<TDatasources> &
  PipeEntityAccessors<TPipes>;

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

  // Create the typed Tinybird client
  const tinybird = new Tinybird({ datasources, pipes });

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

const RESERVED_CLIENT_NAMES = new Set(["tokens", "datasources", "sql", "client"]);

/**
 * Constructor interface for Tinybird class
 * This allows TypeScript to infer the correct return type with typed accessors
 */
interface TinybirdConstructor {
  new <TDatasources extends DatasourcesDefinition, TPipes extends PipesDefinition>(
    config: TinybirdClientConfig<TDatasources, TPipes>
  ): ProjectClient<TDatasources, TPipes>;
}

/**
 * Typed Tinybird client
 *
 * Creates a client with typed pipe query and datasource methods based on
 * the provided datasources and pipes.
 *
 * @example
 * ```ts
 * import { Tinybird } from '@tinybirdco/sdk';
 * import { pageViews, events } from './datasources';
 * import { topPages } from './pipes';
 *
 * export const tinybird = new Tinybird({
 *   datasources: { pageViews, events },
 *   pipes: { topPages },
 * });
 *
 * // Query a pipe (fully typed)
 * const result = await tinybird.topPages.query({
 *   start_date: new Date('2024-01-01'),
 *   end_date: new Date('2024-01-31'),
 * });
 *
 * // Ingest an event (fully typed)
 * await tinybird.pageViews.ingest({
 *   timestamp: new Date(),
 *   pathname: '/home',
 *   session_id: 'abc123',
 * });
 * ```
 */
export const Tinybird: TinybirdConstructor = class Tinybird<
  TDatasources extends DatasourcesDefinition = DatasourcesDefinition,
  TPipes extends PipesDefinition = PipesDefinition
> {
  #client: TinybirdClient | null = null;
  readonly #options: {
    baseUrl?: string;
    token?: string;
    configDir?: string;
    devMode?: boolean;
  };

  constructor(config: TinybirdClientConfig<TDatasources, TPipes>) {
    this.#options = {
      baseUrl: config.baseUrl,
      token: config.token,
      configDir: config.configDir,
      devMode: config.devMode,
    };

    // Build pipe accessors with query methods
    for (const [name, pipe] of Object.entries(config.pipes)) {
      if (name in config.datasources) {
        throw new Error(
          `Name conflict: "${name}" is defined as both datasource and pipe. ` +
            `Rename one of them to expose both as top-level client properties.`
        );
      }
      if (RESERVED_CLIENT_NAMES.has(name)) {
        throw new Error(
          `Name conflict: "${name}" is reserved by the client API. ` +
            `Rename this pipe to expose it as a top-level client property.`
        );
      }

      const endpointConfig = getEndpointConfig(pipe);

      if (!endpointConfig) {
        (this as Record<string, unknown>)[name] = {
          query: async () => {
            throw new Error(
              `Pipe "${name}" is not exposed as an endpoint. ` +
                `Set "endpoint: true" in the pipe definition to enable querying.`
            );
          },
        };
        continue;
      }

      const tinybirdName = pipe._name;
      (this as Record<string, unknown>)[name] = {
        query: async (params?: unknown) => {
          const client = await this.#getClient();
          return client.query(tinybirdName, (params ?? {}) as Record<string, unknown>);
        },
      };
    }

    // Build datasource accessors for top-level access
    for (const [name, datasource] of Object.entries(config.datasources)) {
      if (RESERVED_CLIENT_NAMES.has(name)) {
        throw new Error(
          `Name conflict: "${name}" is reserved by the client API. ` +
            `Rename this datasource to expose it as a top-level client property.`
        );
      }

      const tinybirdName = datasource._name;

      (this as Record<string, unknown>)[name] = {
        ingest: async (event: unknown) => {
          const client = await this.#getClient();
          return client.datasources.ingest(tinybirdName, event as Record<string, unknown>);
        },
        append: async (options: AppendOptions) => {
          const client = await this.#getClient();
          return client.datasources.append(tinybirdName, options);
        },
        replace: async (options: AppendOptions) => {
          const client = await this.#getClient();
          return client.datasources.replace(tinybirdName, options);
        },
        delete: async (options: DeleteOptions) => {
          const client = await this.#getClient();
          return client.datasources.delete(tinybirdName, options);
        },
        truncate: async (options: TruncateOptions = {}) => {
          const client = await this.#getClient();
          return client.datasources.truncate(tinybirdName, options);
        },
      };
    }
  }

  async #getClient(): Promise<TinybirdClient> {
    if (!this.#client) {
      const { createClient } = await import("../client/base.js");
      const { resolveToken } = await import("../client/preview.js");

      const baseUrl =
        this.#options.baseUrl ?? process.env.TINYBIRD_URL ?? "https://api.tinybird.co";
      const token = await resolveToken({ baseUrl, token: this.#options.token });

      this.#client = createClient({
        baseUrl,
        token,
        devMode: this.#options.devMode ?? process.env.NODE_ENV === "development",
        configDir: this.#options.configDir,
      });
    }
    return this.#client;
  }

  /** Execute raw SQL queries */
  async sql<T = unknown>(sqlQuery: string, options: QueryOptions = {}): Promise<QueryResult<T>> {
    const client = await this.#getClient();
    return client.sql<T>(sqlQuery, options);
  }

  /** Token operations (JWT creation, etc.) */
  get tokens(): TokensNamespace {
    if (!this.#client) {
      throw new Error(
        "Client not initialized. Call a query or ingest method first, or access client asynchronously."
      );
    }
    return this.#client.tokens;
  }

  /** Datasource operations (ingest/append/replace/delete/truncate) */
  get datasources(): DatasourcesNamespace {
    if (!this.#client) {
      throw new Error(
        "Client not initialized. Call a query or ingest method first, or access client asynchronously."
      );
    }
    return this.#client.datasources;
  }

  /** Raw client for advanced usage */
  get client(): TinybirdClient {
    if (!this.#client) {
      throw new Error(
        "Client not initialized. Call a query or ingest method first, or access client asynchronously."
      );
    }
    return this.#client;
  }
} as unknown as TinybirdConstructor;

/**
 * Create a typed Tinybird client
 *
 * @deprecated Use `new Tinybird(...)` instead. This function is kept for backward compatibility.
 *
 * @param config - Client configuration with datasources and pipes
 * @returns A typed client with pipe query and datasource methods
 */
export function createTinybirdClient<
  TDatasources extends DatasourcesDefinition,
  TPipes extends PipesDefinition
>(config: TinybirdClientConfig<TDatasources, TPipes>): ProjectClient<TDatasources, TPipes> {
  return new Tinybird(config);
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
