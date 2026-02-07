/**
 * Tinybird client for querying pipes and ingesting events
 */

import type {
  ClientConfig,
  QueryResult,
  IngestResult,
  QueryOptions,
  IngestOptions,
} from "./types.js";
import { TinybirdError } from "./types.js";
import { TinybirdApi, TinybirdApiError } from "../api/tinybird-api.js";

/**
 * Resolved token info from dev mode
 */
interface ResolvedTokenInfo {
  token: string;
  isBranchToken: boolean;
  branchName?: string;
}

/**
 * Tinybird API client
 *
 * Provides methods for querying pipe endpoints and ingesting events to datasources.
 *
 * @example
 * ```ts
 * import { TinybirdClient } from '@tinybirdco/sdk';
 *
 * const client = new TinybirdClient({
 *   baseUrl: 'https://api.tinybird.co',
 *   token: process.env.TINYBIRD_TOKEN,
 * });
 *
 * // Query a pipe
 * const result = await client.query('top_events', {
 *   start_date: '2024-01-01',
 *   end_date: '2024-01-31',
 * });
 *
 * // Ingest an event
 * await client.ingest('events', {
 *   timestamp: new Date().toISOString(),
 *   event_type: 'page_view',
 *   user_id: 'user_123',
 * });
 * ```
 */
export class TinybirdClient {
  private readonly config: ClientConfig;
  private readonly apisByToken = new Map<string, TinybirdApi>();
  private tokenPromise: Promise<ResolvedTokenInfo> | null = null;
  private resolvedToken: string | null = null;

  constructor(config: ClientConfig) {
    // Validate required config
    if (!config.baseUrl) {
      throw new Error("baseUrl is required");
    }
    if (!config.token) {
      throw new Error("token is required");
    }

    // Normalize base URL (remove trailing slash)
    this.config = {
      ...config,
      baseUrl: config.baseUrl.replace(/\/$/, ""),
    };
  }

  /**
   * Get the effective token, resolving branch token in dev mode if needed
   */
  private async getToken(): Promise<string> {
    // If already resolved, return it
    if (this.resolvedToken) {
      return this.resolvedToken;
    }

    // If not in dev mode, use the configured token
    if (!this.config.devMode) {
      this.resolvedToken = this.config.token;
      return this.resolvedToken;
    }

    // In dev mode, lazily resolve the branch token
    if (!this.tokenPromise) {
      this.tokenPromise = this.resolveBranchToken();
    }

    const resolved = await this.tokenPromise;
    this.resolvedToken = resolved.token;
    return this.resolvedToken;
  }

  /**
   * Resolve the branch token in dev mode
   */
  private async resolveBranchToken(): Promise<ResolvedTokenInfo> {
    try {
      // Dynamic import to avoid circular dependencies and to keep CLI code
      // out of the client bundle when not using dev mode
      const { loadConfig } = await import("../cli/config.js");
      const { getOrCreateBranch } = await import("../api/branches.js");

      const config = loadConfig();

      // If on main branch, use the workspace token
      if (config.isMainBranch || !config.tinybirdBranch) {
        return { token: this.config.token, isBranchToken: false };
      }

      const branchName = config.tinybirdBranch;

      // Get or create branch (always fetch fresh to avoid stale cache issues)
      const branch = await getOrCreateBranch(
        { baseUrl: this.config.baseUrl, token: this.config.token },
        branchName
      );

      if (!branch.token) {
        // Fall back to workspace token if no branch token
        return { token: this.config.token, isBranchToken: false };
      }

      return {
        token: branch.token,
        isBranchToken: true,
        branchName,
      };
    } catch {
      // If anything fails, fall back to the workspace token
      return { token: this.config.token, isBranchToken: false };
    }
  }

  /**
   * Query a pipe endpoint
   *
   * @param pipeName - Name of the pipe to query
   * @param params - Query parameters
   * @param options - Additional request options
   * @returns Query result with typed data
   */
  async query<T = unknown>(
    pipeName: string,
    params: Record<string, unknown> = {},
    options: QueryOptions = {}
  ): Promise<QueryResult<T>> {
    const token = await this.getToken();

    try {
      return await this.getApi(token).query<T>(pipeName, params, options);
    } catch (error) {
      this.rethrowApiError(error);
    }
  }

  /**
   * Ingest a single event to a datasource
   *
   * @param datasourceName - Name of the datasource
   * @param event - Event data to ingest
   * @param options - Additional request options
   * @returns Ingest result
   */
  async ingest<T extends Record<string, unknown>>(
    datasourceName: string,
    event: T,
    options: IngestOptions = {}
  ): Promise<IngestResult> {
    return this.ingestBatch(datasourceName, [event], options);
  }

  /**
   * Ingest multiple events to a datasource
   *
   * @param datasourceName - Name of the datasource
   * @param events - Array of events to ingest
   * @param options - Additional request options
   * @returns Ingest result
   */
  async ingestBatch<T extends Record<string, unknown>>(
    datasourceName: string,
    events: T[],
    options: IngestOptions = {}
  ): Promise<IngestResult> {
    const token = await this.getToken();

    try {
      return await this.getApi(token).ingestBatch(datasourceName, events, options);
    } catch (error) {
      this.rethrowApiError(error);
    }
  }

  /**
   * Execute a raw SQL query
   *
   * @param sql - SQL query to execute
   * @param options - Additional request options
   * @returns Query result
   */
  async sql<T = unknown>(
    sql: string,
    options: QueryOptions = {}
  ): Promise<QueryResult<T>> {
    const token = await this.getToken();

    try {
      return await this.getApi(token).sql<T>(sql, options);
    } catch (error) {
      this.rethrowApiError(error);
    }
  }

  private getApi(token: string): TinybirdApi {
    const existing = this.apisByToken.get(token);
    if (existing) {
      return existing;
    }

    const api = new TinybirdApi({
      baseUrl: this.config.baseUrl,
      token,
      fetch: this.config.fetch,
      timeout: this.config.timeout,
    });

    this.apisByToken.set(token, api);
    return api;
  }

  private rethrowApiError(error: unknown): never {
    if (error instanceof TinybirdApiError) {
      throw new TinybirdError(
        error.message,
        error.statusCode,
        error.response
      );
    }

    throw error;
  }
}

/**
 * Create a Tinybird client
 *
 * @param config - Client configuration
 * @returns Configured Tinybird client
 *
 * @example
 * ```ts
 * import { createClient } from '@tinybirdco/sdk';
 *
 * const client = createClient({
 *   baseUrl: process.env.TINYBIRD_URL,
 *   token: process.env.TINYBIRD_TOKEN,
 * });
 * ```
 */
export function createClient(config: ClientConfig): TinybirdClient {
  return new TinybirdClient(config);
}
