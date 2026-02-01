/**
 * Tinybird client for querying pipes and ingesting events
 */

import type {
  ClientConfig,
  QueryResult,
  IngestResult,
  QueryOptions,
  IngestOptions,
  TinybirdErrorResponse,
} from "./types.js";
import { TinybirdError } from "./types.js";

/**
 * Default timeout for requests (30 seconds)
 */
const DEFAULT_TIMEOUT = 30000;

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
 * import { TinybirdClient } from '@tinybird/sdk';
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
  private readonly fetchFn: typeof fetch;
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

    this.fetchFn = config.fetch ?? globalThis.fetch;
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
    const url = new URL(`/v0/pipes/${pipeName}.json`, this.config.baseUrl);

    // Add parameters to query string
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null) {
        if (Array.isArray(value)) {
          // Handle array parameters
          for (const item of value) {
            url.searchParams.append(key, String(item));
          }
        } else if (value instanceof Date) {
          // Handle Date objects
          url.searchParams.set(key, value.toISOString());
        } else {
          url.searchParams.set(key, String(value));
        }
      }
    }

    const response = await this.fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
      signal: this.createAbortSignal(options.timeout, options.signal),
    });

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    const result = (await response.json()) as QueryResult<T>;
    return result;
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
    if (events.length === 0) {
      return { successful_rows: 0, quarantined_rows: 0 };
    }

    const token = await this.getToken();
    const url = new URL("/v0/events", this.config.baseUrl);
    url.searchParams.set("name", datasourceName);

    if (options.wait !== false) {
      url.searchParams.set("wait", "true");
    }

    // Convert events to NDJSON format
    const ndjson = events
      .map((event) => JSON.stringify(this.serializeEvent(event)))
      .join("\n");

    const response = await this.fetch(url.toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/x-ndjson",
      },
      body: ndjson,
      signal: this.createAbortSignal(options.timeout, options.signal),
    });

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    const result = (await response.json()) as IngestResult;
    return result;
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
    const url = new URL("/v0/sql", this.config.baseUrl);

    const response = await this.fetch(url.toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "text/plain",
      },
      body: sql,
      signal: this.createAbortSignal(options.timeout, options.signal),
    });

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    const result = (await response.json()) as QueryResult<T>;
    return result;
  }

  /**
   * Serialize an event for ingestion, handling Date objects and other special types
   */
  private serializeEvent<T extends Record<string, unknown>>(
    event: T
  ): Record<string, unknown> {
    const serialized: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(event)) {
      if (value instanceof Date) {
        // Convert Date to ISO string
        serialized[key] = value.toISOString();
      } else if (value instanceof Map) {
        // Convert Map to object
        serialized[key] = Object.fromEntries(value);
      } else if (typeof value === "bigint") {
        // Convert BigInt to string (ClickHouse will parse it)
        serialized[key] = value.toString();
      } else if (Array.isArray(value)) {
        // Recursively serialize array elements
        serialized[key] = value.map((item) =>
          typeof item === "object" && item !== null
            ? this.serializeEvent(item as Record<string, unknown>)
            : item instanceof Date
              ? item.toISOString()
              : item
        );
      } else if (typeof value === "object" && value !== null) {
        // Recursively serialize nested objects
        serialized[key] = this.serializeEvent(value as Record<string, unknown>);
      } else {
        serialized[key] = value;
      }
    }

    return serialized;
  }

  /**
   * Create an AbortSignal with timeout
   */
  private createAbortSignal(
    timeout?: number,
    existingSignal?: AbortSignal
  ): AbortSignal | undefined {
    const timeoutMs = timeout ?? this.config.timeout ?? DEFAULT_TIMEOUT;

    // If no timeout and no existing signal, return undefined
    if (!timeoutMs && !existingSignal) {
      return undefined;
    }

    // If only existing signal, return it
    if (!timeoutMs && existingSignal) {
      return existingSignal;
    }

    // Create timeout signal
    const timeoutSignal = AbortSignal.timeout(timeoutMs);

    // If only timeout, return timeout signal
    if (!existingSignal) {
      return timeoutSignal;
    }

    // Combine both signals
    return AbortSignal.any([timeoutSignal, existingSignal]);
  }

  /**
   * Handle error responses from the API
   */
  private async handleErrorResponse(response: Response): Promise<never> {
    let errorResponse: TinybirdErrorResponse | undefined;
    let rawBody: string | undefined;

    try {
      rawBody = await response.text();
      errorResponse = JSON.parse(rawBody) as TinybirdErrorResponse;
    } catch {
      // Failed to parse error response - include raw body in message
      if (rawBody) {
        throw new TinybirdError(
          `Request failed with status ${response.status}: ${rawBody}`,
          response.status,
          undefined
        );
      }
    }

    const message =
      errorResponse?.error ?? `Request failed with status ${response.status}`;

    throw new TinybirdError(message, response.status, errorResponse);
  }

  /**
   * Internal fetch wrapper
   */
  private fetch(url: string, init?: RequestInit): Promise<Response> {
    return this.fetchFn(url, init);
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
 * import { createClient } from '@tinybird/sdk';
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
