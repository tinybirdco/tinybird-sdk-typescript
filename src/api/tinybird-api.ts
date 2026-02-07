import { createTinybirdFetcher, type TinybirdFetch } from "./fetcher.js";
import type {
  IngestOptions,
  IngestResult,
  QueryOptions,
  QueryResult,
  TinybirdErrorResponse,
} from "../client/types.js";

const DEFAULT_TIMEOUT = 30000;

/**
 * Public, decoupled Tinybird API wrapper configuration
 */
export interface TinybirdApiConfig {
  /** Tinybird API base URL (e.g. https://api.tinybird.co) */
  baseUrl: string;
  /** Tinybird token used for Authorization bearer header */
  token: string;
  /** Custom fetch implementation (optional) */
  fetch?: typeof fetch;
  /** Default timeout in milliseconds (optional) */
  timeout?: number;
}

/**
 * Request options for the API layer
 */
export interface TinybirdApiRequestInit extends RequestInit {
  /** Optional token override for a specific request */
  token?: string;
}

export interface TinybirdApiQueryOptions extends QueryOptions {
  /** Optional token override for this request */
  token?: string;
}

export interface TinybirdApiIngestOptions extends IngestOptions {
  /** Optional token override for this request */
  token?: string;
}

/**
 * Error thrown by TinybirdApi when a response is not OK
 */
export class TinybirdApiError extends Error {
  readonly statusCode: number;
  readonly responseBody?: string;
  readonly response?: TinybirdErrorResponse;

  constructor(
    message: string,
    statusCode: number,
    responseBody?: string,
    response?: TinybirdErrorResponse
  ) {
    super(message);
    this.name = "TinybirdApiError";
    this.statusCode = statusCode;
    this.responseBody = responseBody;
    this.response = response;
  }
}

/**
 * Low-level Tinybird API wrapper.
 *
 * This layer is intentionally decoupled from the typed TinybirdClient layer
 * so it can be used standalone with just baseUrl + token.
 */
export class TinybirdApi {
  private readonly config: TinybirdApiConfig;
  private readonly baseUrl: string;
  private readonly defaultToken: string;
  private readonly fetchFn: TinybirdFetch;

  constructor(config: TinybirdApiConfig) {
    if (!config.baseUrl) {
      throw new Error("baseUrl is required");
    }

    if (!config.token) {
      throw new Error("token is required");
    }

    this.config = config;
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.defaultToken = config.token;
    this.fetchFn = createTinybirdFetcher(config.fetch ?? globalThis.fetch);
  }

  /**
   * Execute a request against Tinybird API.
   */
  request(path: string, init: TinybirdApiRequestInit = {}): Promise<Response> {
    const { token, headers, ...requestInit } = init;
    const authToken = token ?? this.defaultToken;
    const requestHeaders = new Headers(headers);

    if (!requestHeaders.has("Authorization")) {
      requestHeaders.set("Authorization", `Bearer ${authToken}`);
    }

    return this.fetchFn(this.resolveUrl(path), {
      ...requestInit,
      headers: requestHeaders,
    });
  }

  /**
   * Execute a request and parse JSON response.
   */
  async requestJson<T = unknown>(
    path: string,
    init: TinybirdApiRequestInit = {}
  ): Promise<T> {
    const response = await this.request(path, init);

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    return (await response.json()) as T;
  }

  /**
   * Query a Tinybird endpoint
   */
  async query<T = unknown>(
    endpointName: string,
    params: Record<string, unknown> = {},
    options: TinybirdApiQueryOptions = {}
  ): Promise<QueryResult<T>> {
    const url = new URL(`/v0/pipes/${endpointName}.json`, `${this.baseUrl}/`);

    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null) {
        continue;
      }

      if (Array.isArray(value)) {
        for (const item of value) {
          url.searchParams.append(key, String(item));
        }
        continue;
      }

      if (value instanceof Date) {
        url.searchParams.set(key, value.toISOString());
        continue;
      }

      url.searchParams.set(key, String(value));
    }

    const response = await this.request(url.toString(), {
      method: "GET",
      token: options.token,
      signal: this.createAbortSignal(options.timeout, options.signal),
    });

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    return (await response.json()) as QueryResult<T>;
  }

  /**
   * Ingest a single row into a datasource
   */
  async ingest<T extends Record<string, unknown>>(
    datasourceName: string,
    event: T,
    options: TinybirdApiIngestOptions = {}
  ): Promise<IngestResult> {
    return this.ingestBatch(datasourceName, [event], options);
  }

  /**
   * Ingest a batch of rows into a datasource
   */
  async ingestBatch<T extends Record<string, unknown>>(
    datasourceName: string,
    events: T[],
    options: TinybirdApiIngestOptions = {}
  ): Promise<IngestResult> {
    if (events.length === 0) {
      return { successful_rows: 0, quarantined_rows: 0 };
    }

    const url = new URL("/v0/events", `${this.baseUrl}/`);
    url.searchParams.set("name", datasourceName);

    if (options.wait !== false) {
      url.searchParams.set("wait", "true");
    }

    const ndjson = events
      .map((event) => JSON.stringify(this.serializeEvent(event)))
      .join("\n");

    const response = await this.request(url.toString(), {
      method: "POST",
      token: options.token,
      headers: {
        "Content-Type": "application/x-ndjson",
      },
      body: ndjson,
      signal: this.createAbortSignal(options.timeout, options.signal),
    });

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    return (await response.json()) as IngestResult;
  }

  /**
   * Execute raw SQL against Tinybird
   */
  async sql<T = unknown>(
    sql: string,
    options: TinybirdApiQueryOptions = {}
  ): Promise<QueryResult<T>> {
    const response = await this.request("/v0/sql", {
      method: "POST",
      token: options.token,
      headers: {
        "Content-Type": "text/plain",
      },
      body: sql,
      signal: this.createAbortSignal(options.timeout, options.signal),
    });

    if (!response.ok) {
      await this.handleErrorResponse(response);
    }

    return (await response.json()) as QueryResult<T>;
  }

  private createAbortSignal(
    timeout?: number,
    existingSignal?: AbortSignal
  ): AbortSignal | undefined {
    const timeoutMs = timeout ?? this.config.timeout ?? DEFAULT_TIMEOUT;

    if (!timeoutMs && !existingSignal) {
      return undefined;
    }

    if (!timeoutMs && existingSignal) {
      return existingSignal;
    }

    const timeoutSignal = AbortSignal.timeout(timeoutMs);

    if (!existingSignal) {
      return timeoutSignal;
    }

    return AbortSignal.any([timeoutSignal, existingSignal]);
  }

  private serializeEvent(
    event: Record<string, unknown>
  ): Record<string, unknown> {
    const serialized: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(event)) {
      serialized[key] = this.serializeValue(value);
    }

    return serialized;
  }

  private serializeValue(value: unknown): unknown {
    if (value instanceof Date) {
      return value.toISOString();
    }

    if (value instanceof Map) {
      return Object.fromEntries(value);
    }

    if (typeof value === "bigint") {
      return value.toString();
    }

    if (Array.isArray(value)) {
      return value.map((item) => this.serializeValue(item));
    }

    if (typeof value === "object" && value !== null) {
      return this.serializeEvent(value as Record<string, unknown>);
    }

    return value;
  }

  private async handleErrorResponse(response: Response): Promise<never> {
    const rawBody = await response.text();
    let errorResponse: TinybirdErrorResponse | undefined;

    try {
      errorResponse = JSON.parse(rawBody) as TinybirdErrorResponse;
    } catch {
      // ignore parse error and keep raw body
    }

    const message =
      errorResponse?.error ??
      (rawBody
        ? `Request failed with status ${response.status}: ${rawBody}`
        : `Request failed with status ${response.status}`);

    throw new TinybirdApiError(
      message,
      response.status,
      rawBody || undefined,
      errorResponse
    );
  }

  private resolveUrl(path: string): string {
    return new URL(path, `${this.baseUrl}/`).toString();
  }
}

/**
 * Create a decoupled Tinybird API wrapper.
 */
export function createTinybirdApi(
  config: TinybirdApiConfig
): TinybirdApi {
  return new TinybirdApi(config);
}

/**
 * Alias for teams that prefer "wrapper" naming.
 */
export const createTinybirdApiWrapper = createTinybirdApi;
