import { createTinybirdFetcher, type TinybirdFetch } from "./fetcher.js";

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
}

/**
 * Request options for the API layer
 */
export interface TinybirdApiRequestInit extends RequestInit {
  /** Optional token override for a specific request */
  token?: string;
}

/**
 * Error thrown by TinybirdApi when a response is not OK
 */
export class TinybirdApiError extends Error {
  readonly statusCode: number;
  readonly responseBody?: string;

  constructor(message: string, statusCode: number, responseBody?: string) {
    super(message);
    this.name = "TinybirdApiError";
    this.statusCode = statusCode;
    this.responseBody = responseBody;
  }
}

/**
 * Low-level Tinybird API wrapper.
 *
 * This layer is intentionally decoupled from the typed TinybirdClient layer
 * so it can be used standalone with just baseUrl + token.
 */
export class TinybirdApi {
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
      const body = await response.text();
      const details = body ? `: ${body}` : "";
      throw new TinybirdApiError(
        `Request failed with status ${response.status}${details}`,
        response.status,
        body || undefined
      );
    }

    return (await response.json()) as T;
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
