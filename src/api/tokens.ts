/**
 * Tinybird Token API client
 */

import { tinybirdFetch } from "./fetcher.js";

/**
 * API configuration for token operations.
 * Requires a token with TOKENS or ADMIN scope.
 */
export interface TokenApiConfig {
  /** Tinybird API base URL */
  baseUrl: string;
  /** Workspace token with TOKENS or ADMIN scope */
  token: string;
}

/**
 * Error thrown by token API operations
 */
export class TokenApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "TokenApiError";
  }
}

/**
 * Scope type for JWT tokens
 */
export type JWTScopeType =
  | "PIPES:READ"
  | "DATASOURCES:READ"
  | "DATASOURCES:APPEND";

/**
 * A scope definition for JWT tokens
 */
export interface JWTScope {
  /** The type of access being granted */
  type: JWTScopeType;
  /** The resource name (pipe or datasource) */
  resource: string;
  /** Fixed parameters embedded in the JWT (for pipes) */
  fixed_params?: Record<string, string | number | boolean>;
  /** SQL filter expression (for datasources) */
  filter?: string;
}

/**
 * Rate limiting configuration for JWT tokens
 */
export interface JWTLimits {
  /** Requests per second limit */
  rps?: number;
}

/**
 * Options for creating a JWT token
 */
export interface CreateJWTOptions {
  /** Token name/identifier */
  name: string;
  /** Expiration time as Date, Unix timestamp (number), or ISO string */
  expiresAt: Date | number | string;
  /** Array of scopes defining access permissions */
  scopes: JWTScope[];
  /** Optional rate limiting configuration */
  limits?: JWTLimits;
}

/**
 * Result of creating a JWT token
 */
export interface CreateJWTResult {
  /** The generated JWT token string */
  token: string;
}

/**
 * Request body for creating a JWT token
 */
interface CreateJWTRequestBody {
  name: string;
  scopes: JWTScope[];
  limits?: JWTLimits;
}

/**
 * Convert expiration input to Unix timestamp
 */
function toUnixTimestamp(expiresAt: Date | number | string): number {
  if (typeof expiresAt === "number") {
    return expiresAt;
  }
  if (expiresAt instanceof Date) {
    return Math.floor(expiresAt.getTime() / 1000);
  }
  return Math.floor(new Date(expiresAt).getTime() / 1000);
}

/**
 * Create a JWT token
 * POST /v0/tokens/?expiration_time={unix_timestamp}
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @param options - JWT creation options
 * @returns The created JWT token
 */
export async function createJWT(
  config: TokenApiConfig,
  options: CreateJWTOptions
): Promise<CreateJWTResult> {
  const expirationTime = toUnixTimestamp(options.expiresAt);

  const url = new URL("/v0/tokens/", config.baseUrl);
  url.searchParams.set("expiration_time", String(expirationTime));

  const body: CreateJWTRequestBody = {
    name: options.name,
    scopes: options.scopes,
  };

  if (options.limits) {
    body.limits = options.limits;
  }

  const response = await tinybirdFetch(url.toString(), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${config.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const responseBody = await response.text();

    let message: string;

    if (response.status === 403) {
      message =
        `Permission denied creating JWT token. ` +
        `Make sure the token has TOKENS or ADMIN scope. ` +
        `API response: ${responseBody}`;
    } else if (response.status === 400) {
      message = `Invalid JWT token request: ${responseBody}`;
    } else {
      message = `Failed to create JWT token: ${response.status} ${response.statusText}. API response: ${responseBody}`;
    }

    throw new TokenApiError(message, response.status, responseBody);
  }

  const data = (await response.json()) as { token: string };
  return { token: data.token };
}
