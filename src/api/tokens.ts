/**
 * Tinybird Token API client
 */

import {
  createTinybirdApi,
  TinybirdApiError,
} from "./api.js";

/**
 * API configuration for token operations.
 * Requires a token with TOKENS or ADMIN scope.
 */
export interface TokenApiConfig {
  /** Tinybird API base URL */
  baseUrl: string;
  /** Workspace token with TOKENS or ADMIN scope */
  token: string;
  /** Custom fetch implementation (optional, defaults to global fetch) */
  fetch?: typeof fetch;
  /** Default timeout in milliseconds (optional) */
  timeout?: number;
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

  const body: CreateJWTRequestBody = {
    name: options.name,
    scopes: options.scopes,
  };

  if (options.limits) {
    body.limits = options.limits;
  }

  const api = createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });

  try {
    const result = await api.createToken(body, {
      expirationTime,
    });
    return { token: result.token };
  } catch (error) {
    if (!(error instanceof TinybirdApiError)) {
      throw error;
    }

    const responseBody = error.responseBody ?? error.message;
    let message: string;

    if (error.statusCode === 403) {
      message =
        `Permission denied creating JWT token. ` +
        `Make sure the token has TOKENS or ADMIN scope. ` +
        `API response: ${responseBody}`;
    } else if (error.statusCode === 400) {
      message = `Invalid JWT token request: ${responseBody}`;
    } else {
      message = `Failed to create JWT token: ${error.statusCode}. API response: ${responseBody}`;
    }

    throw new TokenApiError(message, error.statusCode, responseBody);
  }
}
