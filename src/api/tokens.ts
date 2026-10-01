/**
 * Tinybird Token API client
 */

import {
  createTinybirdApi,
  TinybirdApiError,
  type TinybirdApiTokenInfo,
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

/**
 * A scope granted to a token, as returned by the Token API.
 */
export interface TokenScope {
  /** The type of access granted (e.g. "PIPES:READ", "DATASOURCES:APPEND", "ADMIN") */
  type: string;
  /** The resource name the scope applies to (pipe or datasource), if any */
  resource?: string;
  /** SQL filter expression applied to the scope (for datasource scopes), if any */
  filter?: string;
}

/**
 * Token information as returned by list/get/refresh/update operations.
 */
export interface TokenInfo {
  /** Token ID */
  id?: string;
  /** Token name */
  name: string;
  /** Human-readable description */
  description?: string;
  /** Scopes granted to the token (included by the "get" operation) */
  scopes?: TokenScope[];
  /**
   * The raw token secret value, when exposed by the API for this token.
   * This is the value a human would otherwise copy from `tb token copy`.
   */
  value?: string;
}

/**
 * Convert a raw API token payload into the public `TokenInfo` shape.
 */
function toTokenInfo(raw: TinybirdApiTokenInfo): TokenInfo {
  return {
    id: raw.id,
    name: raw.name,
    description: raw.description,
    scopes: raw.scopes?.map((scope) => ({
      type: scope.type,
      resource: scope.resource,
      filter: scope.filter,
    })),
    value: raw.token,
  };
}

/**
 * Build a `TokenApiError` from a caught error, with context-aware messages
 * mirroring the style used by `createJWT`. Rethrows non-API errors as-is.
 */
function toTokenApiError(error: unknown, action: string): never {
  if (!(error instanceof TinybirdApiError)) {
    throw error;
  }

  const responseBody = error.responseBody ?? error.message;
  let message: string;

  if (error.statusCode === 403) {
    message =
      `Permission denied ${action}. ` +
      `Make sure the token has TOKENS or ADMIN scope. ` +
      `API response: ${responseBody}`;
  } else if (error.statusCode === 404) {
    message = `Not found while ${action}. API response: ${responseBody}`;
  } else if (error.statusCode === 400) {
    message = `Invalid request while ${action}: ${responseBody}`;
  } else {
    message = `Failed while ${action}: ${error.statusCode}. API response: ${responseBody}`;
  }

  throw new TokenApiError(message, error.statusCode, responseBody);
}

/**
 * List all tokens in the workspace.
 * GET /v0/tokens
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @returns Array of tokens, including their IDs and (when exposed) raw values
 *
 * @example
 * ```ts
 * const tokens = await listTokens(config);
 * console.log(tokens.map((t) => t.name));
 * ```
 */
export async function listTokens(config: TokenApiConfig): Promise<TokenInfo[]> {
  const api = createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });

  try {
    const tokens = await api.listTokens();
    return tokens.map(toTokenInfo);
  } catch (error) {
    toTokenApiError(error, "listing tokens");
  }
}

/**
 * Get a single token's details, including its scopes.
 * GET /v0/tokens/{name_or_id}
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @param nameOrId - Token name or ID
 * @returns The token's details, including its scopes
 *
 * @example
 * ```ts
 * const token = await getToken(config, "user_123_token");
 * console.log(token.scopes);
 * ```
 */
export async function getToken(
  config: TokenApiConfig,
  nameOrId: string
): Promise<TokenInfo> {
  const api = createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });

  try {
    const token = await api.getToken(nameOrId);
    return toTokenInfo(token);
  } catch (error) {
    toTokenApiError(error, `getting token '${nameOrId}'`);
  }
}

/**
 * Get a token's raw secret value.
 *
 * This is the SDK equivalent of `tb token copy`: the Forward CLI copies the
 * value to the OS clipboard, which doesn't make sense in an SDK context, so
 * this instead just returns the raw string for the caller to use as they see
 * fit (e.g. hand it to another service, store it, etc).
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @param nameOrId - Token name or ID
 * @returns The raw token secret value
 *
 * @example
 * ```ts
 * const value = await getTokenValue(config, "user_123_token");
 * console.log(value); // "p...."
 * ```
 */
export async function getTokenValue(
  config: TokenApiConfig,
  nameOrId: string
): Promise<string> {
  const token = await getToken(config, nameOrId);

  if (!token.value) {
    throw new TokenApiError(
      `Token '${nameOrId}' does not expose a retrievable raw value`,
      404
    );
  }

  return token.value;
}

/**
 * Delete (revoke) a token.
 * DELETE /v0/tokens/{name_or_id}
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @param nameOrId - Token name or ID
 *
 * @example
 * ```ts
 * await deleteToken(config, "user_123_token");
 * ```
 */
export async function deleteToken(
  config: TokenApiConfig,
  nameOrId: string
): Promise<void> {
  const api = createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });

  try {
    await api.deleteToken(nameOrId);
  } catch (error) {
    toTokenApiError(error, `deleting token '${nameOrId}'`);
  }
}

/**
 * Refresh (rotate) a token's value. The token keeps its name/scopes but gets
 * a new secret value; the previous value stops working immediately.
 * POST /v0/tokens/{name_or_id}/refresh
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @param nameOrId - Token name or ID
 * @returns The refreshed token's details
 *
 * @example
 * ```ts
 * const refreshed = await refreshToken(config, "user_123_token");
 * console.log(refreshed.value); // new raw value, if exposed by the API
 * ```
 */
export async function refreshToken(
  config: TokenApiConfig,
  nameOrId: string
): Promise<TokenInfo> {
  const api = createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });

  try {
    const token = await api.refreshToken(nameOrId);
    return toTokenInfo(token);
  } catch (error) {
    toTokenApiError(error, `refreshing token '${nameOrId}'`);
  }
}

/**
 * Update a token's scopes.
 * PUT /v0/tokens/{name_or_id}
 *
 * @param config - API configuration (requires TOKENS or ADMIN scope)
 * @param nameOrId - Token name or ID
 * @param scopes - The new set of scopes to apply to the token
 * @returns The updated token's details
 *
 * @example
 * ```ts
 * const updated = await updateTokenScopes(config, "user_123_token", [
 *   { type: "DATASOURCES:READ", resource: "events" },
 * ]);
 * ```
 */
export async function updateTokenScopes(
  config: TokenApiConfig,
  nameOrId: string,
  scopes: TokenScope[]
): Promise<TokenInfo> {
  const api = createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });

  try {
    const token = await api.updateTokenScopes(nameOrId, scopes);
    return toTokenInfo(token);
  } catch (error) {
    toTokenApiError(error, `updating scopes for token '${nameOrId}'`);
  }
}
