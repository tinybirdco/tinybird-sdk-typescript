/**
 * Token operations namespace for TinybirdClient
 */

import type {
  CreateJWTOptions,
  CreateJWTResult,
  TokenInfo,
  TokenScope,
} from "../api/tokens.js";
import {
  createJWT as apiCreateJWT,
  deleteToken as apiDeleteToken,
  getToken as apiGetToken,
  getTokenValue as apiGetTokenValue,
  listTokens as apiListTokens,
  refreshToken as apiRefreshToken,
  TokenApiError,
  updateTokenScopes as apiUpdateTokenScopes,
} from "../api/tokens.js";
import { TinybirdError } from "./types.js";

/**
 * Token operations namespace for TinybirdClient
 */
export class TokensNamespace {
  constructor(
    private readonly getToken: () => Promise<string>,
    private readonly baseUrl: string,
    private readonly fetchFn?: typeof globalThis.fetch,
    private readonly timeout?: number
  ) {}

  /**
   * Create a JWT token
   *
   * Creates a short-lived JWT token with specific scopes for secure,
   * time-limited access to pipes and datasources.
   *
   * @param options - JWT creation options
   * @returns The created JWT token
   *
   * @example
   * ```ts
   * const result = await client.tokens.createJWT({
   *   name: "user_123_token",
   *   expiresAt: new Date(Date.now() + 3600 * 1000), // 1 hour
   *   scopes: [
   *     {
   *       type: "PIPES:READ",
   *       resource: "user_analytics",
   *       fixed_params: { user_id: 123 },
   *     },
   *   ],
   * });
   *
   * console.log(result.token); // "eyJ..."
   * ```
   */
  async createJWT(options: CreateJWTOptions): Promise<CreateJWTResult> {
    const token = await this.getToken();

    try {
      return await apiCreateJWT(
        {
          baseUrl: this.baseUrl,
          token,
          fetch: this.fetchFn,
          timeout: this.timeout,
        },
        options
      );
    } catch (error) {
      if (error instanceof TokenApiError) {
        throw new TinybirdError(error.message, error.status, {
          error: error.message,
          status: error.status,
        });
      }
      throw error;
    }
  }

  /**
   * List all tokens in the workspace
   *
   * Requires a token with TOKENS or ADMIN scope.
   *
   * @returns Array of tokens, including their IDs and (when exposed) raw values
   *
   * @example
   * ```ts
   * const tokens = await client.tokens.list();
   * console.log(tokens.map((t) => t.name));
   * ```
   */
  async list(): Promise<TokenInfo[]> {
    const token = await this.getToken();

    try {
      return await apiListTokens(this.toApiConfig(token));
    } catch (error) {
      throw this.toTinybirdError(error);
    }
  }

  /**
   * Get a single token's details, including its scopes
   *
   * @param nameOrId - Token name or ID
   * @returns The token's details, including its scopes
   *
   * @example
   * ```ts
   * const token = await client.tokens.get("user_123_token");
   * console.log(token.scopes);
   * ```
   */
  async get(nameOrId: string): Promise<TokenInfo> {
    const token = await this.getToken();

    try {
      return await apiGetToken(this.toApiConfig(token), nameOrId);
    } catch (error) {
      throw this.toTinybirdError(error);
    }
  }

  /**
   * Get a token's raw secret value
   *
   * This is the SDK equivalent of `tb token copy`: the Forward CLI copies the
   * value to the OS clipboard, which doesn't make sense in an SDK context, so
   * this instead returns the raw string for the caller to use as needed.
   *
   * @param nameOrId - Token name or ID
   * @returns The raw token secret value
   *
   * @example
   * ```ts
   * const value = await client.tokens.getValue("user_123_token");
   * console.log(value); // "p...."
   * ```
   */
  async getValue(nameOrId: string): Promise<string> {
    const token = await this.getToken();

    try {
      return await apiGetTokenValue(this.toApiConfig(token), nameOrId);
    } catch (error) {
      throw this.toTinybirdError(error);
    }
  }

  /**
   * Revoke (delete) a token
   *
   * @param nameOrId - Token name or ID
   *
   * @example
   * ```ts
   * await client.tokens.revoke("user_123_token");
   * ```
   */
  async revoke(nameOrId: string): Promise<void> {
    const token = await this.getToken();

    try {
      await apiDeleteToken(this.toApiConfig(token), nameOrId);
    } catch (error) {
      throw this.toTinybirdError(error);
    }
  }

  /**
   * Refresh (rotate) a token's value
   *
   * The token keeps its name/scopes but gets a new secret value; the
   * previous value stops working immediately.
   *
   * @param nameOrId - Token name or ID
   * @returns The refreshed token's details
   *
   * @example
   * ```ts
   * const refreshed = await client.tokens.refresh("user_123_token");
   * console.log(refreshed.value); // new raw value, if exposed by the API
   * ```
   */
  async refresh(nameOrId: string): Promise<TokenInfo> {
    const token = await this.getToken();

    try {
      return await apiRefreshToken(this.toApiConfig(token), nameOrId);
    } catch (error) {
      throw this.toTinybirdError(error);
    }
  }

  /**
   * Update a token's scopes
   *
   * @param nameOrId - Token name or ID
   * @param scopes - The new set of scopes to apply to the token
   * @returns The updated token's details
   *
   * @example
   * ```ts
   * const updated = await client.tokens.updateScopes("user_123_token", [
   *   { type: "DATASOURCES:READ", resource: "events" },
   * ]);
   * ```
   */
  async updateScopes(nameOrId: string, scopes: TokenScope[]): Promise<TokenInfo> {
    const token = await this.getToken();

    try {
      return await apiUpdateTokenScopes(this.toApiConfig(token), nameOrId, scopes);
    } catch (error) {
      throw this.toTinybirdError(error);
    }
  }

  private toApiConfig(token: string) {
    return {
      baseUrl: this.baseUrl,
      token,
      fetch: this.fetchFn,
      timeout: this.timeout,
    };
  }

  private toTinybirdError(error: unknown): unknown {
    if (error instanceof TokenApiError) {
      return new TinybirdError(error.message, error.status, {
        error: error.message,
        status: error.status,
      });
    }
    return error;
  }
}
