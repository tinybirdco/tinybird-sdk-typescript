/**
 * Token operations namespace for TinybirdClient
 */

import type { CreateJWTOptions, CreateJWTResult } from "../api/tokens.js";
import { createJWT as apiCreateJWT, TokenApiError } from "../api/tokens.js";
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
}
