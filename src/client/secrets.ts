/**
 * Secret operations namespace for TinybirdClient
 */

import type { RemoveSecretResult, SecretInfo, SetSecretResult } from "../api/secrets.js";
import {
  listSecrets as apiListSecrets,
  removeSecret as apiRemoveSecret,
  SecretApiError,
  setSecret as apiSetSecret,
} from "../api/secrets.js";
import { TinybirdError } from "./types.js";

/**
 * Secret operations namespace for TinybirdClient
 *
 * Manages the server-side value of a secret via the `/v0/variables` API
 * (`tb secret ls/set/rm`). This is a separate, complementary concern to the
 * `secret()` helper exported from the package root, which only produces a
 * `{{ tb_secret(...) }}` template reference for datafiles and never touches
 * the API.
 */
export class SecretsNamespace {
  constructor(
    private readonly getToken: () => Promise<string>,
    private readonly baseUrl: string,
    private readonly fetchFn?: typeof globalThis.fetch,
    private readonly timeout?: number
  ) {}

  /**
   * List secrets defined in the workspace
   *
   * Returns metadata only (name and timestamps) - secret values are never
   * included in the list response.
   *
   * @returns The list of secrets
   *
   * @example
   * ```ts
   * const secrets = await client.secrets.list();
   * console.log(secrets.map((s) => s.name)); // ["API_KEY", "DB_PASSWORD"]
   * ```
   */
  async list(): Promise<SecretInfo[]> {
    const token = await this.getToken();

    try {
      return await apiListSecrets({
        baseUrl: this.baseUrl,
        token,
        fetch: this.fetchFn,
        timeout: this.timeout,
      });
    } catch (error) {
      this.rethrow(error);
    }
  }

  /**
   * Create or update a secret, transparently
   *
   * Checks whether a secret with this name already exists and creates or
   * updates it accordingly, matching `tb secret set`.
   *
   * @param name - Secret name
   * @param value - Secret value
   * @returns Whether the secret was created or updated
   *
   * @example
   * ```ts
   * await client.secrets.set("API_KEY", "sk_live_...");
   * ```
   */
  async set(name: string, value: string): Promise<SetSecretResult> {
    const token = await this.getToken();

    try {
      return await apiSetSecret(
        {
          baseUrl: this.baseUrl,
          token,
          fetch: this.fetchFn,
          timeout: this.timeout,
        },
        name,
        value
      );
    } catch (error) {
      this.rethrow(error);
    }
  }

  /**
   * Delete a secret
   *
   * @param name - Secret name
   * @returns The name of the removed secret
   *
   * @example
   * ```ts
   * await client.secrets.remove("API_KEY");
   * ```
   */
  async remove(name: string): Promise<RemoveSecretResult> {
    const token = await this.getToken();

    try {
      return await apiRemoveSecret(
        {
          baseUrl: this.baseUrl,
          token,
          fetch: this.fetchFn,
          timeout: this.timeout,
        },
        name
      );
    } catch (error) {
      this.rethrow(error);
    }
  }

  private rethrow(error: unknown): never {
    if (error instanceof SecretApiError) {
      throw new TinybirdError(error.message, error.status, {
        error: error.message,
        status: error.status,
      });
    }
    throw error;
  }
}
