/**
 * Tinybird Secrets (variables) API client
 *
 * Wraps the `/v0/variables` endpoints used by `tb secret ls/set/rm` to manage
 * the actual value of a secret server-side. This is distinct from the
 * `secret()` helper in `src/schema/secret.ts`, which only emits a
 * `{{ tb_secret(...) }}` template reference for use inside datafiles - it
 * never talks to the API.
 */

import { createTinybirdApi } from "./api.js";

/**
 * API configuration for secret operations.
 * Requires a token with ADMIN scope (secrets are workspace-level).
 */
export interface SecretApiConfig {
  /** Tinybird API base URL */
  baseUrl: string;
  /** Workspace token with ADMIN scope */
  token: string;
  /** Custom fetch implementation (optional, defaults to global fetch) */
  fetch?: typeof fetch;
  /** Default timeout in milliseconds (optional) */
  timeout?: number;
}

/**
 * Error thrown by secret API operations.
 *
 * `message` and `body` are sanitized so a secret `value` passed to
 * `setSecret`/`createSecret`/`updateSecret` is never echoed back, even if the
 * underlying API error response happened to include it.
 */
export class SecretApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body?: string
  ) {
    super(message);
    this.name = "SecretApiError";
  }
}

/**
 * Secret metadata as returned by `GET /v0/variables` and used by `tb secret ls`.
 *
 * Note: this is metadata only. The Tinybird API does not return the secret's
 * plaintext value in list responses, and this client never reads/returns the
 * value from `GET /v0/variables/{name}` either (that endpoint is only used
 * internally to check whether a secret already exists).
 */
export interface SecretInfo {
  /** Secret name */
  name: string;
  /** ISO creation timestamp, if returned by the API */
  created_at?: string;
  /** ISO last-updated timestamp, if returned by the API */
  updated_at?: string;
}

/**
 * Result of creating or updating a secret
 */
export interface SetSecretResult {
  /** Secret name */
  name: string;
  /** `true` if the secret was newly created, `false` if an existing secret was updated */
  created: boolean;
}

/**
 * Result of removing a secret
 */
export interface RemoveSecretResult {
  /** Secret name */
  name: string;
}

function assertValidName(name: string): void {
  if (!name || name.trim().length === 0) {
    throw new Error("Secret name must be a non-empty string.");
  }
}

/**
 * Redact a sensitive value from arbitrary response text before it ever
 * reaches an error message, in case the API ever echoes a submitted value
 * back in an error response.
 */
function redact(text: string, sensitiveValue?: string): string {
  if (!sensitiveValue) {
    return text;
  }
  return text.split(sensitiveValue).join("[REDACTED]");
}

async function throwForFailedResponse(
  response: Response,
  sensitiveValue?: string
): Promise<never> {
  const rawBody = await response.text().catch(() => "");
  const sanitizedBody = redact(rawBody, sensitiveValue);

  let parsed: { error?: string } | undefined;
  try {
    parsed = sanitizedBody ? (JSON.parse(sanitizedBody) as { error?: string }) : undefined;
  } catch {
    // ignore parse error and keep the sanitized raw body
  }

  const message =
    parsed?.error ??
    (sanitizedBody
      ? `Request failed with status ${response.status}: ${sanitizedBody}`
      : `Request failed with status ${response.status}`);

  throw new SecretApiError(message, response.status, sanitizedBody || undefined);
}

function getApi(config: SecretApiConfig) {
  return createTinybirdApi({
    baseUrl: config.baseUrl,
    token: config.token,
    fetch: config.fetch,
    timeout: config.timeout,
  });
}

/**
 * List secrets
 * GET /v0/variables
 *
 * Returns metadata only (name + timestamps), matching `tb secret ls` which
 * never prints secret values.
 *
 * @param config - API configuration (requires ADMIN scope)
 * @returns The list of secrets defined in the workspace
 */
export async function listSecrets(config: SecretApiConfig): Promise<SecretInfo[]> {
  const api = getApi(config);

  const response = await api.request("/v0/variables", { method: "GET" });

  if (!response.ok) {
    await throwForFailedResponse(response);
  }

  const data = (await response.json()) as { variables?: SecretInfo[] };
  return data.variables ?? [];
}

/**
 * Check whether a secret already exists.
 * GET /v0/variables/{name}
 *
 * Mirrors the Tinybird CLI's existing-secret check (see
 * `tinybird/tb/modules/secret_common.py` / `secret.py` `secret_set`), which
 * treats any failure to fetch the secret (404, permission error, etc.) as
 * "it doesn't exist yet" and falls back to creating it.
 *
 * This never parses or returns the response body, since it is unconfirmed
 * whether this endpoint includes the plaintext value for an existing secret.
 */
async function secretExists(
  api: ReturnType<typeof createTinybirdApi>,
  name: string
): Promise<boolean> {
  try {
    const response = await api.request(`/v0/variables/${encodeURIComponent(name)}`, {
      method: "GET",
    });
    await response.text().catch(() => undefined);
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * Create a new secret
 * POST /v0/variables
 *
 * @param config - API configuration (requires ADMIN scope)
 * @param name - Secret name
 * @param value - Secret value
 */
export async function createSecret(
  config: SecretApiConfig,
  name: string,
  value: string
): Promise<void> {
  assertValidName(name);
  const api = getApi(config);

  const body = new URLSearchParams();
  body.set("name", name);
  body.set("value", value);

  const response = await api.request("/v0/variables", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!response.ok) {
    await throwForFailedResponse(response, value);
  }

  // Never parse/return the success body - it is unconfirmed whether it
  // echoes the plaintext value, and callers never need it.
  await response.text().catch(() => undefined);
}

/**
 * Update an existing secret's value
 * PUT /v0/variables/{name}
 *
 * @param config - API configuration (requires ADMIN scope)
 * @param name - Secret name
 * @param value - New secret value
 */
export async function updateSecret(
  config: SecretApiConfig,
  name: string,
  value: string
): Promise<void> {
  assertValidName(name);
  const api = getApi(config);

  const body = new URLSearchParams();
  body.set("value", value);

  const response = await api.request(`/v0/variables/${encodeURIComponent(name)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!response.ok) {
    await throwForFailedResponse(response, value);
  }

  await response.text().catch(() => undefined);
}

/**
 * Create or update a secret, transparently.
 *
 * Checks whether the secret already exists (`GET /v0/variables/{name}`) and
 * creates it (`POST /v0/variables`) or updates it (`PUT /v0/variables/{name}`)
 * accordingly - mirroring `tb secret set` / `load_secrets` in the Tinybird CLI.
 *
 * @param config - API configuration (requires ADMIN scope)
 * @param name - Secret name
 * @param value - Secret value
 * @returns Whether the secret was created or updated
 */
export async function setSecret(
  config: SecretApiConfig,
  name: string,
  value: string
): Promise<SetSecretResult> {
  assertValidName(name);
  const api = getApi(config);
  const exists = await secretExists(api, name);

  if (exists) {
    await updateSecret(config, name, value);
    return { name, created: false };
  }

  await createSecret(config, name, value);
  return { name, created: true };
}

/**
 * Delete a secret
 * DELETE /v0/variables/{name}
 *
 * @param config - API configuration (requires ADMIN scope)
 * @param name - Secret name
 */
export async function removeSecret(
  config: SecretApiConfig,
  name: string
): Promise<RemoveSecretResult> {
  assertValidName(name);
  const api = getApi(config);

  const response = await api.request(`/v0/variables/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });

  if (!response.ok) {
    await throwForFailedResponse(response);
  }

  await response.text().catch(() => undefined);
  return { name };
}
