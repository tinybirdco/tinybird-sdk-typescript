import { loadEnvFiles } from "../src/cli/config.js";
import { tinybirdFetch } from "../src/api/fetcher.js";

export interface LiveE2EConfig {
  baseUrl: string;
  userToken: string;
  workspaceAdminToken: string;
}

export interface WorkspaceResponse {
  id: string;
  name: string;
  token?: string;
}

interface WorkspaceTokenInfo {
  id: string;
  name: string;
  scope?: string;
}

interface JsonRequestOptions {
  method: "GET" | "POST" | "DELETE";
  token: string;
  body?: string;
  contentType?: string;
}

// Load env from repository root where tinybird.config.* usually lives.
loadEnvFiles(process.cwd());

function parseJsonOrThrow(responseText: string, endpoint: string): unknown {
  try {
    return JSON.parse(responseText);
  } catch {
    throw new Error(`Expected JSON from ${endpoint}, got: ${responseText || "<empty>"}`);
  }
}

async function requestJson(endpoint: string, options: JsonRequestOptions): Promise<unknown> {
  const response = await tinybirdFetch(endpoint, {
    method: options.method,
    headers: {
      Authorization: `Bearer ${options.token}`,
      ...(options.contentType ? { "Content-Type": options.contentType } : {}),
    },
    body: options.body,
  });

  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(
      `Request failed (${options.method} ${endpoint}): ` +
        `${response.status} ${response.statusText} - ${responseText || "<empty>"}`
    );
  }

  return parseJsonOrThrow(responseText, endpoint);
}

export function getLiveE2EConfigFromEnv(): LiveE2EConfig | null {
  const userToken = process.env.TINYBIRD_E2E_USER_TOKEN;
  const workspaceAdminToken = process.env.TINYBIRD_E2E_WORKSPACE_ADMIN_TOKEN;
  const baseUrl = process.env.TINYBIRD_E2E_BASE_URL ?? "https://api.tinybird.co";

  if (!userToken || !workspaceAdminToken) {
    return null;
  }

  return {
    baseUrl,
    userToken,
    workspaceAdminToken,
  };
}

export function createWorkspaceName(prefix = "sdk_e2e"): string {
  const suffix = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${Date.now()}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");
}

export async function assertWorkspaceAdminToken(config: LiveE2EConfig): Promise<void> {
  const endpoint = new URL("/v1/workspace", config.baseUrl).toString();
  const payload = (await requestJson(endpoint, {
    method: "GET",
    token: config.workspaceAdminToken,
  })) as WorkspaceTokenInfo;

  if (!payload.id || !payload.name) {
    throw new Error("Workspace admin token validation failed: missing workspace identity in response.");
  }

  if (payload.scope !== "admin") {
    throw new Error(
      `Workspace admin token validation failed: expected scope=admin, got scope=${payload.scope ?? "unknown"}.`
    );
  }
}

export async function createWorkspace(
  config: LiveE2EConfig,
  workspaceName: string
): Promise<WorkspaceResponse> {
  const endpoint = new URL("/v1/workspaces", config.baseUrl).toString();
  const body = new URLSearchParams({ name: workspaceName }).toString();

  const payload = (await requestJson(endpoint, {
    method: "POST",
    token: config.userToken,
    body,
    contentType: "application/x-www-form-urlencoded",
  })) as WorkspaceResponse;

  if (!payload.id || !payload.name) {
    throw new Error("Workspace creation failed: response missing id or name.");
  }

  return payload;
}

export async function getWorkspaceWithToken(
  config: LiveE2EConfig,
  workspaceId: string
): Promise<WorkspaceResponse> {
  const endpoint = new URL(`/v1/workspaces/${encodeURIComponent(workspaceId)}`, config.baseUrl);
  endpoint.searchParams.set("with_token", "true");

  const payload = (await requestJson(endpoint.toString(), {
    method: "GET",
    token: config.userToken,
  })) as WorkspaceResponse;

  if (!payload.id || !payload.name) {
    throw new Error(`Failed to fetch workspace ${workspaceId}: response missing id or name.`);
  }

  return payload;
}

export async function waitForWorkspaceToken(
  config: LiveE2EConfig,
  workspaceId: string,
  maxAttempts = 15,
  delayMs = 1_000
): Promise<string> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const workspace = await getWorkspaceWithToken(config, workspaceId);
    if (workspace.token) {
      return workspace.token;
    }
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }

  throw new Error(`Timed out waiting for workspace token (workspace=${workspaceId}).`);
}

export async function createWorkspaceWithToken(
  config: LiveE2EConfig,
  namePrefix = "sdk_e2e"
): Promise<Required<WorkspaceResponse>> {
  const workspace = await createWorkspace(config, createWorkspaceName(namePrefix));
  const token = await waitForWorkspaceToken(config, workspace.id);
  return {
    id: workspace.id,
    name: workspace.name,
    token,
  };
}

export async function deleteWorkspace(
  config: LiveE2EConfig,
  workspaceId: string,
  workspaceName: string
): Promise<void> {
  const endpoint = new URL(`/v1/workspaces/${encodeURIComponent(workspaceId)}`, config.baseUrl);
  endpoint.searchParams.set("confirmation", workspaceName);

  await requestJson(endpoint.toString(), {
    method: "DELETE",
    token: config.userToken,
  });
}
