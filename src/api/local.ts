/**
 * Local Tinybird container API client
 * For use with tinybird-local Docker image
 */

import * as crypto from "crypto";
import { LOCAL_BASE_URL } from "../cli/config.js";

/**
 * Tokens returned by the local /tokens endpoint
 */
export interface LocalTokens {
  /** User token for user-level operations */
  user_token: string;
  /** Admin token for admin operations like listing workspaces */
  admin_token: string;
  /** Default workspace admin token */
  workspace_admin_token: string;
}

/**
 * Workspace info from local Tinybird
 */
export interface LocalWorkspace {
  /** Workspace ID */
  id: string;
  /** Workspace name */
  name: string;
  /** Workspace token */
  token: string;
}

/**
 * Response from /v1/user/workspaces endpoint
 */
interface UserWorkspacesResponse {
  organization_id?: string;
  workspaces: Array<{
    id: string;
    name: string;
    token: string;
  }>;
}

/**
 * Error thrown when local container is not running
 */
export class LocalNotRunningError extends Error {
  constructor() {
    super(
      `Tinybird local is not running. Start it with:\n` +
        `docker run -d -p 7181:7181 --name tinybird-local tinybirdco/tinybird-local:latest`
    );
    this.name = "LocalNotRunningError";
  }
}

/**
 * Error thrown by local API operations
 */
export class LocalApiError extends Error {
  constructor(
    message: string,
    public readonly status?: number,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "LocalApiError";
  }
}

/**
 * Check if local Tinybird container is running
 *
 * @returns true if container is running and healthy
 */
export async function isLocalRunning(): Promise<boolean> {
  try {
    const response = await fetch(`${LOCAL_BASE_URL}/tokens`, {
      method: "GET",
      signal: AbortSignal.timeout(5000),
    });
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * Get tokens from local Tinybird container
 *
 * @returns Local tokens
 * @throws LocalNotRunningError if container is not running
 */
export async function getLocalTokens(): Promise<LocalTokens> {
  try {
    const response = await fetch(`${LOCAL_BASE_URL}/tokens`, {
      method: "GET",
      signal: AbortSignal.timeout(5000),
    });

    if (!response.ok) {
      throw new LocalApiError(
        `Failed to get local tokens: ${response.status} ${response.statusText}`,
        response.status
      );
    }

    const tokens = (await response.json()) as LocalTokens;

    // Validate response structure
    if (!tokens.user_token || !tokens.admin_token || !tokens.workspace_admin_token) {
      throw new LocalApiError(
        "Invalid tokens response from local Tinybird - missing required fields"
      );
    }

    return tokens;
  } catch (error) {
    if (error instanceof LocalApiError) {
      throw error;
    }
    // Connection error - container not running
    throw new LocalNotRunningError();
  }
}

/**
 * List workspaces in local Tinybird
 *
 * @param adminToken - Admin token from getLocalTokens()
 * @returns List of workspaces with their info
 */
export async function listLocalWorkspaces(
  adminToken: string
): Promise<{ workspaces: LocalWorkspace[]; organizationId?: string }> {
  const url = `${LOCAL_BASE_URL}/v1/user/workspaces?with_organization=true&token=${adminToken}`;

  const response = await fetch(url, {
    method: "GET",
  });

  if (!response.ok) {
    const body = await response.text();
    throw new LocalApiError(
      `Failed to list local workspaces: ${response.status} ${response.statusText}`,
      response.status,
      body
    );
  }

  const data = (await response.json()) as UserWorkspacesResponse;

  return {
    workspaces: data.workspaces.map((ws) => ({
      id: ws.id,
      name: ws.name,
      token: ws.token,
    })),
    organizationId: data.organization_id,
  };
}

/**
 * Create a workspace in local Tinybird
 *
 * @param userToken - User token from getLocalTokens()
 * @param workspaceName - Name for the new workspace
 * @param organizationId - Organization ID to assign workspace to
 * @returns Created workspace info
 */
export async function createLocalWorkspace(
  userToken: string,
  workspaceName: string,
  organizationId?: string
): Promise<LocalWorkspace> {
  const url = `${LOCAL_BASE_URL}/v1/workspaces`;

  const formData = new URLSearchParams();
  formData.append("name", workspaceName);
  if (organizationId) {
    formData.append("assign_to_organization_id", organizationId);
  }

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${userToken}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: formData.toString(),
  });

  if (!response.ok) {
    const responseBody = await response.text();
    throw new LocalApiError(
      `Failed to create local workspace: ${response.status} ${response.statusText}`,
      response.status,
      responseBody
    );
  }

  const data = (await response.json()) as { id: string; name: string; token: string };

  return {
    id: data.id,
    name: data.name,
    token: data.token,
  };
}

/**
 * Get or create a workspace in local Tinybird
 *
 * @param tokens - Tokens from getLocalTokens()
 * @param workspaceName - Name of the workspace to get or create
 * @returns Workspace info and whether it was newly created
 */
export async function getOrCreateLocalWorkspace(
  tokens: LocalTokens,
  workspaceName: string
): Promise<{ workspace: LocalWorkspace; wasCreated: boolean }> {
  // List existing workspaces
  const { workspaces, organizationId } = await listLocalWorkspaces(tokens.admin_token);

  // Check if workspace already exists
  const existing = workspaces.find((ws) => ws.name === workspaceName);
  if (existing) {
    return { workspace: existing, wasCreated: false };
  }

  // Create new workspace
  await createLocalWorkspace(
    tokens.user_token,
    workspaceName,
    organizationId
  );

  // Fetch the workspace again to get the token (create response may not include it)
  const { workspaces: updatedWorkspaces } = await listLocalWorkspaces(tokens.admin_token);
  const newWorkspace = updatedWorkspaces.find((ws) => ws.name === workspaceName);

  if (!newWorkspace) {
    throw new LocalApiError(
      `Created workspace '${workspaceName}' but could not find it in workspace list`
    );
  }

  return { workspace: newWorkspace, wasCreated: true };
}

/**
 * Get workspace name for local mode based on git branch or path
 *
 * @param tinybirdBranch - Sanitized git branch name (or null if not in git)
 * @param cwd - Current working directory (used for hash if no branch)
 * @returns Workspace name to use
 */
export function getLocalWorkspaceName(
  tinybirdBranch: string | null,
  cwd: string
): string {
  if (tinybirdBranch) {
    return `Local_${tinybirdBranch}`;
  }

  // No branch detected - use hash of path like Python implementation
  const hash = crypto.createHash("sha256").update(cwd).digest("hex");
  return `Local_Build_${hash.substring(0, 16)}`;
}
