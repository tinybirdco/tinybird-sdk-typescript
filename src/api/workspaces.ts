/**
 * Tinybird Workspace API client
 */

import { tinybirdFetch } from "./fetcher.js";

/**
 * Workspace information from Tinybird API
 */
export interface TinybirdWorkspace {
  /** Workspace ID (UUID) */
  id: string;
  /** Workspace name */
  name: string;
  /** User ID of the workspace owner */
  user_id: string;
  /** Email of the workspace owner */
  user_email: string;
  /** Workspace scope */
  scope: string;
  /** Main branch (null for main workspace) */
  main: string | null;
}

/**
 * API configuration for workspace operations
 */
export interface WorkspaceApiConfig {
  /** Tinybird API base URL */
  baseUrl: string;
  /** Workspace token */
  token: string;
}

/**
 * Error thrown by workspace API operations
 */
export class WorkspaceApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "WorkspaceApiError";
  }
}

/**
 * Get workspace information
 * GET /v1/workspace
 *
 * @param config - API configuration
 * @returns Workspace information
 */
export async function getWorkspace(
  config: WorkspaceApiConfig
): Promise<TinybirdWorkspace> {
  const url = new URL("/v1/workspace", config.baseUrl);

  const response = await tinybirdFetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new WorkspaceApiError(
      `Failed to get workspace: ${response.status} ${response.statusText}`,
      response.status,
      body
    );
  }

  const data = (await response.json()) as TinybirdWorkspace;
  return data;
}
