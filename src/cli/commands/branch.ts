/**
 * Branch management commands
 */

import { loadConfigAsync, type ResolvedConfig } from "../config.js";
import {
  listBranches,
  getBranch,
  deleteBranch,
  type TinybirdBranch,
  BranchApiError,
} from "../../api/branches.js";
import { getWorkspace } from "../../api/workspaces.js";
import {
  getBranchToken,
  removeBranch as removeCachedBranch,
  listCachedBranches,
} from "../branch-store.js";
import { getBranchDashboardUrl } from "../../api/dashboard.js";

/**
 * Branch command options
 */
export interface BranchCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
}

/**
 * Result of listing branches
 */
export interface BranchListResult {
  /** Whether the operation was successful */
  success: boolean;
  /** List of branches */
  branches?: TinybirdBranch[];
  /** Error message if failed */
  error?: string;
}

/**
 * Result of getting branch status
 */
export interface BranchStatusResult {
  /** Whether the operation was successful */
  success: boolean;
  /** Current git branch */
  gitBranch: string | null;
  /** Sanitized Tinybird branch name */
  tinybirdBranchName: string | null;
  /** Whether we're on the main branch */
  isMainBranch: boolean;
  /** Tinybird branch info (if exists) */
  tinybirdBranch?: TinybirdBranch;
  /** Whether a cached token exists */
  hasCachedToken: boolean;
  /** Dashboard URL for the branch */
  dashboardUrl?: string;
  /** Error message if failed */
  error?: string;
}

/**
 * Result of deleting a branch
 */
export interface BranchDeleteResult {
  /** Whether the operation was successful */
  success: boolean;
  /** Error message if failed */
  error?: string;
}

/**
 * List all Tinybird branches
 *
 * @param options - Command options
 * @returns List result
 */
export async function runBranchList(
  options: BranchCommandOptions = {}
): Promise<BranchListResult> {
  const cwd = options.cwd ?? process.cwd();

  let config: ResolvedConfig;
  try {
    config = await loadConfigAsync(cwd);
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
    };
  }

  try {
    const branches = await listBranches({
      baseUrl: config.baseUrl,
      token: config.token,
    });

    return {
      success: true,
      branches,
    };
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
    };
  }
}

/**
 * Get current branch status
 *
 * @param options - Command options
 * @returns Status result
 */
export async function runBranchStatus(
  options: BranchCommandOptions = {}
): Promise<BranchStatusResult> {
  const cwd = options.cwd ?? process.cwd();

  let config: ResolvedConfig;
  try {
    config = await loadConfigAsync(cwd);
  } catch (error) {
    return {
      success: false,
      gitBranch: null,
      tinybirdBranchName: null,
      isMainBranch: false,
      hasCachedToken: false,
      error: (error as Error).message,
    };
  }

  const gitBranch = config.gitBranch;
  const tinybirdBranchName = config.tinybirdBranch; // Sanitized name
  const isMainBranch = config.isMainBranch;

  // Fetch the workspace from the API
  let workspaceId: string;
  let workspaceName: string;
  try {
    const workspace = await getWorkspace({
      baseUrl: config.baseUrl,
      token: config.token,
    });
    workspaceId = workspace.id;
    workspaceName = workspace.name;
  } catch (error) {
    return {
      success: false,
      gitBranch,
      tinybirdBranchName,
      isMainBranch,
      hasCachedToken: false,
      error: (error as Error).message,
    };
  }

  // Generate dashboard URL for the branch
  const dashboardUrl = tinybirdBranchName
    ? getBranchDashboardUrl(config.baseUrl, workspaceName, tinybirdBranchName) ?? undefined
    : undefined;

  // Check for cached token (use sanitized name)
  const cachedBranch = tinybirdBranchName ? getBranchToken(workspaceId, tinybirdBranchName) : null;
  const hasCachedToken = cachedBranch !== null;

  // If on main branch, just return status
  if (isMainBranch || !tinybirdBranchName) {
    return {
      success: true,
      gitBranch,
      tinybirdBranchName,
      isMainBranch,
      hasCachedToken,
      dashboardUrl,
    };
  }

  // Try to get the Tinybird branch info (use sanitized name)
  try {
    const tinybirdBranch = await getBranch(
      {
        baseUrl: config.baseUrl,
        token: config.token,
      },
      tinybirdBranchName
    );

    return {
      success: true,
      gitBranch,
      tinybirdBranchName,
      isMainBranch,
      tinybirdBranch,
      hasCachedToken,
      dashboardUrl,
    };
  } catch (error) {
    // If 404, branch doesn't exist yet
    if (error instanceof BranchApiError && error.status === 404) {
      return {
        success: true,
        gitBranch,
        tinybirdBranchName,
        isMainBranch,
        hasCachedToken,
        dashboardUrl,
      };
    }

    return {
      success: false,
      gitBranch,
      tinybirdBranchName,
      isMainBranch,
      hasCachedToken,
      dashboardUrl,
      error: (error as Error).message,
    };
  }
}

/**
 * Delete a Tinybird branch
 *
 * @param name - Branch name to delete
 * @param options - Command options
 * @returns Delete result
 */
export async function runBranchDelete(
  name: string,
  options: BranchCommandOptions = {}
): Promise<BranchDeleteResult> {
  const cwd = options.cwd ?? process.cwd();

  let config: ResolvedConfig;
  try {
    config = await loadConfigAsync(cwd);
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
    };
  }

  try {
    // Fetch the workspace ID from the API
    const workspace = await getWorkspace({
      baseUrl: config.baseUrl,
      token: config.token,
    });
    const workspaceId = workspace.id;

    // Delete from Tinybird API
    await deleteBranch(
      {
        baseUrl: config.baseUrl,
        token: config.token,
      },
      name
    );

    // Remove from local cache
    removeCachedBranch(workspaceId, name);

    return {
      success: true,
    };
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
    };
  }
}

/**
 * List cached branches (requires API call to get workspace ID)
 */
export async function runBranchListCached(
  options: BranchCommandOptions = {}
): Promise<{ branches: Record<string, { id: string; createdAt: string }> }> {
  const cwd = options.cwd ?? process.cwd();

  let config: ResolvedConfig;
  try {
    config = await loadConfigAsync(cwd);
  } catch {
    return { branches: {} };
  }

  // Fetch the workspace ID from the API
  let workspaceId: string;
  try {
    const workspace = await getWorkspace({
      baseUrl: config.baseUrl,
      token: config.token,
    });
    workspaceId = workspace.id;
  } catch {
    return { branches: {} };
  }

  const cached = listCachedBranches(workspaceId);

  // Return without tokens for security
  const branches: Record<string, { id: string; createdAt: string }> = {};
  for (const [name, info] of Object.entries(cached)) {
    branches[name] = {
      id: info.id,
      createdAt: info.createdAt,
    };
  }

  return { branches };
}
