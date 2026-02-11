/**
 * Info command - shows information about the current project and workspace
 */

import { loadConfig, LOCAL_BASE_URL, type ResolvedConfig } from "../config.js";
import { getWorkspace, type TinybirdWorkspace } from "../../api/workspaces.js";
import { listBranches, getBranch, type TinybirdBranch } from "../../api/branches.js";
import { getDashboardUrl, getBranchDashboardUrl, getLocalDashboardUrl } from "../../api/dashboard.js";
import {
  isLocalRunning,
  getLocalTokens,
  getOrCreateLocalWorkspace,
  getLocalWorkspaceName,
} from "../../api/local.js";

/**
 * Info command options
 */
export interface InfoCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Output as JSON */
  json?: boolean;
}

/**
 * Cloud/workspace information
 */
export interface CloudInfo {
  /** Workspace name */
  workspaceName: string;
  /** Workspace ID */
  workspaceId: string;
  /** User email */
  userEmail: string;
  /** API host URL */
  apiHost: string;
  /** Dashboard URL */
  dashboardUrl?: string;
  /** Token */
  token: string;
}

/**
 * Project configuration information
 */
export interface ProjectInfo {
  /** Current working directory */
  cwd: string;
  /** Path to tinybird.json */
  configPath: string;
  /** Development mode */
  devMode: string;
  /** Git branch */
  gitBranch: string | null;
  /** Tinybird branch (sanitized) */
  tinybirdBranch: string | null;
  /** Whether on main branch */
  isMainBranch: boolean;
}

/**
 * Current branch information (when working on a branch)
 */
export interface BranchInfo {
  /** Branch name */
  name: string;
  /** Branch ID */
  id: string;
  /** Branch token */
  token: string;
  /** Dashboard URL */
  dashboardUrl?: string;
}

/**
 * Local Tinybird workspace information
 */
export interface LocalInfo {
  /** Whether local Tinybird is running */
  running: boolean;
  /** Workspace name */
  workspaceName?: string;
  /** Workspace ID */
  workspaceId?: string;
  /** API host URL */
  apiHost: string;
  /** Dashboard URL */
  dashboardUrl?: string;
  /** Token */
  token?: string;
}

/**
 * Result of the info command
 */
export interface InfoCommandResult {
  /** Whether the operation was successful */
  success: boolean;
  /** Cloud/workspace info */
  cloud?: CloudInfo;
  /** Local workspace info (when devMode is local) */
  local?: LocalInfo;
  /** Project info */
  project?: ProjectInfo;
  /** Current branch info (if on a branch) */
  branch?: BranchInfo;
  /** List of all branches */
  branches?: TinybirdBranch[];
  /** Error message if failed */
  error?: string;
}

/**
 * Run the info command
 *
 * @param options - Command options
 * @returns Info result
 */
export async function runInfo(
  options: InfoCommandOptions = {}
): Promise<InfoCommandResult> {
  const cwd = options.cwd ?? process.cwd();

  // Load config
  let config: ResolvedConfig;
  try {
    config = loadConfig(cwd);
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
    };
  }

  // Build project info first (always available)
  const projectInfo: ProjectInfo = {
    cwd: config.cwd,
    configPath: config.configPath,
    devMode: config.devMode,
    gitBranch: config.gitBranch,
    tinybirdBranch: config.tinybirdBranch,
    isMainBranch: config.isMainBranch,
  };

  // Get local info if in local mode
  let localInfo: LocalInfo | undefined;
  if (config.devMode === "local") {
    const localRunning = await isLocalRunning();
    localInfo = {
      running: localRunning,
      apiHost: LOCAL_BASE_URL,
    };

    if (localRunning) {
      try {
        const tokens = await getLocalTokens();
        const workspaceName = getLocalWorkspaceName(config.tinybirdBranch, config.cwd);
        const { workspace } = await getOrCreateLocalWorkspace(tokens, workspaceName);
        localInfo = {
          running: true,
          workspaceName: workspace.name,
          workspaceId: workspace.id,
          apiHost: LOCAL_BASE_URL,
          dashboardUrl: getLocalDashboardUrl(workspace.name),
          token: workspace.token,
        };
      } catch {
        // Local is running but couldn't get workspace info
      }
    }
  }

  // Always get cloud/workspace info
  let workspace: TinybirdWorkspace;
  try {
    workspace = await getWorkspace({
      baseUrl: config.baseUrl,
      token: config.token,
    });
  } catch (error) {
    return {
      success: false,
      error: `Failed to get workspace info: ${(error as Error).message}`,
    };
  }

  // Build cloud info
  const cloudInfo: CloudInfo = {
    workspaceName: workspace.name,
    workspaceId: workspace.id,
    userEmail: workspace.user_email,
    apiHost: config.baseUrl,
    dashboardUrl: getDashboardUrl(config.baseUrl, workspace.name) ?? undefined,
    token: config.token,
  };

  // Get current branch info only if we're in branch mode (not local mode)
  let branchInfo: BranchInfo | undefined;
  let branches: TinybirdBranch[] = [];

  if (config.devMode === "branch") {
    // Get current branch info if we're on a branch (not main)
    if (!config.isMainBranch && config.tinybirdBranch) {
      try {
        const branch = await getBranch(
          {
            baseUrl: config.baseUrl,
            token: config.token,
          },
          config.tinybirdBranch
        );
        const dashboardUrl = getBranchDashboardUrl(config.baseUrl, workspace.name, branch.name) ?? undefined;
        branchInfo = {
          name: branch.name,
          id: branch.id,
          token: branch.token ?? "",
          dashboardUrl,
        };
      } catch {
        // Branch might not exist yet, that's ok
      }
    }

    // Get all branches
    try {
      branches = await listBranches({
        baseUrl: config.baseUrl,
        token: config.token,
      });
    } catch {
      // Branches are optional, don't fail if we can't fetch them
    }
  }

  return {
    success: true,
    cloud: cloudInfo,
    local: localInfo,
    project: projectInfo,
    branch: branchInfo,
    branches,
  };
}
