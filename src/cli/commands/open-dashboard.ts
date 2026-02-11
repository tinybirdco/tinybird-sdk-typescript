/**
 * Open Dashboard command - opens the Tinybird dashboard in the default browser
 */

import { loadConfig, type ResolvedConfig } from "../config.js";
import { getWorkspace } from "../../api/workspaces.js";
import { getBranch } from "../../api/branches.js";
import {
  getDashboardUrl,
  getBranchDashboardUrl,
  getLocalDashboardUrl,
} from "../../api/dashboard.js";
import {
  isLocalRunning,
  getLocalTokens,
  getOrCreateLocalWorkspace,
  getLocalWorkspaceName,
} from "../../api/local.js";
import { openBrowser } from "../auth.js";

/**
 * Environment options for opening dashboard
 */
export type Environment = "cloud" | "local" | "branch";

/**
 * Open dashboard command options
 */
export interface OpenDashboardCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Which environment to open: "cloud", "local", or "branch" */
  environment?: Environment;
}

/**
 * Result of the open dashboard command
 */
export interface OpenDashboardCommandResult {
  /** Whether the operation was successful */
  success: boolean;
  /** The URL that was opened */
  url?: string;
  /** Which environment was opened */
  environment?: Environment;
  /** Whether the browser was opened */
  browserOpened?: boolean;
  /** Error message if failed */
  error?: string;
}

/**
 * Run the open dashboard command
 *
 * @param options - Command options
 * @returns Result with URL and status
 */
export async function runOpenDashboard(
  options: OpenDashboardCommandOptions = {}
): Promise<OpenDashboardCommandResult> {
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

  // Determine environment: use option if provided, otherwise use devMode from config
  // When devMode is "branch" but on main branch, default to "cloud"
  let environment: Environment;
  if (options.environment) {
    environment = options.environment;
  } else if (config.devMode === "local") {
    environment = "local";
  } else if (config.devMode === "branch" && !config.isMainBranch && config.tinybirdBranch) {
    environment = "branch";
  } else {
    environment = "cloud";
  }

  // Get workspace info (needed for all dashboard URLs)
  let workspace;
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

  let url: string | null = null;

  if (environment === "local") {
    // Open local dashboard
    const localRunning = await isLocalRunning();
    if (!localRunning) {
      return {
        success: false,
        error:
          "Tinybird local is not running. Start with: docker run -d -p 7181:7181 tinybirdco/tinybird-local",
      };
    }

    try {
      const tokens = await getLocalTokens();
      // Determine workspace name: use authenticated workspace name on main branch,
      // otherwise use branch name (for trunk-based development support)
      let workspaceName: string;
      if (config.isMainBranch || !config.tinybirdBranch) {
        workspaceName = workspace.name;
      } else {
        workspaceName = getLocalWorkspaceName(config.tinybirdBranch, config.cwd);
      }
      const { workspace: localWorkspace } = await getOrCreateLocalWorkspace(
        tokens,
        workspaceName
      );
      url = getLocalDashboardUrl(localWorkspace.name);
    } catch (error) {
      return {
        success: false,
        error: `Failed to get local workspace: ${(error as Error).message}`,
      };
    }
  } else if (environment === "branch") {
    // Open branch dashboard
    if (config.isMainBranch || !config.tinybirdBranch) {
      return {
        success: false,
        error: "Cannot open branch dashboard: not on a feature branch.",
      };
    }

    try {
      const branch = await getBranch(
        { baseUrl: config.baseUrl, token: config.token },
        config.tinybirdBranch
      );
      url = getBranchDashboardUrl(config.baseUrl, workspace.name, branch.name);
    } catch (error) {
      return {
        success: false,
        error: `Branch '${config.tinybirdBranch}' does not exist. Run 'tinybird build' to create it.`,
      };
    }
  } else {
    // Open cloud (main workspace) dashboard
    url = getDashboardUrl(config.baseUrl, workspace.name);
  }

  if (!url) {
    return {
      success: false,
      error: "Could not generate dashboard URL for this configuration.",
    };
  }

  // Open the browser
  const browserOpened = await openBrowser(url);

  return {
    success: true,
    url,
    environment,
    browserOpened,
  };
}
