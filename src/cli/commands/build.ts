/**
 * Build command - generates and pushes resources to Tinybird
 */

import { loadConfig, LOCAL_BASE_URL, type ResolvedConfig, type DevMode } from "../config.js";
import { buildFromInclude, type BuildFromIncludeResult } from "../../generator/index.js";
import { buildToTinybird, type BuildApiResult } from "../../api/build.js";
import { deployToMain } from "../../api/deploy.js";
import { getOrCreateBranch } from "../../api/branches.js";
import {
  getLocalTokens,
  getOrCreateLocalWorkspace,
  getLocalWorkspaceName,
  LocalNotRunningError,
} from "../../api/local.js";

/**
 * Build command options
 */
export interface BuildCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Skip pushing to API (just generate) */
  dryRun?: boolean;
  /** Override the token from config (used for branch tokens) */
  tokenOverride?: string;
  /** Use /v1/deploy instead of /v1/build (for main branch) */
  useDeployEndpoint?: boolean;
  /** Override the devMode from config */
  devModeOverride?: DevMode;
}

/**
 * Build command result
 */
export interface BuildCommandResult {
  /** Whether the build was successful */
  success: boolean;
  /** Build result with generated resources */
  build?: BuildFromIncludeResult;
  /** Build API result (if not dry run) */
  deploy?: BuildApiResult;
  /** Error message if failed */
  error?: string;
  /** Duration in milliseconds */
  durationMs: number;
}

/**
 * Run the build command
 *
 * Loads the schema, generates resources, and pushes to Tinybird API.
 *
 * @param options - Build options
 * @returns Build command result
 */
export async function runBuild(options: BuildCommandOptions = {}): Promise<BuildCommandResult> {
  const startTime = Date.now();
  const cwd = options.cwd ?? process.cwd();

  // Load config
  let config: ResolvedConfig;
  try {
    config = loadConfig(cwd);
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      durationMs: Date.now() - startTime,
    };
  }

  // Build resources from include paths
  let buildResult: BuildFromIncludeResult;
  try {
    buildResult = await buildFromInclude({
      includePaths: config.include,
      cwd: config.cwd,
    });
  } catch (error) {
    return {
      success: false,
      error: `Build failed: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
  }

  // If dry run, return without pushing
  if (options.dryRun) {
    return {
      success: true,
      build: buildResult,
      durationMs: Date.now() - startTime,
    };
  }

  // Determine devMode
  const devMode = options.devModeOverride ?? config.devMode;
  const debug = !!process.env.TINYBIRD_DEBUG;

  if (debug) {
    console.log(`[debug] devMode: ${devMode}`);
  }

  let deployResult: BuildApiResult;

  // Handle local mode
  if (devMode === "local") {
    try {
      // Get tokens from local container
      if (debug) {
        console.log(`[debug] Getting local tokens from ${LOCAL_BASE_URL}/tokens`);
      }

      const localTokens = await getLocalTokens();

      // Get or create workspace based on branch name
      const workspaceName = getLocalWorkspaceName(config.tinybirdBranch, config.cwd);
      if (debug) {
        console.log(`[debug] Using local workspace: ${workspaceName}`);
      }

      const { workspace, wasCreated } = await getOrCreateLocalWorkspace(localTokens, workspaceName);
      if (debug) {
        console.log(`[debug] Workspace ${wasCreated ? "created" : "found"}: ${workspace.name}`);
      }

      // Always use /v1/build for local (no deploy endpoint)
      deployResult = await buildToTinybird(
        {
          baseUrl: LOCAL_BASE_URL,
          token: workspace.token,
        },
        buildResult.resources
      );
    } catch (error) {
      if (error instanceof LocalNotRunningError) {
        return {
          success: false,
          build: buildResult,
          error: error.message,
          durationMs: Date.now() - startTime,
        };
      }
      return {
        success: false,
        build: buildResult,
        error: `Local build failed: ${(error as Error).message}`,
        durationMs: Date.now() - startTime,
      };
    }
  } else {
    // Branch mode (default) - existing logic
    // Deploy to Tinybird
    // Determine token and endpoint based on git branch
    let effectiveToken = options.tokenOverride ?? config.token;
    // Use deploy endpoint if on main branch OR if no branch can be detected
    let useDeployEndpoint = options.useDeployEndpoint ?? (config.isMainBranch || !config.tinybirdBranch);

    if (debug) {
      console.log(`[debug] isMainBranch: ${config.isMainBranch}`);
      console.log(`[debug] tinybirdBranch: ${config.tinybirdBranch}`);
      console.log(`[debug] tokenOverride: ${!!options.tokenOverride}`);
    }

    // For feature branches, get or create the Tinybird branch and use its token
    if (!config.isMainBranch && config.tinybirdBranch && !options.tokenOverride) {
      if (debug) {
        console.log(`[debug] Getting/creating Tinybird branch: ${config.tinybirdBranch}`);
      }
      try {
        const tinybirdBranch = await getOrCreateBranch(
          {
            baseUrl: config.baseUrl,
            token: config.token,
          },
          config.tinybirdBranch
        );

        if (!tinybirdBranch.token) {
          return {
            success: false,
            build: buildResult,
            error: `Branch '${config.tinybirdBranch}' was created but no token was returned.`,
            durationMs: Date.now() - startTime,
          };
        }

        effectiveToken = tinybirdBranch.token;
        useDeployEndpoint = false; // Always use /v1/build for branches
        if (debug) {
          console.log(`[debug] Using branch token for branch: ${config.tinybirdBranch}`);
        }
      } catch (error) {
        return {
          success: false,
          build: buildResult,
          error: `Failed to get/create branch: ${(error as Error).message}`,
          durationMs: Date.now() - startTime,
        };
      }
    }

    try {
      // Use /v1/deploy for main branch, /v1/build for feature branches
      if (useDeployEndpoint) {
        deployResult = await deployToMain(
          {
            baseUrl: config.baseUrl,
            token: effectiveToken,
          },
          buildResult.resources
        );
      } else {
        deployResult = await buildToTinybird(
          {
            baseUrl: config.baseUrl,
            token: effectiveToken,
          },
          buildResult.resources
        );
      }
    } catch (error) {
      return {
        success: false,
        build: buildResult,
        error: `Deploy failed: ${(error as Error).message}`,
        durationMs: Date.now() - startTime,
      };
    }
  }

  if (!deployResult.success) {
    return {
      success: false,
      build: buildResult,
      deploy: deployResult,
      error: deployResult.error,
      durationMs: Date.now() - startTime,
    };
  }

  return {
    success: true,
    build: buildResult,
    deploy: deployResult,
    durationMs: Date.now() - startTime,
  };
}
