/**
 * Dev command - watch mode with automatic sync
 */

import * as path from "path";
import { watch } from "chokidar";
import { loadConfig, configExists, findConfigFile, hasValidToken, updateConfig, type ResolvedConfig } from "../config.js";
import { runBuild, type BuildCommandResult } from "./build.js";
import { getOrCreateBranch, type TinybirdBranch } from "../../api/branches.js";
import { browserLogin } from "../auth.js";
import { saveTinybirdToken } from "../env.js";
import {
  validatePipeSchemas,
  type SchemaValidationResult,
} from "../utils/schema-validation.js";

/**
 * Login result info
 */
export interface LoginInfo {
  /** Workspace name */
  workspaceName?: string;
  /** User email */
  userEmail?: string;
}

/**
 * Dev command options
 */
export interface DevCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Debounce delay in milliseconds (default: 100) */
  debounce?: number;
  /** Callback when build starts */
  onBuildStart?: () => void;
  /** Callback when build completes */
  onBuildComplete?: (result: BuildCommandResult) => void;
  /** Callback when an error occurs */
  onError?: (error: Error) => void;
  /** Callback when branch is created/detected */
  onBranchReady?: (info: BranchReadyInfo) => void;
  /** Callback when login is needed and completed */
  onLoginComplete?: (info: LoginInfo) => void;
  /** Callback when schema validation completes */
  onSchemaValidation?: (result: SchemaValidationResult) => void;
}

/**
 * Information about the branch being used
 */
export interface BranchReadyInfo {
  /** Git branch name */
  gitBranch: string | null;
  /** Whether we're on the main branch */
  isMainBranch: boolean;
  /** Tinybird branch info (null if on main) */
  tinybirdBranch?: TinybirdBranch;
  /** Whether the branch was newly created */
  wasCreated?: boolean;
}

/**
 * Dev command controller
 */
export interface DevController {
  /** Stop watching and clean up */
  stop: () => Promise<void>;
  /** Trigger a manual rebuild */
  rebuild: () => Promise<BuildCommandResult>;
  /** The configuration being used */
  config: ResolvedConfig;
  /** The effective token (branch token or main token) */
  effectiveToken: string;
  /** Branch info */
  branchInfo: BranchReadyInfo;
}

/**
 * Run the dev command
 *
 * Watches for file changes and automatically rebuilds and pushes to Tinybird.
 * Automatically manages Tinybird branches based on git branch:
 * - Main branch: uses workspace token and /v1/deploy
 * - Feature branches: creates/reuses Tinybird branch and uses /v1/build
 *
 * @param options - Dev options
 * @returns Dev controller
 */
export async function runDev(options: DevCommandOptions = {}): Promise<DevController> {
  const cwd = options.cwd ?? process.cwd();
  const debounceMs = options.debounce ?? 100;

  // Check if project is initialized
  if (!configExists(cwd)) {
    throw new Error(
      "No tinybird.json found. Run 'npx tinybird init' to initialize a project."
    );
  }

  // Check if authentication is set up, if not trigger login
  if (!hasValidToken(cwd)) {
    console.log("No authentication found. Starting login flow...\n");

    const authResult = await browserLogin();

    if (!authResult.success || !authResult.token) {
      throw new Error(
        authResult.error ?? "Login failed. Run 'npx tinybird login' to authenticate."
      );
    }

    // Find the config file (may be in parent directory)
    const configPath = findConfigFile(cwd);
    if (!configPath) {
      throw new Error("No tinybird.json found. Run 'npx tinybird init' first.");
    }

    // Save token to .env.local (in same directory as tinybird.json)
    const configDir = path.dirname(configPath);
    saveTinybirdToken(configDir, authResult.token);

    // Update baseUrl in tinybird.json if it changed
    if (authResult.baseUrl) {
      updateConfig(configPath, {
        baseUrl: authResult.baseUrl,
      });
    }

    // Set the token in the environment for this session
    process.env.TINYBIRD_TOKEN = authResult.token;

    options.onLoginComplete?.({
      workspaceName: authResult.workspaceName,
      userEmail: authResult.userEmail,
    });
  }

  // Load config (now should have valid token)
  let config: ResolvedConfig;
  try {
    config = loadConfig(cwd);
  } catch (error) {
    throw error;
  }

  // Determine effective token based on git branch
  let effectiveToken = config.token;
  let branchInfo: BranchReadyInfo = {
    gitBranch: config.gitBranch,
    isMainBranch: config.isMainBranch,
  };

  // If we're on a feature branch, get or create the Tinybird branch
  // Use tinybirdBranch (sanitized name) for Tinybird API, gitBranch for display
  if (!config.isMainBranch && config.tinybirdBranch) {
    const branchName = config.tinybirdBranch; // Sanitized name for Tinybird

    // Always fetch fresh from API to avoid stale cache issues
    const tinybirdBranch = await getOrCreateBranch(
      {
        baseUrl: config.baseUrl,
        token: config.token,
      },
      branchName
    );

    if (!tinybirdBranch.token) {
      throw new Error(
        `Branch '${branchName}' was created but no token was returned. ` +
          `This may be an API issue.`
      );
    }

    effectiveToken = tinybirdBranch.token;
    branchInfo = {
      gitBranch: config.gitBranch, // Original git branch name for display
      isMainBranch: false,
      tinybirdBranch,
      wasCreated: tinybirdBranch.wasCreated ?? false,
    };
  }

  // Notify about branch readiness
  options.onBranchReady?.(branchInfo);

  // Get the schema directory to watch
  const schemaPath = path.isAbsolute(config.schema)
    ? config.schema
    : path.resolve(config.cwd, config.schema);
  const schemaDir = path.dirname(schemaPath);

  // Debounce state
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let isBuilding = false;
  let pendingBuild = false;

  // Build function
  async function doBuild(): Promise<BuildCommandResult> {
    if (isBuilding) {
      pendingBuild = true;
      return { success: false, error: "Build already in progress", durationMs: 0 };
    }

    isBuilding = true;
    options.onBuildStart?.();

    try {
      const result = await runBuild({
        cwd: config.cwd,
        tokenOverride: effectiveToken,
        useDeployEndpoint: config.isMainBranch,
      });
      options.onBuildComplete?.(result);

      // Validate pipe schemas after successful deploy
      if (
        result.success &&
        result.build?.project &&
        result.deploy?.pipes &&
        options.onSchemaValidation
      ) {
        // Get changed pipes from deploy result
        const changedPipes = [
          ...result.deploy.pipes.created,
          ...result.deploy.pipes.changed,
        ];

        if (changedPipes.length > 0) {
          const validation = await validatePipeSchemas({
            project: result.build.project,
            pipeNames: changedPipes,
            baseUrl: config.baseUrl,
            token: effectiveToken,
          });

          options.onSchemaValidation(validation);
        }
      }

      return result;
    } catch (error) {
      const result: BuildCommandResult = {
        success: false,
        error: (error as Error).message,
        durationMs: 0,
      };
      options.onBuildComplete?.(result);
      return result;
    } finally {
      isBuilding = false;

      // If there was a pending build, trigger it
      if (pendingBuild) {
        pendingBuild = false;
        scheduleBuild();
      }
    }
  }

  // Schedule a debounced build
  function scheduleBuild(): void {
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }

    debounceTimer = setTimeout(() => {
      debounceTimer = null;
      doBuild().catch((error) => {
        options.onError?.(error as Error);
      });
    }, debounceMs);
  }

  // Set up file watcher
  const watcher = watch(schemaDir, {
    ignored: [
      /(^|[\/\\])\../, // Ignore dotfiles
      /node_modules/,
      /\.tinybird-schema-.*\.mjs$/, // Ignore temporary bundle files
    ],
    persistent: true,
    ignoreInitial: true,
  });

  // Watch for changes
  watcher.on("change", (filePath) => {
    if (filePath.endsWith(".ts") || filePath.endsWith(".js")) {
      scheduleBuild();
    }
  });

  watcher.on("add", (filePath) => {
    if (filePath.endsWith(".ts") || filePath.endsWith(".js")) {
      scheduleBuild();
    }
  });

  watcher.on("unlink", (filePath) => {
    if (filePath.endsWith(".ts") || filePath.endsWith(".js")) {
      scheduleBuild();
    }
  });

  watcher.on("error", (error: unknown) => {
    options.onError?.(error instanceof Error ? error : new Error(String(error)));
  });

  // Do initial build
  await doBuild();

  // Return controller
  return {
    stop: async () => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }
      await watcher.close();
    },
    rebuild: doBuild,
    config,
    effectiveToken,
    branchInfo,
  };
}
