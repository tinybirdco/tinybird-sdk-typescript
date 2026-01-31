/**
 * Dev command - watch mode with automatic sync
 */

import * as path from "path";
import { watch } from "chokidar";
import { loadConfig, type ResolvedConfig } from "../config.js";
import { runBuild, type BuildCommandResult } from "./build.js";

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
}

/**
 * Dev command controller
 */
export interface DevController {
  /** Stop watching and clean up */
  stop: () => Promise<void>;
  /** Trigger a manual rebuild */
  rebuild: () => Promise<BuildCommandResult>;
}

/**
 * Run the dev command
 *
 * Watches for file changes and automatically rebuilds and pushes to Tinybird.
 *
 * @param options - Dev options
 * @returns Dev controller
 */
export async function runDev(options: DevCommandOptions = {}): Promise<DevController> {
  const cwd = options.cwd ?? process.cwd();
  const debounceMs = options.debounce ?? 100;

  // Load config
  let config: ResolvedConfig;
  try {
    config = loadConfig(cwd);
  } catch (error) {
    throw error;
  }

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
      const result = await runBuild({ cwd: config.cwd });
      options.onBuildComplete?.(result);
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
  };
}
