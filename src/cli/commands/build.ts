/**
 * Build command - generates and pushes resources to Tinybird
 */

import { loadConfig, type ResolvedConfig } from "../config.js";
import { build, type BuildResult } from "../../generator/index.js";
import { pushToTinybird, type PushResult } from "../../api/push.js";

/**
 * Build command options
 */
export interface BuildCommandOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Skip pushing to API (just generate) */
  dryRun?: boolean;
}

/**
 * Build command result
 */
export interface BuildCommandResult {
  /** Whether the build was successful */
  success: boolean;
  /** Build result with generated resources */
  build?: BuildResult;
  /** Push result (if not dry run) */
  push?: PushResult;
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

  // Build resources
  let buildResult: BuildResult;
  try {
    buildResult = await build({
      schemaPath: config.schema,
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

  // Push to Tinybird
  let pushResult: PushResult;
  try {
    pushResult = await pushToTinybird(
      {
        baseUrl: config.baseUrl,
        token: config.token,
      },
      buildResult.resources
    );
  } catch (error) {
    return {
      success: false,
      build: buildResult,
      error: `Push failed: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
  }

  if (!pushResult.success) {
    return {
      success: false,
      build: buildResult,
      push: pushResult,
      error: pushResult.error,
      durationMs: Date.now() - startTime,
    };
  }

  return {
    success: true,
    build: buildResult,
    push: pushResult,
    durationMs: Date.now() - startTime,
  };
}
