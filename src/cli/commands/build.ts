/**
 * Build command - generates and pushes resources to Tinybird
 */

import { loadConfig, type ResolvedConfig } from "../config.js";
import { build, type BuildResult } from "../../generator/index.js";
import { buildToTinybird, type BuildApiResult } from "../../api/build.js";

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

  // Deploy to Tinybird
  let deployResult: BuildApiResult;
  try {
    deployResult = await buildToTinybird(
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
      error: `Deploy failed: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
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
