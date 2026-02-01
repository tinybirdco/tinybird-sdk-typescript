/**
 * Build command - generates and pushes resources to Tinybird
 */

import * as fs from "fs";
import * as path from "path";
import { loadConfig, type ResolvedConfig } from "../config.js";
import { buildFromInclude, type BuildFromIncludeResult } from "../../generator/index.js";
import { buildToTinybird, type BuildApiResult } from "../../api/build.js";
import { deployToMain } from "../../api/deploy.js";

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
  /** Path to generated client file */
  clientFilePath?: string;
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
      outputPath: config.output,
      cwd: config.cwd,
    });
  } catch (error) {
    return {
      success: false,
      error: `Build failed: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
  }

  // Write the generated client file
  const clientFilePath = path.join(config.cwd, config.output);
  const clientFileDir = path.dirname(clientFilePath);
  try {
    fs.mkdirSync(clientFileDir, { recursive: true });
    fs.writeFileSync(clientFilePath, buildResult.clientFile.content);

    // Write package.json for @tinybird/client if generating to node_modules
    if (buildResult.clientFile.packageJson) {
      fs.writeFileSync(
        buildResult.clientFile.packageJson.path,
        buildResult.clientFile.packageJson.content
      );
    }
  } catch (error) {
    return {
      success: false,
      build: buildResult,
      error: `Failed to write client file: ${(error as Error).message}`,
      durationMs: Date.now() - startTime,
    };
  }

  // If dry run, return without pushing
  if (options.dryRun) {
    return {
      success: true,
      build: buildResult,
      clientFilePath,
      durationMs: Date.now() - startTime,
    };
  }

  // Deploy to Tinybird
  // Use token override if provided (for branch tokens)
  const effectiveToken = options.tokenOverride ?? config.token;

  let deployResult: BuildApiResult;
  try {
    // Use /v1/deploy for main branch, /v1/build for feature branches
    if (options.useDeployEndpoint) {
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
    clientFilePath,
    durationMs: Date.now() - startTime,
  };
}
