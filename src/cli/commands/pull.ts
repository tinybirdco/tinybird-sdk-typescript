/**
 * Pull command - pulls resources from a Tinybird workspace and generates TypeScript files
 */

import * as fs from "fs";
import * as path from "path";
import {
  loadConfig,
  getTinybirdDir,
  getRelativeTinybirdDir,
} from "../config.js";
import { fetchAllResources, ResourceApiError } from "../../api/resources.js";
import { generateAllFiles } from "../../codegen/index.js";

/**
 * Pull command options
 */
export interface PullOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Force overwrite existing files */
  force?: boolean;
}

/**
 * Pull command result
 */
export interface PullResult {
  /** Whether the pull was successful */
  success: boolean;
  /** Files that were created */
  created: string[];
  /** Error message if failed */
  error?: string;
  /** Number of datasources pulled */
  datasourceCount?: number;
  /** Number of pipes pulled */
  pipeCount?: number;
}

/**
 * Run the pull command
 *
 * Fetches resources from the Tinybird workspace and generates TypeScript files.
 *
 * @param options - Pull options
 * @returns Pull result
 */
export async function runPull(options: PullOptions = {}): Promise<PullResult> {
  const cwd = options.cwd ?? process.cwd();
  const force = options.force ?? false;

  // Load config
  let config;
  try {
    config = loadConfig(cwd);
  } catch (error) {
    return {
      success: false,
      created: [],
      error: `${(error as Error).message}`,
    };
  }

  // Token is already resolved by loadConfig
  const token = config.token;
  if (!token) {
    return {
      success: false,
      created: [],
      error:
        "No token configured. Set TINYBIRD_TOKEN environment variable or run 'tinybird login'.",
    };
  }

  // Get base URL (already resolved by loadConfig)
  const baseUrl = config.baseUrl;

  // Get tinybird directory paths
  const tinybirdDir = getTinybirdDir(cwd);
  const relativeTinybirdDir = getRelativeTinybirdDir(cwd);
  const datasourcesPath = path.join(tinybirdDir, "datasources.ts");
  const pipesPath = path.join(tinybirdDir, "pipes.ts");
  const clientPath = path.join(tinybirdDir, "client.ts");

  // Check if files already exist
  const filesExist =
    fs.existsSync(datasourcesPath) ||
    fs.existsSync(pipesPath) ||
    fs.existsSync(clientPath);

  if (filesExist && !force) {
    return {
      success: false,
      created: [],
      error: `TypeScript files already exist in ${relativeTinybirdDir}/\nUse --force to overwrite existing files.`,
    };
  }

  try {
    // Fetch all resources from workspace
    console.log("Fetching resources from workspace...\n");

    const { datasources, pipes } = await fetchAllResources({ baseUrl, token });

    // If no resources found
    if (datasources.length === 0 && pipes.length === 0) {
      return {
        success: true,
        created: [],
        datasourceCount: 0,
        pipeCount: 0,
        error: "No resources found in workspace.",
      };
    }

    console.log(
      `Found ${datasources.length} datasource(s) and ${pipes.length} pipe(s). Pulling...\n`
    );

    // Generate TypeScript files
    const generated = generateAllFiles(datasources, pipes);
    const created: string[] = [];

    // Ensure tinybird directory exists
    fs.mkdirSync(tinybirdDir, { recursive: true });

    // Write datasources.ts
    fs.writeFileSync(datasourcesPath, generated.datasourcesContent);
    created.push(`${relativeTinybirdDir}/datasources.ts`);

    // Write pipes.ts
    fs.writeFileSync(pipesPath, generated.pipesContent);
    created.push(`${relativeTinybirdDir}/pipes.ts`);

    // Write client.ts
    fs.writeFileSync(clientPath, generated.clientContent);
    created.push(`${relativeTinybirdDir}/client.ts`);

    return {
      success: true,
      created,
      datasourceCount: generated.datasourceCount,
      pipeCount: generated.pipeCount,
    };
  } catch (error) {
    if (error instanceof ResourceApiError) {
      return {
        success: false,
        created: [],
        error: `Failed to fetch resources: ${error.message}`,
      };
    }

    return {
      success: false,
      created: [],
      error: `Failed to pull resources: ${(error as Error).message}`,
    };
  }
}
