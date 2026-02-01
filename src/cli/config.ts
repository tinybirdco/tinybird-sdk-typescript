/**
 * Configuration loader for tinybird.json
 */

import * as fs from "fs";
import * as path from "path";
import { getCurrentGitBranch, isMainBranch, getTinybirdBranchName } from "./git.js";

/**
 * Tinybird configuration file structure
 */
export interface TinybirdConfig {
  /** Path to the TypeScript schema entry point */
  schema: string;
  /** API token (supports ${ENV_VAR} interpolation) */
  token: string;
  /** Tinybird API base URL (optional, defaults to EU region) */
  baseUrl?: string;
}

/**
 * Resolved configuration with all values expanded
 */
export interface ResolvedConfig {
  /** Path to the TypeScript schema entry point */
  schema: string;
  /** Resolved API token (workspace main token) */
  token: string;
  /** Tinybird API base URL */
  baseUrl: string;
  /** Path to the config file */
  configPath: string;
  /** Working directory */
  cwd: string;
  /** Current git branch (null if not in git repo or detached HEAD) */
  gitBranch: string | null;
  /** Sanitized branch name for Tinybird (symbols replaced with underscores) */
  tinybirdBranch: string | null;
  /** Whether we're on the main/master branch */
  isMainBranch: boolean;
}

/**
 * Default base URL (EU region)
 */
const DEFAULT_BASE_URL = "https://api.tinybird.co";

/**
 * Config file name
 */
const CONFIG_FILE = "tinybird.json";

/**
 * Tinybird schema file name
 */
const TINYBIRD_SCHEMA_FILE = "tinybird.ts";

/**
 * Detect if project has a src folder
 */
export function hasSrcFolder(cwd: string): boolean {
  const srcPath = path.join(cwd, "src");
  return fs.existsSync(srcPath) && fs.statSync(srcPath).isDirectory();
}

/**
 * Get the lib directory path based on project structure
 * Returns 'src/lib' if project has src folder, otherwise 'lib'
 */
export function getLibDir(cwd: string): string {
  return hasSrcFolder(cwd) ? path.join(cwd, "src", "lib") : path.join(cwd, "lib");
}

/**
 * Get the relative lib directory path based on project structure
 */
export function getRelativeLibDir(cwd: string): string {
  return hasSrcFolder(cwd) ? "src/lib" : "lib";
}

/**
 * Get the tinybird.ts schema path based on project structure
 */
export function getTinybirdSchemaPath(cwd: string): string {
  return path.join(getLibDir(cwd), TINYBIRD_SCHEMA_FILE);
}

/**
 * Get the relative schema path based on project structure
 */
export function getRelativeSchemaPath(cwd: string): string {
  return `${getRelativeLibDir(cwd)}/${TINYBIRD_SCHEMA_FILE}`;
}

/**
 * Interpolate environment variables in a string
 *
 * Supports ${VAR_NAME} syntax
 */
function interpolateEnvVars(value: string): string {
  return value.replace(/\$\{([^}]+)\}/g, (_, envVar) => {
    const envValue = process.env[envVar];
    if (envValue === undefined) {
      throw new Error(`Environment variable ${envVar} is not set`);
    }
    return envValue;
  });
}

/**
 * Find the config file by walking up the directory tree
 *
 * @param startDir - Directory to start searching from
 * @returns Path to the config file, or null if not found
 */
export function findConfigFile(startDir: string): string | null {
  let currentDir = startDir;

  while (true) {
    const configPath = path.join(currentDir, CONFIG_FILE);
    if (fs.existsSync(configPath)) {
      return configPath;
    }

    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      // Reached root
      return null;
    }
    currentDir = parentDir;
  }
}

/**
 * Load and resolve the tinybird.json configuration
 *
 * @param cwd - Working directory to start searching from (defaults to process.cwd())
 * @returns Resolved configuration
 *
 * @example
 * ```ts
 * const config = loadConfig();
 * console.log(config.schema); // 'lib/tinybird.ts' or 'src/lib/tinybird.ts'
 * console.log(config.token);  // 'p.xxx' (resolved from ${TINYBIRD_TOKEN})
 * ```
 */
export function loadConfig(cwd: string = process.cwd()): ResolvedConfig {
  const configPath = findConfigFile(cwd);

  if (!configPath) {
    throw new Error(
      `Could not find ${CONFIG_FILE}. Run 'npx tinybird init' to create one.`
    );
  }

  let rawContent: string;
  try {
    rawContent = fs.readFileSync(configPath, "utf-8");
  } catch (error) {
    throw new Error(`Failed to read ${configPath}: ${(error as Error).message}`);
  }

  let config: TinybirdConfig;
  try {
    config = JSON.parse(rawContent) as TinybirdConfig;
  } catch (error) {
    throw new Error(`Failed to parse ${configPath}: ${(error as Error).message}`);
  }

  // Validate required fields
  if (!config.schema) {
    throw new Error(`Missing 'schema' field in ${configPath}`);
  }

  if (!config.token) {
    throw new Error(`Missing 'token' field in ${configPath}`);
  }

  // Resolve token (may contain env vars)
  let resolvedToken: string;
  try {
    resolvedToken = interpolateEnvVars(config.token);
  } catch (error) {
    throw new Error(
      `Failed to resolve token in ${configPath}: ${(error as Error).message}`
    );
  }

  // Resolve base URL
  let resolvedBaseUrl = DEFAULT_BASE_URL;
  if (config.baseUrl) {
    try {
      resolvedBaseUrl = interpolateEnvVars(config.baseUrl);
    } catch (error) {
      throw new Error(
        `Failed to resolve baseUrl in ${configPath}: ${(error as Error).message}`
      );
    }
  }

  // Get the directory containing the config file
  const configDir = path.dirname(configPath);

  // Detect git branch
  const gitBranch = getCurrentGitBranch();
  const tinybirdBranch = getTinybirdBranchName();

  return {
    schema: config.schema,
    token: resolvedToken,
    baseUrl: resolvedBaseUrl,
    configPath,
    cwd: configDir,
    gitBranch,
    tinybirdBranch,
    isMainBranch: isMainBranch(),
  };
}

/**
 * Check if a config file exists in the given directory
 */
export function configExists(cwd: string = process.cwd()): boolean {
  return findConfigFile(cwd) !== null;
}

/**
 * Get the expected config file path for a directory
 */
export function getConfigPath(cwd: string = process.cwd()): string {
  return path.join(cwd, CONFIG_FILE);
}

/**
 * Update specific fields in tinybird.json
 *
 * Throws an error if the config file doesn't exist to prevent creating
 * partial config files that would break loadConfig.
 *
 * @param configPath - Path to the config file
 * @param updates - Fields to update
 * @throws Error if config file doesn't exist
 */
export function updateConfig(
  configPath: string,
  updates: Partial<TinybirdConfig>
): void {
  if (!fs.existsSync(configPath)) {
    throw new Error(`Config not found at ${configPath}`);
  }

  const content = fs.readFileSync(configPath, "utf-8");
  const config = JSON.parse(content) as TinybirdConfig;

  // Merge updates
  const updated = { ...config, ...updates };

  fs.writeFileSync(configPath, JSON.stringify(updated, null, 2) + "\n");
}

/**
 * Check if a valid token is configured (either in file or via env var)
 *
 * @param cwd - Working directory to search from
 * @returns true if a valid token exists
 */
export function hasValidToken(cwd: string = process.cwd()): boolean {
  try {
    const configPath = findConfigFile(cwd);
    if (!configPath) {
      return false;
    }

    const content = fs.readFileSync(configPath, "utf-8");
    const config = JSON.parse(content) as TinybirdConfig;

    if (!config.token) {
      return false;
    }

    // Check if token is a placeholder or env var reference
    if (config.token.includes("${")) {
      // Try to resolve the env var
      try {
        const resolved = interpolateEnvVars(config.token);
        return Boolean(resolved);
      } catch {
        return false;
      }
    }

    // Token is a literal value
    return Boolean(config.token);
  } catch {
    return false;
  }
}
