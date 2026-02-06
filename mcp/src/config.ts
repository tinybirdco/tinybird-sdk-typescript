/**
 * Configuration loader for tinybird.json
 * Minimal version for MCP server - only needs token and baseUrl
 */

import * as fs from "fs";
import * as path from "path";

/**
 * Tinybird configuration file structure
 */
export interface TinybirdConfig {
  /** API token (supports ${ENV_VAR} interpolation) */
  token: string;
  /** Tinybird API base URL (optional, defaults to EU region) */
  baseUrl?: string;
}

/**
 * Resolved configuration with all values expanded
 */
export interface ResolvedConfig {
  /** Resolved API token */
  token: string;
  /** Tinybird API base URL */
  baseUrl: string;
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
 * Interpolate environment variables in a string
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
      return null;
    }
    currentDir = parentDir;
  }
}

/**
 * Load and resolve the tinybird.json configuration
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

  if (!config.token) {
    throw new Error(`Missing 'token' field in ${configPath}`);
  }

  // Resolve token
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

  return {
    token: resolvedToken,
    baseUrl: resolvedBaseUrl,
  };
}
