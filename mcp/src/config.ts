/**
 * Configuration loader for tinybird.json and .tinyb files
 * Minimal version for MCP server - only needs token and baseUrl
 */

import * as fs from "fs";
import * as path from "path";

/**
 * Tinybird configuration file structure (tinybird.json)
 */
export interface TinybirdJsonConfig {
  /** API token (supports ${ENV_VAR} interpolation) */
  token: string;
  /** Tinybird API base URL (optional, defaults to EU region) */
  baseUrl?: string;
}

/**
 * .tinyb file structure (created by tb login)
 */
export interface TinybConfig {
  /** Tinybird API host URL */
  host?: string;
  /** Authentication token */
  token?: string;
  /** Map of hosts to tokens */
  tokens?: Record<string, string>;
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
 * Config file names in order of priority
 */
const CONFIG_FILES = ["tinybird.json", ".tinyb"];

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
 * Find a config file by walking up the directory tree
 * Checks for tinybird.json first, then .tinyb
 */
export function findConfigFile(startDir: string): { path: string; type: "tinybird.json" | ".tinyb" } | null {
  let currentDir = startDir;

  while (true) {
    // Check each config file type in priority order
    for (const configFile of CONFIG_FILES) {
      const configPath = path.join(currentDir, configFile);
      if (fs.existsSync(configPath)) {
        return { path: configPath, type: configFile as "tinybird.json" | ".tinyb" };
      }
    }

    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      return null;
    }
    currentDir = parentDir;
  }
}

/**
 * Load config from tinybird.json format
 */
function loadTinybirdJsonConfig(configPath: string): ResolvedConfig {
  let rawContent: string;
  try {
    rawContent = fs.readFileSync(configPath, "utf-8");
  } catch (error) {
    throw new Error(`Failed to read ${configPath}: ${(error as Error).message}`);
  }

  let config: TinybirdJsonConfig;
  try {
    config = JSON.parse(rawContent) as TinybirdJsonConfig;
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

/**
 * Load config from .tinyb format (created by tb login)
 */
function loadTinybConfigFile(configPath: string): ResolvedConfig {
  let rawContent: string;
  try {
    rawContent = fs.readFileSync(configPath, "utf-8");
  } catch (error) {
    throw new Error(`Failed to read ${configPath}: ${(error as Error).message}`);
  }

  let config: TinybConfig;
  try {
    config = JSON.parse(rawContent) as TinybConfig;
  } catch (error) {
    throw new Error(`Failed to parse ${configPath}: ${(error as Error).message}`);
  }

  // Get token - prefer direct token, fallback to tokens map with host
  let token = config.token;
  if (!token && config.tokens && config.host) {
    token = config.tokens[config.host];
  }

  if (!token) {
    throw new Error(`Missing 'token' field in ${configPath}. Run 'tb login' to authenticate.`);
  }

  // Get base URL from host field
  let baseUrl = DEFAULT_BASE_URL;
  if (config.host) {
    baseUrl = config.host;
  }

  return {
    token,
    baseUrl,
  };
}

/**
 * Load and resolve the configuration from tinybird.json or .tinyb
 */
export function loadConfig(cwd: string = process.cwd()): ResolvedConfig {
  const configResult = findConfigFile(cwd);

  if (!configResult) {
    throw new Error(
      `Could not find tinybird.json or .tinyb. Run 'npx @tinybirdco/sdk init' or 'tb login' to create one.`
    );
  }

  if (configResult.type === "tinybird.json") {
    return loadTinybirdJsonConfig(configResult.path);
  } else {
    return loadTinybConfigFile(configResult.path);
  }
}
