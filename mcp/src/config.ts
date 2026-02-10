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
 * Environment type for selecting which Tinybird instance to use
 * - "cloud": Main workspace (default)
 * - "local": Local container at localhost:7181
 * - string: Branch name - fetches branch token from API
 */
export type Environment = "cloud" | "local" | string;

/**
 * Default base URL (EU region)
 */
const DEFAULT_BASE_URL = "https://api.tinybird.co";

/**
 * Local container base URL
 */
const LOCAL_BASE_URL = "http://localhost:7181";

/**
 * Config file names in order of priority
 * Supports the new tinybird.config.* format and legacy tinybird.json
 */
const CONFIG_FILES = [
  "tinybird.config.ts",
  "tinybird.config.js",
  "tinybird.config.json",
  "tinybird.json",
  ".tinyb",
];

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

type ConfigFileType = (typeof CONFIG_FILES)[number];

/**
 * Find a config file by walking up the directory tree
 * Checks for config files in priority order
 */
export function findConfigFile(startDir: string): { path: string; type: ConfigFileType } | null {
  let currentDir = startDir;

  while (true) {
    // Check each config file type in priority order
    for (const configFile of CONFIG_FILES) {
      const configPath = path.join(currentDir, configFile);
      if (fs.existsSync(configPath)) {
        return { path: configPath, type: configFile as ConfigFileType };
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
 * Load a JS/TS config file using dynamic import
 */
async function loadJsConfig(configPath: string): Promise<ResolvedConfig> {
  try {
    // Convert path to file URL for proper handling on all platforms
    const { pathToFileURL } = await import("url");
    const fileUrl = pathToFileURL(configPath).href;
    // Dynamic import - Node.js 22+ supports TypeScript with --experimental-strip-types
    const module = await import(fileUrl);

    // Support both default export and named 'config' export
    const config = module.default ?? module.config;

    if (!config) {
      throw new Error(
        `Config file must export a default config object or named 'config' export`
      );
    }

    // If it's a function, call it to get the config
    const resolvedConfig = typeof config === "function" ? await config() : config;

    if (!resolvedConfig.token) {
      throw new Error(`Missing 'token' field in ${configPath}`);
    }

    // Resolve token
    let resolvedToken: string;
    try {
      resolvedToken = interpolateEnvVars(resolvedConfig.token);
    } catch (error) {
      throw new Error(
        `Failed to resolve token in ${configPath}: ${(error as Error).message}`
      );
    }

    // Resolve base URL
    let resolvedBaseUrl = DEFAULT_BASE_URL;
    if (resolvedConfig.baseUrl) {
      try {
        resolvedBaseUrl = interpolateEnvVars(resolvedConfig.baseUrl);
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
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ERR_UNKNOWN_FILE_EXTENSION") {
      throw new Error(
        `Cannot load ${configPath}. For TypeScript config files, ensure tsx or ts-node is available, ` +
        `or use a .js or .json config file instead.`
      );
    }
    throw new Error(`Failed to load ${configPath}: ${(error as Error).message}`);
  }
}

/**
 * Check if a config type is a JSON file
 */
function isJsonConfig(type: ConfigFileType): boolean {
  return type === "tinybird.config.json" || type === "tinybird.json";
}

/**
 * Check if a config type is a JS/TS file
 */
function isJsTsConfig(type: ConfigFileType): boolean {
  return type === "tinybird.config.ts" || type === "tinybird.config.js";
}

/**
 * Load and resolve the configuration (sync version)
 *
 * Priority order:
 * 1. Environment variables (TINYBIRD_TOKEN, TINYBIRD_URL)
 * 2. tinybird.config.json / tinybird.json file
 * 3. .tinyb file
 *
 * Note: This sync version does not support JS/TS config files.
 * Use loadConfigAsync() if you need to load JS/TS config files.
 */
export function loadConfig(cwd: string = process.cwd()): ResolvedConfig {
  // First, check for direct environment variables
  const envToken = process.env.TINYBIRD_TOKEN;
  if (envToken) {
    const envBaseUrl = process.env.TINYBIRD_URL || DEFAULT_BASE_URL;
    return {
      token: envToken,
      baseUrl: envBaseUrl,
    };
  }

  // Fall back to config file lookup
  const configResult = findConfigFile(cwd);

  if (!configResult) {
    throw new Error(
      `TINYBIRD_TOKEN environment variable not set and could not find a config file. ` +
      `Either set TINYBIRD_TOKEN or run 'npx @tinybirdco/sdk init' / 'tb login' to create a config file.`
    );
  }

  if (isJsTsConfig(configResult.type)) {
    throw new Error(
      `Config file ${configResult.path} is a ${configResult.type.endsWith(".ts") ? "TypeScript" : "JavaScript"} file. ` +
      `Use loadConfigAsync() instead of loadConfig() to load JS/TS config files.`
    );
  }

  if (isJsonConfig(configResult.type)) {
    return loadTinybirdJsonConfig(configResult.path);
  } else {
    return loadTinybConfigFile(configResult.path);
  }
}

/**
 * Load and resolve the configuration (async version)
 *
 * Priority order:
 * 1. Environment variables (TINYBIRD_TOKEN, TINYBIRD_URL)
 * 2. tinybird.config.ts / tinybird.config.js file
 * 3. tinybird.config.json / tinybird.json file
 * 4. .tinyb file
 */
export async function loadConfigAsync(cwd: string = process.cwd()): Promise<ResolvedConfig> {
  // First, check for direct environment variables
  const envToken = process.env.TINYBIRD_TOKEN;
  if (envToken) {
    const envBaseUrl = process.env.TINYBIRD_URL || DEFAULT_BASE_URL;
    return {
      token: envToken,
      baseUrl: envBaseUrl,
    };
  }

  // Fall back to config file lookup
  const configResult = findConfigFile(cwd);

  if (!configResult) {
    throw new Error(
      `TINYBIRD_TOKEN environment variable not set and could not find a config file. ` +
      `Either set TINYBIRD_TOKEN or run 'npx @tinybirdco/sdk init' / 'tb login' to create a config file.`
    );
  }

  if (isJsTsConfig(configResult.type)) {
    return loadJsConfig(configResult.path);
  }

  if (isJsonConfig(configResult.type)) {
    return loadTinybirdJsonConfig(configResult.path);
  } else {
    return loadTinybConfigFile(configResult.path);
  }
}

/**
 * Branch information from Tinybird API
 */
interface BranchResponse {
  id: string;
  name: string;
  token?: string;
}

/**
 * Local workspace response
 */
interface LocalWorkspaceResponse {
  name: string;
  token: string;
}

/**
 * Get the local workspace name based on current directory
 */
function getLocalWorkspaceName(): string {
  const cwd = process.cwd();
  const dirName = path.basename(cwd);
  // Sanitize: replace non-alphanumeric with underscore
  return dirName.replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
}

/**
 * Get or create local workspace and return its token
 */
async function getLocalWorkspaceToken(): Promise<string> {
  const workspaceName = getLocalWorkspaceName();
  const url = `${LOCAL_BASE_URL}/v0/workspaces`;

  // First, try to get existing workspace
  const listResponse = await fetch(url, {
    method: "GET",
  });

  if (listResponse.ok) {
    const data = (await listResponse.json()) as { workspaces: LocalWorkspaceResponse[] };
    const existing = data.workspaces?.find((w) => w.name === workspaceName);
    if (existing?.token) {
      return existing.token;
    }
  }

  // Create new workspace
  const createResponse = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ name: workspaceName }),
  });

  if (!createResponse.ok) {
    const errorText = await createResponse.text();
    throw new Error(`Failed to create local workspace: ${createResponse.status} ${errorText}`);
  }

  const workspace = (await createResponse.json()) as LocalWorkspaceResponse;
  if (!workspace.token) {
    throw new Error("Local workspace created but no token returned");
  }

  return workspace.token;
}

/**
 * Get branch token from Tinybird API
 */
async function getBranchToken(config: ResolvedConfig, branchName: string): Promise<string> {
  const url = new URL(`/v0/environments/${encodeURIComponent(branchName)}`, config.baseUrl);
  url.searchParams.set("with_token", "true");

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    if (response.status === 404) {
      throw new Error(`Branch '${branchName}' not found. Create it first with 'npx @tinybirdco/sdk dev'.`);
    }
    throw new Error(`Failed to get branch '${branchName}': ${response.status} ${errorText}`);
  }

  const branch = (await response.json()) as BranchResponse;
  if (!branch.token) {
    throw new Error(`Branch '${branchName}' exists but no token returned`);
  }

  return branch.token;
}

/**
 * Resolve configuration for a specific environment
 *
 * @param baseConfig - Base configuration from tinybird.json or .tinyb
 * @param environment - Environment to use: "cloud" (default), "local", or branch name
 * @returns Resolved configuration with appropriate token and baseUrl
 */
export async function resolveEnvironmentConfig(
  baseConfig: ResolvedConfig,
  environment?: Environment
): Promise<ResolvedConfig> {
  // Default to cloud
  if (!environment || environment === "cloud") {
    return baseConfig;
  }

  // Local container
  if (environment === "local") {
    const token = await getLocalWorkspaceToken();
    return {
      token,
      baseUrl: LOCAL_BASE_URL,
    };
  }

  // Branch name - fetch token from API
  const branchToken = await getBranchToken(baseConfig, environment);
  return {
    token: branchToken,
    baseUrl: baseConfig.baseUrl,
  };
}
