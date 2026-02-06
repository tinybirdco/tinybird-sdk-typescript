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
