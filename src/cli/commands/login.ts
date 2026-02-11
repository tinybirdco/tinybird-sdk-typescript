/**
 * Login command - authenticate with Tinybird via browser
 */

import * as path from "path";
import { browserLogin, type LoginOptions, type AuthResult } from "../auth.js";
import { updateConfig, findConfigFile } from "../config.js";
import { saveTinybirdToken } from "../env.js";

/**
 * Login command options
 */
export interface RunLoginOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Override the API host (region) */
  apiHost?: string;
}

/**
 * Login command result
 */
export interface LoginResult {
  /** Whether login was successful */
  success: boolean;
  /** Workspace name after login */
  workspaceName?: string;
  /** User email */
  userEmail?: string;
  /** API base URL */
  baseUrl?: string;
  /** Error message if failed */
  error?: string;
}

/**
 * Run the login command
 *
 * Opens browser for authentication and stores credentials in tinybird.json
 *
 * @param options - Login options
 * @returns Login result
 */
export async function runLogin(options: RunLoginOptions = {}): Promise<LoginResult> {
  const cwd = options.cwd ?? process.cwd();

  // Find the actual config file (may be in parent directory)
  const configResult = findConfigFile(cwd);
  if (!configResult) {
    return {
      success: false,
      error: "No tinybird config found. Run 'npx tinybird init' first.",
    };
  }

  const configPath = configResult.path;
  // Get the directory containing the config file for .env.local
  const configDir = path.dirname(configPath);

  const loginOptions: LoginOptions = {};
  if (options.apiHost) {
    loginOptions.apiHost = options.apiHost;
  }

  // Perform browser login
  const authResult: AuthResult = await browserLogin(loginOptions);

  // Guard against missing token in auth response
  if (!authResult.success || !authResult.token) {
    return {
      success: false,
      error: authResult.error ?? "Login failed",
    };
  }

  // Save token to .env.local (in same directory as config file)
  try {
    saveTinybirdToken(configDir, authResult.token);

    // Update baseUrl in config file if it changed (only for JSON configs)
    if (authResult.baseUrl && configPath.endsWith(".json")) {
      updateConfig(configPath, {
        baseUrl: authResult.baseUrl,
      });
    }
  } catch (error) {
    return {
      success: false,
      error: `Failed to save credentials: ${(error as Error).message}`,
    };
  }

  return {
    success: true,
    workspaceName: authResult.workspaceName,
    userEmail: authResult.userEmail,
    baseUrl: authResult.baseUrl,
  };
}
