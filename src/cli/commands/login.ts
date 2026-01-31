/**
 * Login command - authenticate with Tinybird via browser
 */

import { browserLogin, type LoginOptions, type AuthResult } from "../auth.js";
import { getConfigPath, updateConfig, configExists } from "../config.js";

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

  // Check if config exists - if not, suggest running init first
  if (!configExists(cwd)) {
    return {
      success: false,
      error: "No tinybird.json found. Run 'npx tinybird init' first.",
    };
  }

  const loginOptions: LoginOptions = {};
  if (options.apiHost) {
    loginOptions.apiHost = options.apiHost;
  }

  // Perform browser login
  const authResult: AuthResult = await browserLogin(loginOptions);

  if (!authResult.success) {
    return {
      success: false,
      error: authResult.error ?? "Login failed",
    };
  }

  // Update tinybird.json with the new credentials
  const configPath = getConfigPath(cwd);
  try {
    updateConfig(configPath, {
      token: authResult.token,
      baseUrl: authResult.baseUrl,
    });
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
