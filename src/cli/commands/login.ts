/**
 * Login command - authenticate with Tinybird via browser
 */

import * as fs from "fs";
import * as path from "path";
import { browserLogin, type LoginOptions, type AuthResult } from "../auth.js";
import { updateConfig, findConfigFile } from "../config.js";

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
  const configPath = findConfigFile(cwd);
  if (!configPath) {
    return {
      success: false,
      error: "No tinybird.json found. Run 'npx tinybird init' first.",
    };
  }

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

  // Save token to .env.local (in same directory as tinybird.json)
  try {
    const envLocalPath = path.join(configDir, ".env.local");
    const envContent = `TINYBIRD_TOKEN=${authResult.token}\n`;

    // Append to existing .env.local or create new one
    if (fs.existsSync(envLocalPath)) {
      const existingContent = fs.readFileSync(envLocalPath, "utf-8");
      // Check if TINYBIRD_TOKEN already exists
      if (existingContent.includes("TINYBIRD_TOKEN=")) {
        // Replace existing token
        const updatedContent = existingContent.replace(
          /TINYBIRD_TOKEN=.*/,
          `TINYBIRD_TOKEN=${authResult.token}`
        );
        fs.writeFileSync(envLocalPath, updatedContent);
      } else {
        // Append token
        fs.appendFileSync(envLocalPath, envContent);
      }
    } else {
      fs.writeFileSync(envLocalPath, envContent);
    }

    // Update baseUrl in tinybird.json if it changed
    if (authResult.baseUrl) {
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
