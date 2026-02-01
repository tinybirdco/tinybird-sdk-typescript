/**
 * Init command - scaffolds a new Tinybird TypeScript project
 */

import * as fs from "fs";
import * as path from "path";
import {
  hasValidToken,
  getTinybirdSchemaPath,
  getRelativeSchemaPath,
  getConfigPath,
  updateConfig,
} from "../config.js";
import { browserLogin } from "../auth.js";
import { saveTinybirdToken } from "../env.js";

/**
 * Default schema content
 */
const DEFAULT_SCHEMA = `import { defineProject } from "@tinybird/sdk";

export default defineProject({
  datasources: {},
  pipes: {},
});
`;

/**
 * Default config content generator
 */
function createDefaultConfig(schemaPath: string) {
  return {
    schema: schemaPath,
    token: "${TINYBIRD_TOKEN}",
    baseUrl: "https://api.tinybird.co",
  };
}

/**
 * Init command options
 */
export interface InitOptions {
  /** Working directory (defaults to cwd) */
  cwd?: string;
  /** Force overwrite existing files */
  force?: boolean;
  /** Skip the login flow */
  skipLogin?: boolean;
}

/**
 * Init command result
 */
export interface InitResult {
  /** Whether initialization was successful */
  success: boolean;
  /** Files that were created */
  created: string[];
  /** Files that were skipped (already exist) */
  skipped: string[];
  /** Error message if failed */
  error?: string;
  /** Whether login was completed */
  loggedIn?: boolean;
  /** Workspace name after login */
  workspaceName?: string;
  /** User email after login */
  userEmail?: string;
}

/**
 * Run the init command
 *
 * Creates:
 * - tinybird.json in the project root
 * - lib/tinybird.ts (or src/lib/tinybird.ts if project has src folder)
 *
 * @param options - Init options
 * @returns Init result
 */
export async function runInit(options: InitOptions = {}): Promise<InitResult> {
  const cwd = options.cwd ?? process.cwd();
  const force = options.force ?? false;
  const skipLogin = options.skipLogin ?? false;

  const created: string[] = [];
  const skipped: string[] = [];

  // Determine schema path based on project structure
  const schemaPath = getTinybirdSchemaPath(cwd);
  const schemaDir = path.dirname(schemaPath);
  const relativeSchemaPath = getRelativeSchemaPath(cwd);

  // Create config file (tinybird.json)
  const configPath = getConfigPath(cwd);
  if (fs.existsSync(configPath) && !force) {
    skipped.push("tinybird.json");
  } else {
    try {
      const config = createDefaultConfig(relativeSchemaPath);
      fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + "\n");
      created.push("tinybird.json");
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create tinybird.json: ${(error as Error).message}`,
      };
    }
  }

  // Create schema file
  if (fs.existsSync(schemaPath) && !force) {
    skipped.push(relativeSchemaPath);
  } else {
    try {
      // Create directory if needed
      fs.mkdirSync(schemaDir, { recursive: true });
      fs.writeFileSync(schemaPath, DEFAULT_SCHEMA);
      created.push(relativeSchemaPath);
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create schema file: ${(error as Error).message}`,
      };
    }
  }

  // Check if login is needed
  if (!skipLogin && !hasValidToken(cwd)) {
    console.log("\nNo authentication found. Starting login flow...\n");

    const authResult = await browserLogin();

    if (authResult.success && authResult.token) {
      // Save token to .env.local
      try {
        const saveResult = saveTinybirdToken(cwd, authResult.token);
        if (saveResult.created) {
          created.push(".env.local");
        }

        // If custom base URL, update tinybird.json
        if (authResult.baseUrl && authResult.baseUrl !== "https://api.tinybird.co") {
          updateConfig(configPath, { baseUrl: authResult.baseUrl });
        }

        return {
          success: true,
          created,
          skipped,
          loggedIn: true,
          workspaceName: authResult.workspaceName,
          userEmail: authResult.userEmail,
        };
      } catch (error) {
        // Login succeeded but saving credentials failed
        console.error(`Warning: Failed to save credentials: ${(error as Error).message}`);
        return {
          success: true,
          created,
          skipped,
          loggedIn: false,
        };
      }
    } else {
      // Login failed or was cancelled
      return {
        success: true,
        created,
        skipped,
        loggedIn: false,
      };
    }
  }

  return {
    success: true,
    created,
    skipped,
  };
}
