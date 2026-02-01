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
 * Default starter content for the tinybird file
 * Contains example datasource and pipe definitions
 */
const DEFAULT_STARTER_CONTENT = `import { defineDatasource, definePipe, node, t, p, engine } from "@tinybird/sdk";

/**
 * Example datasource - page views tracking
 * Define your table schema with full type safety
 */
export const pageViews = defineDatasource("page_views", {
  description: "Page view tracking data",
  schema: {
    timestamp: t.dateTime(),
    session_id: t.string(),
    pathname: t.string(),
    referrer: t.string().nullable(),
  },
  engine: engine.mergeTree({
    sortingKey: ["pathname", "timestamp"],
  }),
});

/**
 * Example pipe - top pages query
 * Define SQL transformations with typed parameters and output
 */
export const topPages = definePipe("top_pages", {
  description: "Get the most visited pages",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
    limit: p.int32().optional(10).describe("Number of results"),
  },
  nodes: [
    node({
      name: "aggregated",
      sql: \`
        SELECT
          pathname,
          count() AS views
        FROM page_views
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
        GROUP BY pathname
        ORDER BY views DESC
        LIMIT {{Int32(limit, 10)}}
      \`,
    }),
  ],
  output: {
    pathname: t.string(),
    views: t.uint64(),
  },
  endpoint: true,
});
`;

/**
 * Default config content generator
 */
function createDefaultConfig(includePath: string) {
  return {
    include: [includePath],
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

  // Create starter file with example definitions
  if (fs.existsSync(schemaPath) && !force) {
    skipped.push(relativeSchemaPath);
  } else {
    try {
      // Create directory if needed
      fs.mkdirSync(schemaDir, { recursive: true });
      fs.writeFileSync(schemaPath, DEFAULT_STARTER_CONTENT);
      created.push(relativeSchemaPath);
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create starter file: ${(error as Error).message}`,
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
