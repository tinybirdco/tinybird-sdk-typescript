/**
 * Init command - scaffolds a new Tinybird TypeScript project
 */

import * as fs from "fs";
import * as path from "path";
import { getConfigPath, updateConfig, hasValidToken } from "../config.js";
import { browserLogin } from "../auth.js";
import { saveTinybirdToken } from "../env.js";

/**
 * Default schema content
 */
const DEFAULT_SCHEMA = `import {
  defineProject,
  defineDatasource,
  definePipe,
  node,
  t,
  p,
  engine,
} from "@tinybird/sdk";

// ============ Datasources ============

export const events = defineDatasource("events", {
  description: "User events tracking",
  schema: {
    timestamp: t.dateTime(),
    event_id: t.uuid(),
    user_id: t.string(),
    event_type: t.string().lowCardinality(),
    properties: t.json(),
  },
  engine: engine.mergeTree({
    sortingKey: ["user_id", "timestamp"],
    partitionKey: "toYYYYMM(timestamp)",
  }),
});

// ============ Pipes ============

export const topEvents = definePipe("top_events", {
  description: "Get top events by count",
  params: {
    start_date: p.dateTime(),
    end_date: p.dateTime(),
    limit: p.int32().optional(10),
  },
  nodes: [
    node({
      name: "endpoint",
      sql: \`
        SELECT
          event_type,
          count() as event_count,
          uniqExact(user_id) as unique_users
        FROM events
        WHERE timestamp BETWEEN {{DateTime(start_date)}} AND {{DateTime(end_date)}}
        GROUP BY event_type
        ORDER BY event_count DESC
        LIMIT {{Int32(limit, 10)}}
      \`,
    }),
  ],
  output: {
    event_type: t.string(),
    event_count: t.uint64(),
    unique_users: t.uint64(),
  },
  endpoint: true,
});

// ============ Project ============

export default defineProject({
  datasources: {
    events,
  },
  pipes: {
    topEvents,
  },
});
`;

/**
 * Default config content
 */
const DEFAULT_CONFIG = {
  schema: "src/tinybird/schema.ts",
  token: "${TINYBIRD_TOKEN}",
  baseUrl: "https://api.tinybird.co",
};

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
 * - src/tinybird/schema.ts with example schema
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

  // Create config file
  const configPath = getConfigPath(cwd);
  if (fs.existsSync(configPath) && !force) {
    skipped.push("tinybird.json");
  } else {
    try {
      fs.writeFileSync(configPath, JSON.stringify(DEFAULT_CONFIG, null, 2) + "\n");
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

  // Create schema directory and file
  const schemaDir = path.join(cwd, "src", "tinybird");
  const schemaPath = path.join(schemaDir, "schema.ts");

  if (fs.existsSync(schemaPath) && !force) {
    skipped.push("src/tinybird/schema.ts");
  } else {
    try {
      // Create directory if needed
      fs.mkdirSync(schemaDir, { recursive: true });
      fs.writeFileSync(schemaPath, DEFAULT_SCHEMA);
      created.push("src/tinybird/schema.ts");
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
      // Save token to .env.local and update baseUrl in tinybird.json
      try {
        const saveResult = saveTinybirdToken(cwd, authResult.token);
        if (saveResult.created) {
          created.push(".env.local");
        }

        // Update baseUrl in tinybird.json if it changed
        if (authResult.baseUrl) {
          updateConfig(configPath, {
            baseUrl: authResult.baseUrl,
          });
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
