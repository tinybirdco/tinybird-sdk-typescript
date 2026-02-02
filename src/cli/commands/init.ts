/**
 * Init command - scaffolds a new Tinybird TypeScript project
 */

import * as fs from "fs";
import * as path from "path";
import * as p from "@clack/prompts";
import pc from "picocolors";
import {
  hasValidToken,
  getRelativeTinybirdDir,
  getConfigPath,
  updateConfig,
  type DevMode,
} from "../config.js";
import { browserLogin } from "../auth.js";
import { saveTinybirdToken } from "../env.js";

/**
 * Default starter content for datasources.ts
 */
const DATASOURCES_CONTENT = `import { defineDatasource, t, engine, type InferRow } from "@tinybirdco/sdk";

/**
 * Page views datasource - tracks page view events
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

// Row type - use this for ingesting data
export type PageViewsRow = InferRow<typeof pageViews>;
`;

/**
 * Default starter content for pipes.ts
 */
const PIPES_CONTENT = `import { defineEndpoint, node, t, p, type InferParams, type InferOutputRow } from "@tinybirdco/sdk";

/**
 * Top pages endpoint - get the most visited pages
 */
export const topPages = defineEndpoint("top_pages", {
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
});

// Endpoint types - use these for calling the API
export type TopPagesParams = InferParams<typeof topPages>;
export type TopPagesOutput = InferOutputRow<typeof topPages>;
`;

/**
 * Default starter content for client.ts
 */
const CLIENT_CONTENT = `/**
 * Tinybird Client
 *
 * This file defines the typed Tinybird client for your project.
 * Add your datasources and pipes here as you create them.
 */

import { createTinybirdClient } from "@tinybirdco/sdk";

// Import datasources and their row types
import { pageViews, type PageViewsRow } from "./datasources";

// Import endpoints and their types
import { topPages, type TopPagesParams, type TopPagesOutput } from "./pipes";

// Create the typed Tinybird client
export const tinybird = createTinybirdClient({
  datasources: { pageViews },
  pipes: { topPages },
});

// Re-export types for convenience
export type { PageViewsRow, TopPagesParams, TopPagesOutput };

// Re-export entities
export { pageViews, topPages };
`;

/**
 * Default config content generator
 */
function createDefaultConfig(tinybirdDir: string, devMode: DevMode) {
  return {
    include: [`${tinybirdDir}/datasources.ts`, `${tinybirdDir}/pipes.ts`],
    token: "${TINYBIRD_TOKEN}",
    baseUrl: "https://api.tinybird.co",
    devMode,
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
  /** Development mode - if provided, skip interactive prompt */
  devMode?: DevMode;
  /** Client path - if provided, skip interactive prompt */
  clientPath?: string;
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
  /** Selected development mode */
  devMode?: DevMode;
  /** Selected client path */
  clientPath?: string;
}

/**
 * Run the init command
 *
 * Creates:
 * - tinybird.json in the project root
 * - src/tinybird/ folder with datasources.ts, pipes.ts, and client.ts
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

  // Determine devMode - prompt if not provided
  let devMode: DevMode = options.devMode ?? "branch";

  if (!options.devMode) {
    // Show interactive prompt for workflow selection
    p.intro(pc.cyan("tinybird init"));

    const workflowChoice = await p.select({
      message: "How do you want to develop with Tinybird?",
      options: [
        {
          value: "branch",
          label: "Branches",
          hint: "Use Tinybird Cloud with git-based branching",
        },
        {
          value: "local",
          label: "Tinybird Local",
          hint: "Run your own Tinybird instance locally",
        },
      ],
    });

    if (p.isCancel(workflowChoice)) {
      p.cancel("Init cancelled.");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    devMode = workflowChoice as DevMode;
  }

  // Determine tinybird folder path based on project structure
  const defaultRelativePath = getRelativeTinybirdDir(cwd);
  let relativeTinybirdDir = options.clientPath ?? defaultRelativePath;

  if (!options.clientPath && !options.devMode) {
    // Ask user to confirm or change the client path
    const clientPathChoice = await p.text({
      message: "Where should we generate the Tinybird client?",
      placeholder: defaultRelativePath,
      defaultValue: defaultRelativePath,
    });

    if (p.isCancel(clientPathChoice)) {
      p.cancel("Init cancelled.");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    relativeTinybirdDir = clientPathChoice || defaultRelativePath;
  }

  const tinybirdDir = path.join(cwd, relativeTinybirdDir);

  // File paths
  const datasourcesPath = path.join(tinybirdDir, "datasources.ts");
  const pipesPath = path.join(tinybirdDir, "pipes.ts");
  const clientPath = path.join(tinybirdDir, "client.ts");

  // Create config file (tinybird.json)
  const configPath = getConfigPath(cwd);
  if (fs.existsSync(configPath) && !force) {
    skipped.push("tinybird.json");
  } else {
    try {
      const config = createDefaultConfig(relativeTinybirdDir, devMode);
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

  // Create tinybird directory
  try {
    fs.mkdirSync(tinybirdDir, { recursive: true });
  } catch (error) {
    return {
      success: false,
      created,
      skipped,
      error: `Failed to create ${relativeTinybirdDir} folder: ${
        (error as Error).message
      }`,
    };
  }

  // Create datasources.ts
  if (fs.existsSync(datasourcesPath) && !force) {
    skipped.push(`${relativeTinybirdDir}/datasources.ts`);
  } else {
    try {
      fs.writeFileSync(datasourcesPath, DATASOURCES_CONTENT);
      created.push(`${relativeTinybirdDir}/datasources.ts`);
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create datasources.ts: ${(error as Error).message}`,
      };
    }
  }

  // Create pipes.ts
  if (fs.existsSync(pipesPath) && !force) {
    skipped.push(`${relativeTinybirdDir}/pipes.ts`);
  } else {
    try {
      fs.writeFileSync(pipesPath, PIPES_CONTENT);
      created.push(`${relativeTinybirdDir}/pipes.ts`);
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create pipes.ts: ${(error as Error).message}`,
      };
    }
  }

  // Create client.ts
  if (fs.existsSync(clientPath) && !force) {
    skipped.push(`${relativeTinybirdDir}/client.ts`);
  } else {
    try {
      fs.writeFileSync(clientPath, CLIENT_CONTENT);
      created.push(`${relativeTinybirdDir}/client.ts`);
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create client.ts: ${(error as Error).message}`,
      };
    }
  }

  // Add scripts to package.json if it exists
  const packageJsonPath = path.join(cwd, "package.json");
  if (fs.existsSync(packageJsonPath)) {
    try {
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf-8"));
      let modified = false;

      if (!packageJson.scripts) {
        packageJson.scripts = {};
      }

      if (!packageJson.scripts["tinybird:dev"]) {
        packageJson.scripts["tinybird:dev"] = "tinybird dev";
        modified = true;
      }

      if (!packageJson.scripts["tinybird:build"]) {
        packageJson.scripts["tinybird:build"] = "tinybird build";
        modified = true;
      }

      if (modified) {
        fs.writeFileSync(
          packageJsonPath,
          JSON.stringify(packageJson, null, 2) + "\n"
        );
        created.push("package.json (added tinybird scripts)");
      }
    } catch {
      // Silently ignore package.json errors - not critical
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
        if (
          authResult.baseUrl &&
          authResult.baseUrl !== "https://api.tinybird.co"
        ) {
          updateConfig(configPath, { baseUrl: authResult.baseUrl });
        }

        return {
          success: true,
          created,
          skipped,
          loggedIn: true,
          workspaceName: authResult.workspaceName,
          userEmail: authResult.userEmail,
          devMode,
          clientPath: relativeTinybirdDir,
        };
      } catch (error) {
        // Login succeeded but saving credentials failed
        console.error(
          `Warning: Failed to save credentials: ${(error as Error).message}`
        );
        return {
          success: true,
          created,
          skipped,
          loggedIn: false,
          devMode,
          clientPath: relativeTinybirdDir,
        };
      }
    } else {
      // Login failed or was cancelled
      return {
        success: true,
        created,
        skipped,
        loggedIn: false,
        devMode,
        clientPath: relativeTinybirdDir,
      };
    }
  }

  return {
    success: true,
    created,
    skipped,
    devMode,
    clientPath: relativeTinybirdDir,
  };
}
