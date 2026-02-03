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
 * Default starter content for endpoints.ts
 */
const ENDPOINTS_CONTENT = `import { defineEndpoint, node, t, p, type InferParams, type InferOutputRow } from "@tinybirdco/sdk";

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
 * Add your datasources and endpoints here as you create them.
 */

import { createTinybirdClient } from "@tinybirdco/sdk";

// Import datasources and their row types
import { pageViews, type PageViewsRow } from "./datasources";

// Import endpoints and their types
import { topPages, type TopPagesParams, type TopPagesOutput } from "./endpoints";

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

const TINYBIRD_CI_WORKFLOW = `name: Tinybird CI

on:
  pull_request:
    paths:
      - "tinybird.json"
      - "src/tinybird/**"
      - "tinybird/**"
      - "**/*.datasource"
      - "**/*.pipe"

jobs:
  tinybird:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
          cache: "pnpm"
      - run: pnpm install --frozen-lockfile
      - run: pnpm exec tinybird deploy --check
        env:
          TINYBIRD_TOKEN: \${{ secrets.TINYBIRD_TOKEN }}
`;

const TINYBIRD_CD_WORKFLOW = `name: Tinybird CD

on:
  push:
    branches:
      - main
    paths:
      - "tinybird.json"
      - "src/tinybird/**"
      - "tinybird/**"
      - "**/*.datasource"
      - "**/*.pipe"

jobs:
  tinybird:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
          cache: "pnpm"
      - run: pnpm install --frozen-lockfile
      - run: pnpm exec tinybird deploy
        env:
          TINYBIRD_TOKEN: \${{ secrets.TINYBIRD_TOKEN }}
`;

/**
 * Default config content generator
 */
function createDefaultConfig(
  tinybirdDir: string,
  devMode: DevMode,
  additionalIncludes: string[] = []
) {
  const include = [
    `${tinybirdDir}/datasources.ts`,
    `${tinybirdDir}/endpoints.ts`,
    ...additionalIncludes,
  ];
  return {
    include,
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
  /** Skip prompts for existing datafiles - for testing */
  skipDatafilePrompt?: boolean;
  /** Auto-include existing datafiles without prompting - for testing */
  includeExistingDatafiles?: boolean;
  /** Skip GitHub Actions workflow prompts */
  skipWorkflowPrompt?: boolean;
  /** Include Tinybird CI workflow */
  includeCiWorkflow?: boolean;
  /** Include Tinybird CD workflow */
  includeCdWorkflow?: boolean;
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
  /** Existing datafiles that were added to config */
  existingDatafiles?: string[];
  /** Whether a Tinybird CI workflow was created */
  ciWorkflowCreated?: boolean;
  /** Whether a Tinybird CD workflow was created */
  cdWorkflowCreated?: boolean;
}

/**
 * Find existing .datasource and .pipe files in the repository
 *
 * @param cwd - Working directory to search from
 * @param maxDepth - Maximum directory depth to search (default: 5)
 * @returns Array of relative file paths
 */
export function findExistingDatafiles(
  cwd: string,
  maxDepth: number = 5
): string[] {
  const files: string[] = [];

  function searchDir(dir: string, depth: number): void {
    if (depth > maxDepth) return;

    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return; // Skip directories we can't read
    }

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);

      // Skip node_modules and hidden directories
      if (
        entry.isDirectory() &&
        (entry.name === "node_modules" ||
          entry.name.startsWith(".") ||
          entry.name === "dist" ||
          entry.name === "build")
      ) {
        continue;
      }

      if (entry.isDirectory()) {
        searchDir(fullPath, depth + 1);
      } else if (
        entry.isFile() &&
        (entry.name.endsWith(".datasource") || entry.name.endsWith(".pipe"))
      ) {
        // Convert to relative path
        const relativePath = path.relative(cwd, fullPath);
        files.push(relativePath);
      }
    }
  }

  searchDir(cwd, 0);
  return files.sort();
}

/**
 * Run the init command
 *
 * Creates:
 * - tinybird.json in the project root
 * - src/tinybird/ folder with datasources.ts, endpoints.ts, and client.ts
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
  let existingDatafiles: string[] = [];
  let ciWorkflowCreated = false;
  let cdWorkflowCreated = false;

  // Check for existing .datasource and .pipe files
  const foundDatafiles = findExistingDatafiles(cwd);

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

  const skipWorkflowPrompt =
    options.skipWorkflowPrompt ??
    (options.devMode !== undefined || options.clientPath !== undefined);
  let includeCiWorkflow = options.includeCiWorkflow ?? false;
  let includeCdWorkflow = options.includeCdWorkflow ?? false;

  if (!skipWorkflowPrompt && options.includeCiWorkflow === undefined) {
    const confirmCiWorkflow = await p.confirm({
      message: "Create GitHub Actions workflow for Tinybird CI (tinybird-ci.yaml)?",
      initialValue: true,
    });

    if (p.isCancel(confirmCiWorkflow)) {
      p.cancel("Init cancelled.");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    includeCiWorkflow = confirmCiWorkflow;
  }

  if (!skipWorkflowPrompt && options.includeCdWorkflow === undefined) {
    const confirmCdWorkflow = await p.confirm({
      message: "Create GitHub Actions workflow for Tinybird CD (tinybird-cd.yaml)?",
      initialValue: true,
    });

    if (p.isCancel(confirmCdWorkflow)) {
      p.cancel("Init cancelled.");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    includeCdWorkflow = confirmCdWorkflow;
  }

  // Ask about existing datafiles if found
  if (foundDatafiles.length > 0 && !options.skipDatafilePrompt) {
    const includeDatafiles =
      options.includeExistingDatafiles ??
      (await promptForExistingDatafiles(foundDatafiles));

    if (includeDatafiles) {
      existingDatafiles = foundDatafiles;
    }
  } else if (options.includeExistingDatafiles && foundDatafiles.length > 0) {
    existingDatafiles = foundDatafiles;
  }

  const tinybirdDir = path.join(cwd, relativeTinybirdDir);

  // File paths
  const datasourcesPath = path.join(tinybirdDir, "datasources.ts");
  const endpointsPath = path.join(tinybirdDir, "endpoints.ts");
  const clientPath = path.join(tinybirdDir, "client.ts");

  // Create config file (tinybird.json)
  const configPath = getConfigPath(cwd);
  if (fs.existsSync(configPath) && !force) {
    skipped.push("tinybird.json");
  } else {
    try {
      const config = createDefaultConfig(
        relativeTinybirdDir,
        devMode,
        existingDatafiles
      );
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

  // Create endpoints.ts
  if (fs.existsSync(endpointsPath) && !force) {
    skipped.push(`${relativeTinybirdDir}/endpoints.ts`);
  } else {
    try {
      fs.writeFileSync(endpointsPath, ENDPOINTS_CONTENT);
      created.push(`${relativeTinybirdDir}/endpoints.ts`);
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create endpoints.ts: ${(error as Error).message}`,
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

      if (!packageJson.scripts["tinybird:deploy"]) {
        packageJson.scripts["tinybird:deploy"] = "tinybird deploy";
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

  const workflowsDir = path.join(cwd, ".github", "workflows");
  const ciWorkflowPath = path.join(workflowsDir, "tinybird-ci.yaml");
  const cdWorkflowPath = path.join(workflowsDir, "tinybird-cd.yaml");

  if (includeCiWorkflow || includeCdWorkflow) {
    try {
      fs.mkdirSync(workflowsDir, { recursive: true });
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create .github/workflows folder: ${(error as Error).message}`,
      };
    }
  }

  if (includeCiWorkflow) {
    if (fs.existsSync(ciWorkflowPath) && !force) {
      skipped.push(".github/workflows/tinybird-ci.yaml");
    } else {
      try {
        fs.writeFileSync(ciWorkflowPath, TINYBIRD_CI_WORKFLOW);
        created.push(".github/workflows/tinybird-ci.yaml");
        ciWorkflowCreated = true;
      } catch (error) {
        return {
          success: false,
          created,
          skipped,
          error: `Failed to create tinybird-ci.yaml: ${(error as Error).message}`,
        };
      }
    }
  }

  if (includeCdWorkflow) {
    if (fs.existsSync(cdWorkflowPath) && !force) {
      skipped.push(".github/workflows/tinybird-cd.yaml");
    } else {
      try {
        fs.writeFileSync(cdWorkflowPath, TINYBIRD_CD_WORKFLOW);
        created.push(".github/workflows/tinybird-cd.yaml");
        cdWorkflowCreated = true;
      } catch (error) {
        return {
          success: false,
          created,
          skipped,
          error: `Failed to create tinybird-cd.yaml: ${(error as Error).message}`,
        };
      }
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
        const baseUrl = authResult.baseUrl ?? "https://api.tinybird.co";
        if (baseUrl !== "https://api.tinybird.co") {
          updateConfig(configPath, { baseUrl });
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
          existingDatafiles:
            existingDatafiles.length > 0 ? existingDatafiles : undefined,
          ciWorkflowCreated,
          cdWorkflowCreated,
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
          existingDatafiles:
            existingDatafiles.length > 0 ? existingDatafiles : undefined,
          ciWorkflowCreated,
          cdWorkflowCreated,
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
        existingDatafiles:
          existingDatafiles.length > 0 ? existingDatafiles : undefined,
        ciWorkflowCreated,
        cdWorkflowCreated,
      };
    }
  }

  return {
    success: true,
    created,
    skipped,
    devMode,
    clientPath: relativeTinybirdDir,
    existingDatafiles:
      existingDatafiles.length > 0 ? existingDatafiles : undefined,
    ciWorkflowCreated,
    cdWorkflowCreated,
  };
}

/**
 * Prompt user about including existing datafiles
 */
async function promptForExistingDatafiles(
  datafiles: string[]
): Promise<boolean> {
  const datasourceCount = datafiles.filter((f) =>
    f.endsWith(".datasource")
  ).length;
  const pipeCount = datafiles.filter((f) => f.endsWith(".pipe")).length;

  const parts: string[] = [];
  if (datasourceCount > 0) {
    parts.push(`${datasourceCount} .datasource file${datasourceCount > 1 ? "s" : ""}`);
  }
  if (pipeCount > 0) {
    parts.push(`${pipeCount} .pipe file${pipeCount > 1 ? "s" : ""}`);
  }

  const confirmInclude = await p.confirm({
    message: `Found ${parts.join(" and ")} in your project. Include them in tinybird.json?`,
    initialValue: true,
  });

  if (p.isCancel(confirmInclude)) {
    return false;
  }

  return confirmInclude;
}
