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
  findExistingConfigPath,
  updateConfig,
  loadConfigAsync,
  type DevMode,
} from "../config.js";
import { browserLogin } from "../auth.js";
import { saveTinybirdToken } from "../env.js";
import { getGitRoot } from "../git.js";
import { fetchAllResources } from "../../api/resources.js";
import { generateCombinedFile } from "../../codegen/index.js";
import { execSync } from "child_process";
import {
  detectPackageManager,
  getPackageManagerAddCmd,
  hasTinybirdSdkDependency,
} from "../utils/package-manager.js";

/**
 * Default starter content for tinybird.ts (single file with everything)
 */
const TINYBIRD_CONTENT = `/**
 * Tinybird Definitions
 *
 * Define your datasources, endpoints, and client here.
 */

import {
  defineDatasource,
  defineEndpoint,
  createTinybirdClient,
  node,
  t,
  p,
  engine,
  type InferRow,
  type InferParams,
  type InferOutputRow,
} from "@tinybirdco/sdk";

// ============================================================================
// Datasources
// ============================================================================

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

export type PageViewsRow = InferRow<typeof pageViews>;

// ============================================================================
// Endpoints
// ============================================================================

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

export type TopPagesParams = InferParams<typeof topPages>;
export type TopPagesOutput = InferOutputRow<typeof topPages>;

// ============================================================================
// Client
// ============================================================================

export const tinybird = createTinybirdClient({
  datasources: { pageViews },
  pipes: { topPages },
});
`;

/**
 * Generate GitHub CI workflow content
 * @param workingDirectory - Optional subdirectory where the project lives
 */
function generateGithubCiWorkflow(workingDirectory?: string): string {
  const pathPrefix = workingDirectory ? `${workingDirectory}/` : "";
  const workDir = workingDirectory ?? ".";
  const cachePathOption = workingDirectory
    ? `
          cache-dependency-path: ${workingDirectory}/pnpm-lock.yaml`
    : "";

  return `name: Tinybird CI

on:
  pull_request:
    paths:
      - "${pathPrefix}tinybird.config.*"
      - "${pathPrefix}tinybird.json"
      - "${pathPrefix}**/*.ts"

env:
  TINYBIRD_TOKEN: \${{ secrets.TINYBIRD_TOKEN }}

jobs:
  tinybird:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: "${workDir}"
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 10
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
          cache: "pnpm"${cachePathOption}
      - run: pnpm install --frozen-lockfile
      - name: Create preview branch
        run: pnpm run tinybird:preview
`;
}

/**
 * Generate GitHub CD workflow content
 * @param workingDirectory - Optional subdirectory where the project lives
 */
function generateGithubCdWorkflow(workingDirectory?: string): string {
  const pathPrefix = workingDirectory ? `${workingDirectory}/` : "";
  const workDir = workingDirectory ?? ".";
  const cachePathOption = workingDirectory
    ? `
          cache-dependency-path: ${workingDirectory}/pnpm-lock.yaml`
    : "";

  return `name: Tinybird CD

on:
  push:
    branches:
      - main
    paths:
      - "${pathPrefix}tinybird.config.*"
      - "${pathPrefix}tinybird.json"
      - "${pathPrefix}**/*.ts"

env:
  TINYBIRD_TOKEN: \${{ secrets.TINYBIRD_TOKEN }}

jobs:
  tinybird:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: "${workDir}"
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 10
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
          cache: "pnpm"${cachePathOption}
      - run: pnpm install --frozen-lockfile
      - run: pnpm run tinybird:deploy
`;
}

/**
 * Generate GitLab CI workflow content
 * @param workingDirectory - Optional subdirectory where the project lives
 */
function generateGitlabCiWorkflow(workingDirectory?: string): string {
  const pathPrefix = workingDirectory ? `${workingDirectory}/` : "";
  const cdCommand = workingDirectory ? `cd ${workingDirectory} && ` : "";

  return `stages:
  - tinybird

tinybird_ci:
  stage: tinybird
  image: node:22
  rules:
    - changes:
        - ${pathPrefix}tinybird.config.*
        - ${pathPrefix}tinybird.json
        - ${pathPrefix}**/*.ts
  script:
    - corepack enable
    - ${cdCommand}pnpm install --frozen-lockfile
    - ${cdCommand}pnpm run tinybird:preview
  variables:
    TINYBIRD_TOKEN: \${TINYBIRD_TOKEN}
`;
}

/**
 * Generate GitLab CD workflow content
 * @param workingDirectory - Optional subdirectory where the project lives
 */
function generateGitlabCdWorkflow(workingDirectory?: string): string {
  const pathPrefix = workingDirectory ? `${workingDirectory}/` : "";
  const cdCommand = workingDirectory ? `cd ${workingDirectory} && ` : "";

  return `stages:
  - tinybird

tinybird_cd:
  stage: tinybird
  image: node:22
  rules:
    - if: '$CI_COMMIT_BRANCH == "main"'
      changes:
        - ${pathPrefix}tinybird.config.*
        - ${pathPrefix}tinybird.json
        - ${pathPrefix}**/*.ts
  script:
    - corepack enable
    - ${cdCommand}pnpm install --frozen-lockfile
    - ${cdCommand}pnpm run tinybird:deploy
  variables:
    TINYBIRD_TOKEN: \${TINYBIRD_TOKEN}
`;
}

/**
 * Default config content generator (for JSON files)
 */
function createDefaultConfig(
  tinybirdFilePath: string,
  devMode: DevMode,
  additionalIncludes: string[] = []
) {
  const include = [tinybirdFilePath, ...additionalIncludes];
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
  /** Git provider for workflow templates */
  workflowProvider?: "github" | "gitlab";
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
  /** Git provider used for workflow templates */
  workflowProvider?: "github" | "gitlab";
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
  let didPrompt = false;
  let existingDatafiles: string[] = [];
  let ciWorkflowCreated = false;
  let cdWorkflowCreated = false;
  let workflowProvider = options.workflowProvider;

  // Check for existing .datasource and .pipe files
  const foundDatafiles = findExistingDatafiles(cwd);

  // Determine devMode - prompt if not provided
  let devMode: DevMode = options.devMode ?? "branch";

  if (!options.devMode) {
    // Show interactive prompt for workflow selection
    p.intro(pc.cyan("tinybird.config.json"));

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
      p.cancel("Operation cancelled");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    didPrompt = true;
    devMode = workflowChoice as DevMode;
  }

  // Determine tinybird folder path based on project structure
  const defaultRelativePath = getRelativeTinybirdDir(cwd);
  let relativeTinybirdDir = options.clientPath ?? defaultRelativePath;

  if (!options.clientPath && !options.devMode) {
    // Ask user to confirm or change the client path
    const clientPathChoice = await p.text({
      message: "Where should we create initial tinybird.ts file?",
      placeholder: defaultRelativePath,
      defaultValue: defaultRelativePath,
    });

    if (p.isCancel(clientPathChoice)) {
      p.cancel("Operation cancelled");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    didPrompt = true;
    relativeTinybirdDir = clientPathChoice || defaultRelativePath;
  }

  const skipWorkflowPrompt =
    options.skipWorkflowPrompt ??
    (options.devMode !== undefined || options.clientPath !== undefined);
  let includeCiWorkflow = options.includeCiWorkflow ?? false;
  let includeCdWorkflow = options.includeCdWorkflow ?? false;
  const shouldPromptWorkflows =
    !skipWorkflowPrompt && options.includeCiWorkflow === undefined;

  if (shouldPromptWorkflows) {
    const ciChoice = await p.select({
      message: "Set up CI/CD workflows?",
      options: [
        {
          value: "github",
          label: "GitHub Actions",
        },
        {
          value: "gitlab",
          label: "GitLab CI",
        },
        {
          value: "skip",
          label: "Skip",
        },
      ],
    });

    if (p.isCancel(ciChoice)) {
      p.cancel("Operation cancelled");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }

    didPrompt = true;
    if (ciChoice !== "skip") {
      includeCiWorkflow = true;
      includeCdWorkflow = true;
      workflowProvider = ciChoice as "github" | "gitlab";
    }
  } else if ((includeCiWorkflow || includeCdWorkflow) && !workflowProvider) {
    workflowProvider = "github";
  }

  // Ask about existing datafiles if found
  let datafileAction: DatafileAction = "skip";
  if (foundDatafiles.length > 0 && !options.skipDatafilePrompt) {
    if (options.includeExistingDatafiles !== undefined) {
      datafileAction = options.includeExistingDatafiles ? "include" : "skip";
    } else {
      didPrompt = true;
      datafileAction = await promptForExistingDatafiles(foundDatafiles);
    }

    if (datafileAction === "include") {
      existingDatafiles = foundDatafiles;
    }
    // Note: "codegen" option is handled after file creation
  } else if (options.includeExistingDatafiles && foundDatafiles.length > 0) {
    existingDatafiles = foundDatafiles;
    datafileAction = "include";
  }

  if (didPrompt) {
    const devModeLabel = devMode === "local" ? "Tinybird Local" : "Branches";
    let datafileSummary = "none found";
    if (foundDatafiles.length > 0) {
      if (datafileAction === "include") {
        datafileSummary = `${foundDatafiles.length} included`;
      } else if (datafileAction === "codegen") {
        datafileSummary = `${foundDatafiles.length} will generate .ts`;
      } else {
        datafileSummary = "skipped";
      }
    }
    let cicdSummary = "skipped";
    if (includeCiWorkflow || includeCdWorkflow) {
      cicdSummary = workflowProvider === "gitlab" ? "GitLab" : "GitHub";
    }

    const summaryLines = [
      `Mode: ${devModeLabel}`,
      `Folder: ${relativeTinybirdDir}/`,
      `Existing datafiles: ${datafileSummary}`,
      `CI/CD: ${cicdSummary}`,
    ];

    p.note(summaryLines.join("\n"), "Installation Summary");

    const confirmInit = await p.confirm({
      message: "Proceed with initialization?",
      initialValue: true,
    });

    if (p.isCancel(confirmInit) || !confirmInit) {
      p.cancel("Init cancelled.");
      return {
        success: false,
        created: [],
        skipped: [],
        error: "Cancelled by user",
      };
    }
  }

  // relativeTinybirdDir is now a file path like "src/lib/tinybird.ts"
  const tinybirdFilePath = path.join(cwd, relativeTinybirdDir);
  const tinybirdDir = path.dirname(tinybirdFilePath);

  // Create or update config file
  // Check for any existing config file first
  const existingConfigPath = findExistingConfigPath(cwd);
  const newConfigPath = getConfigPath(cwd);

  if (existingConfigPath && !force) {
    // Update existing config file (only if it's JSON)
    const configFileName = path.basename(existingConfigPath);
    if (existingConfigPath.endsWith(".json")) {
      try {
        const config = createDefaultConfig(
          relativeTinybirdDir,
          devMode,
          existingDatafiles
        );
        updateConfig(existingConfigPath, {
          include: config.include,
          devMode: config.devMode,
        });
        created.push(`${configFileName} (updated)`);
      } catch (error) {
        return {
          success: false,
          created,
          skipped,
          error: `Failed to update ${configFileName}: ${
            (error as Error).message
          }`,
        };
      }
    } else {
      // JS config file exists - skip and let user update manually
      skipped.push(
        `${configFileName} (JS config files must be updated manually)`
      );
    }
  } else {
    // Create new config file with JSON format
    try {
      const config = createDefaultConfig(
        relativeTinybirdDir,
        devMode,
        existingDatafiles
      );
      fs.writeFileSync(newConfigPath, JSON.stringify(config, null, 2) + "\n");
      created.push("tinybird.config.json");
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create tinybird.config.json: ${
          (error as Error).message
        }`,
      };
    }
  }

  // Create lib directory
  try {
    fs.mkdirSync(tinybirdDir, { recursive: true });
  } catch (error) {
    return {
      success: false,
      created,
      skipped,
      error: `Failed to create ${path.dirname(relativeTinybirdDir)} folder: ${
        (error as Error).message
      }`,
    };
  }

  // Create tinybird.ts (skip if codegen will generate it)
  if (datafileAction !== "codegen") {
    if (fs.existsSync(tinybirdFilePath) && !force) {
      skipped.push(relativeTinybirdDir);
    } else {
      try {
        fs.writeFileSync(tinybirdFilePath, TINYBIRD_CONTENT);
        created.push(relativeTinybirdDir);
      } catch (error) {
        return {
          success: false,
          created,
          skipped,
          error: `Failed to create ${relativeTinybirdDir}: ${
            (error as Error).message
          }`,
        };
      }
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

      if (!packageJson.scripts["tinybird:preview"]) {
        packageJson.scripts["tinybird:preview"] = "tinybird preview";
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

  // Install @tinybirdco/sdk if not already installed
  if (!hasTinybirdSdkDependency(cwd)) {
    const packageManager = detectPackageManager(cwd);
    const addCmd = getPackageManagerAddCmd(packageManager);
    const s = p.spinner();
    s.start("Installing dependencies");
    try {
      execSync(`${addCmd} @tinybirdco/sdk`, { cwd, stdio: "pipe" });
      s.stop("Installed dependencies");
      created.push("@tinybirdco/sdk");
    } catch (error) {
      s.stop("Failed to install dependencies");
      console.error(
        `Warning: Failed to install @tinybirdco/sdk: ${
          (error as Error).message
        }`
      );
    }
  }

  // Use git root for workflow files, fallback to cwd if not in a git repo
  const projectRoot = getGitRoot() ?? cwd;
  const githubWorkflowsDir = path.join(projectRoot, ".github", "workflows");
  const gitlabWorkflowsDir = path.join(projectRoot, ".gitlab");
  const githubCiPath = path.join(githubWorkflowsDir, "tinybird-ci.yaml");
  const githubCdPath = path.join(githubWorkflowsDir, "tinybird-cd.yaml");
  const gitlabCiPath = path.join(gitlabWorkflowsDir, "tinybird-ci.yaml");
  const gitlabCdPath = path.join(gitlabWorkflowsDir, "tinybird-cd.yaml");

  // Calculate working directory relative to git root (for monorepo support)
  const gitRoot = getGitRoot();
  const workingDirectory =
    gitRoot && cwd !== gitRoot ? path.relative(gitRoot, cwd) : undefined;

  if (includeCiWorkflow || includeCdWorkflow) {
    const workflowsDir =
      workflowProvider === "github" ? githubWorkflowsDir : gitlabWorkflowsDir;
    try {
      fs.mkdirSync(workflowsDir, { recursive: true });
    } catch (error) {
      return {
        success: false,
        created,
        skipped,
        error: `Failed to create ${
          workflowProvider === "github" ? ".github/workflows" : ".gitlab"
        } folder: ${(error as Error).message}`,
      };
    }
  }

  if (workflowProvider === "github") {
    if (includeCiWorkflow) {
      if (fs.existsSync(githubCiPath) && !force) {
        skipped.push(".github/workflows/tinybird-ci.yaml");
      } else {
        try {
          fs.writeFileSync(
            githubCiPath,
            generateGithubCiWorkflow(workingDirectory)
          );
          created.push(".github/workflows/tinybird-ci.yaml");
          ciWorkflowCreated = true;
        } catch (error) {
          return {
            success: false,
            created,
            skipped,
            error: `Failed to create .github/workflows/tinybird-ci.yaml: ${
              (error as Error).message
            }`,
          };
        }
      }
    }

    if (includeCdWorkflow) {
      if (fs.existsSync(githubCdPath) && !force) {
        skipped.push(".github/workflows/tinybird-cd.yaml");
      } else {
        try {
          fs.writeFileSync(
            githubCdPath,
            generateGithubCdWorkflow(workingDirectory)
          );
          created.push(".github/workflows/tinybird-cd.yaml");
          cdWorkflowCreated = true;
        } catch (error) {
          return {
            success: false,
            created,
            skipped,
            error: `Failed to create .github/workflows/tinybird-cd.yaml: ${
              (error as Error).message
            }`,
          };
        }
      }
    }
  }

  if (workflowProvider === "gitlab") {
    if (includeCiWorkflow) {
      if (fs.existsSync(gitlabCiPath) && !force) {
        skipped.push(".gitlab/tinybird-ci.yaml");
      } else {
        try {
          fs.writeFileSync(
            gitlabCiPath,
            generateGitlabCiWorkflow(workingDirectory)
          );
          created.push(".gitlab/tinybird-ci.yaml");
          ciWorkflowCreated = true;
        } catch (error) {
          return {
            success: false,
            created,
            skipped,
            error: `Failed to create .gitlab/tinybird-ci.yaml: ${
              (error as Error).message
            }`,
          };
        }
      }
    }

    if (includeCdWorkflow) {
      if (fs.existsSync(gitlabCdPath) && !force) {
        skipped.push(".gitlab/tinybird-cd.yaml");
      } else {
        try {
          fs.writeFileSync(
            gitlabCdPath,
            generateGitlabCdWorkflow(workingDirectory)
          );
          created.push(".gitlab/tinybird-cd.yaml");
          cdWorkflowCreated = true;
        } catch (error) {
          return {
            success: false,
            created,
            skipped,
            error: `Failed to create .gitlab/tinybird-cd.yaml: ${
              (error as Error).message
            }`,
          };
        }
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

        // If custom base URL, update config file
        const baseUrl = authResult.baseUrl ?? "https://api.tinybird.co";
        if (baseUrl !== "https://api.tinybird.co") {
          const currentConfigPath = findExistingConfigPath(cwd);
          if (currentConfigPath && currentConfigPath.endsWith(".json")) {
            updateConfig(currentConfigPath, { baseUrl });
          }
        }

        // Generate TypeScript from existing Tinybird resources if requested
        if (datafileAction === "codegen") {
          const tinybirdDir = path.join(cwd, relativeTinybirdDir);
          await runCodegen(
            baseUrl,
            authResult.token,
            tinybirdDir,
            relativeTinybirdDir,
            created,
            foundDatafiles
          );
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
          workflowProvider,
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
          workflowProvider,
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
        workflowProvider,
      };
    }
  }

  // Generate TypeScript from existing Tinybird resources if requested
  // (when user is already logged in)
  if (datafileAction === "codegen" && hasValidToken(cwd)) {
    try {
      const config = await loadConfigAsync(cwd);
      const tinybirdDir = path.join(cwd, relativeTinybirdDir);
      await runCodegen(
        config.baseUrl,
        config.token,
        tinybirdDir,
        relativeTinybirdDir,
        created,
        foundDatafiles
      );
    } catch (error) {
      console.error(
        `Warning: Failed to generate TypeScript: ${(error as Error).message}`
      );
    }
  }

  if (didPrompt) {
    p.outro("Done!");
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
    workflowProvider,
  };
}

type DatafileAction = "include" | "codegen" | "skip";

/**
 * Generate TypeScript files from Tinybird workspace resources
 * Only generates for resources that match the local datafiles
 */
async function runCodegen(
  baseUrl: string,
  token: string,
  tinybirdFilePath: string,
  relativeTinybirdPath: string,
  created: string[],
  localDatafiles: string[]
): Promise<void> {
  try {
    // Extract names from local datafiles (without extension)
    const localDatasourceNames = new Set(
      localDatafiles
        .filter((f) => f.endsWith(".datasource"))
        .map((f) => path.basename(f, ".datasource"))
    );
    const localPipeNames = new Set(
      localDatafiles
        .filter((f) => f.endsWith(".pipe"))
        .map((f) => path.basename(f, ".pipe"))
    );

    const resources = await fetchAllResources({ baseUrl, token });

    // Filter to only resources matching local files
    const matchedDatasources = resources.datasources.filter((ds) =>
      localDatasourceNames.has(ds.name)
    );
    const matchedPipes = resources.pipes.filter((p) =>
      localPipeNames.has(p.name)
    );

    if (matchedDatasources.length > 0 || matchedPipes.length > 0) {
      // Generate combined tinybird.ts file
      const content = generateCombinedFile(matchedDatasources, matchedPipes);
      // Ensure directory exists
      const dir = path.dirname(tinybirdFilePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      fs.writeFileSync(tinybirdFilePath, content);
      created.push(`${relativeTinybirdPath} (generated)`);
    }
  } catch (error) {
    console.error(
      `Warning: Failed to generate TypeScript from workspace: ${
        (error as Error).message
      }`
    );
  }
}

/**
 * Prompt user about including existing datafiles
 */
async function promptForExistingDatafiles(
  datafiles: string[]
): Promise<DatafileAction> {
  const datasourceCount = datafiles.filter((f) =>
    f.endsWith(".datasource")
  ).length;
  const pipeCount = datafiles.filter((f) => f.endsWith(".pipe")).length;

  const parts: string[] = [];
  if (datasourceCount > 0) {
    parts.push(
      `${datasourceCount} .datasource file${datasourceCount > 1 ? "s" : ""}`
    );
  }
  if (pipeCount > 0) {
    parts.push(`${pipeCount} .pipe file${pipeCount > 1 ? "s" : ""}`);
  }

  const choice = await p.select({
    message: `Found ${parts.join(" and ")} in your project.`,
    options: [
      {
        value: "include",
        label: "Include existing resources",
        hint: "Add to tinybird.json",
      },
      {
        value: "codegen",
        label: "Define resources in TypeScript",
        hint: "Generate TypeScript definitions from existing resources",
      },
      {
        value: "skip",
        label: "Skip",
        hint: "Don't include existing resources",
      },
    ],
  });

  if (p.isCancel(choice)) {
    return "skip";
  }

  return choice as DatafileAction;
}
