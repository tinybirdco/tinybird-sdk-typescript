#!/usr/bin/env node
/**
 * Tinybird CLI
 * Commands for building and deploying Tinybird projects
 */

import { config } from "dotenv";

// Load .env files in priority order (later files don't override earlier ones)
config({ path: ".env.local" });
config({ path: ".env" });

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { Command } from "commander";
import pc from "picocolors";
import { runInit } from "./commands/init.js";
import { runBuild } from "./commands/build.js";
import { runDeploy } from "./commands/deploy.js";
import { runPreview } from "./commands/preview.js";
import { runDev } from "./commands/dev.js";
import { runLogin } from "./commands/login.js";
import {
  runBranchList,
  runBranchStatus,
  runBranchDelete,
} from "./commands/branch.js";
import { runClear } from "./commands/clear.js";
import { runInfo } from "./commands/info.js";
import { runLogs, LOG_SOURCES, type LogSource, type LogsEnvironment } from "./commands/logs.js";
import { runOpenDashboard, type Environment } from "./commands/open-dashboard.js";
import {
  detectPackageManagerInstallCmd,
  detectPackageManagerRunCmd,
  hasTinybirdSdkDependency,
} from "./utils/package-manager.js";
import type { DevMode } from "./config.js";
import { output, type ResourceChange } from "./output.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageJson = JSON.parse(
  readFileSync(resolve(__dirname, "../../package.json"), "utf-8")
) as { version: string };
const VERSION = packageJson.version;

/**
 * Create and configure the CLI
 */
function createCli(): Command {
  const program = new Command();

  program
    .name("tinybird")
    .description("Tinybird TypeScript SDK CLI")
    .version(VERSION);

  // Init command
  program
    .command("init")
    .description("Initialize a new Tinybird TypeScript project")
    .option("-f, --force", "Overwrite existing files")
    .option("--skip-login", "Skip browser login flow")
    .option("-m, --mode <mode>", "Development mode: 'branch' or 'local'")
    .option("-p, --path <path>", "Path for Tinybird client files")
    .action(async (options) => {
      // Validate mode if provided
      if (options.mode && !["branch", "local"].includes(options.mode)) {
        console.error(
          `Error: Invalid mode '${options.mode}'. Use 'branch' or 'local'.`
        );
        process.exit(1);
      }

      const result = await runInit({
        force: options.force,
        skipLogin: options.skipLogin,
        devMode: options.mode,
        clientPath: options.path,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      // Detect package manager for run command
      const clientPath = result.clientPath ?? "tinybird";
      const sdkCheckDir = result.clientPath
        ? dirname(join(process.cwd(), clientPath))
        : process.cwd();
      const runCmd = detectPackageManagerRunCmd(sdkCheckDir);
      const installCmd = detectPackageManagerInstallCmd(sdkCheckDir);
      const needsInstallStep = !hasTinybirdSdkDependency(sdkCheckDir);

      if (result.loggedIn) {
        console.log(`\nLogged in successfully!`);
        if (result.workspaceName) {
          console.log(`  Workspace: ${result.workspaceName}`);
        }
        if (result.userEmail) {
          console.log(`  User: ${result.userEmail}`);
        }

        if (result.existingDatafiles && result.existingDatafiles.length > 0) {
          console.log(
            `\nAdded ${result.existingDatafiles.length} existing datafile(s) to tinybird.json.`
          );
        }
        console.log("\nNext steps:");
        const steps = [
          needsInstallStep
            ? `Install dependencies with '${installCmd}'`
            : undefined,
          `Edit your schema in ${clientPath}`,
          `Run '${runCmd} tinybird:dev' to start development`,
        ].filter(Boolean);
        steps.forEach((step, index) => {
          console.log(`  ${index + 1}. ${step}`);
        });
      } else if (result.loggedIn === false) {
        console.log("\nLogin was skipped or failed.");
        console.log("\nNext steps:");
        const steps = [
          "Run 'npx tinybird login' to authenticate",
          needsInstallStep
            ? `Install dependencies with '${installCmd}'`
            : undefined,
          `Edit your schema in ${clientPath}`,
          `Run '${runCmd} tinybird:dev' to start development`,
        ].filter(Boolean);
        steps.forEach((step, index) => {
          console.log(`  ${index + 1}. ${step}`);
        });
      } else {
        console.log("\nNext steps:");
        const steps = [
          needsInstallStep
            ? `Install dependencies with '${installCmd}'`
            : undefined,
          `Edit your schema in ${clientPath}`,
          `Run '${runCmd} tinybird:dev' to start development`,
        ].filter(Boolean);
        steps.forEach((step, index) => {
          console.log(`  ${index + 1}. ${step}`);
        });
      }
    });

  // Login command
  program
    .command("login")
    .description("Authenticate with Tinybird via browser")
    .action(async () => {
      console.log("Starting authentication...\n");

      const result = await runLogin();

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      console.log("\nAuthentication successful!");
      if (result.workspaceName) {
        console.log(`  Workspace: ${result.workspaceName}`);
      }
      if (result.userEmail) {
        console.log(`  User: ${result.userEmail}`);
      }
      if (result.baseUrl) {
        console.log(`  API Host: ${result.baseUrl}`);
      }
    });

  // Info command
  program
    .command("info")
    .description("Show information about the current project and workspace")
    .option("--json", "Output as JSON")
    .action(async (options) => {
      const result = await runInfo({ json: options.json });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      if (options.json) {
        // JSON output
        const jsonOutput = {
          cloud: result.cloud,
          local: result.local,
          branch: result.branch,
          project: result.project,
          branches: result.branches,
        };
        console.log(JSON.stringify(jsonOutput, null, 2));
      } else {
        // Human-readable output
        output.showInfo({
          cloud: result.cloud,
          local: result.local,
          branch: result.branch,
          project: result.project,
        });
      }
    });

  // Open command
  program
    .command("open")
    .description("Open the Tinybird dashboard in the default browser")
    .option(
      "-e, --env <env>",
      "Which environment to open: 'cloud', 'local', or 'branch'"
    )
    .action(async (options) => {
      const validEnvs = ["cloud", "local", "branch"];
      if (options.env && !validEnvs.includes(options.env)) {
        console.error(
          `Error: Invalid environment '${options.env}'. Use one of: ${validEnvs.join(", ")}`
        );
        process.exit(1);
      }

      const result = await runOpenDashboard({
        environment: options.env as Environment | undefined,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      console.log(`Opening ${result.environment} dashboard...`);
      if (result.browserOpened) {
        console.log(`Dashboard: ${result.url}`);
      } else {
        console.log(`Could not open browser. Please visit: ${result.url}`);
      }
    });

  // Build command
  program
    .command("build")
    .description("Build and push resources to a Tinybird branch (not main)")
    .option("--dry-run", "Generate without pushing to API")
    .option("--debug", "Show debug output including API requests/responses")
    .option("--local", "Use local Tinybird container")
    .option("--branch", "Use Tinybird cloud with branches")
    .action(async (options) => {
      if (options.debug) {
        process.env.TINYBIRD_DEBUG = "1";
      }

      // Determine devMode override
      let devModeOverride: DevMode | undefined;
      if (options.local) {
        devModeOverride = "local";
      } else if (options.branch) {
        devModeOverride = "branch";
      }

      const result = await runBuild({
        dryRun: options.dryRun,
        devModeOverride,
      });

      const { build, deploy, branchInfo } = result;

      // Show branch info
      if (branchInfo) {
        output.showBranchInfo(branchInfo);
      }

      output.highlight("Building...");

      if (!result.success) {
        // Show detailed errors if available
        if (deploy?.errors && deploy.errors.length > 0) {
          output.showBuildErrors(deploy.errors);
        } else if (result.error) {
          output.error(result.error);
        }
        output.showBuildFailure();
        process.exit(1);
      }

      if (options.dryRun) {
        console.log("\n[Dry run] Resources not deployed to API");

        // Show generated content
        if (build) {
          console.log("\n--- Generated Datasources ---");
          build.resources.datasources.forEach((ds) => {
            console.log(`\n${ds.name}.datasource:`);
            console.log(ds.content);
          });

          console.log("\n--- Generated Pipes ---");
          build.resources.pipes.forEach((pipe) => {
            console.log(`\n${pipe.name}.pipe:`);
            console.log(pipe.content);
          });
        }
        output.showBuildSuccess(result.durationMs);
      } else if (deploy) {
        if (deploy.result === "no_changes") {
          output.showNoChanges();
        } else {
          // Show datasource changes
          if (deploy.datasources) {
            for (const name of deploy.datasources.created) {
              output.showResourceChange(`${name}.datasource`, "created");
            }
            for (const name of deploy.datasources.changed) {
              output.showResourceChange(`${name}.datasource`, "changed");
            }
            for (const name of deploy.datasources.deleted) {
              output.showResourceChange(`${name}.datasource`, "deleted");
            }
          }

          // Show pipe changes
          if (deploy.pipes) {
            for (const name of deploy.pipes.created) {
              output.showResourceChange(`${name}.pipe`, "created");
            }
            for (const name of deploy.pipes.changed) {
              output.showResourceChange(`${name}.pipe`, "changed");
            }
            for (const name of deploy.pipes.deleted) {
              output.showResourceChange(`${name}.pipe`, "deleted");
            }
          }

          output.showBuildSuccess(result.durationMs);
        }
      }
    });

  // Deploy command
  program
    .command("deploy")
    .description("Deploy resources to main Tinybird workspace (production)")
    .option("--dry-run", "Generate without pushing to API")
    .option("--check", "Validate deploy with Tinybird API without applying")
    .option("--debug", "Show debug output including API requests/responses")
    .action(async (options) => {
      if (options.debug) {
        process.env.TINYBIRD_DEBUG = "1";
      }

      output.highlight("Deploying to main workspace...");

      const result = await runDeploy({
        dryRun: options.dryRun,
        check: options.check,
        callbacks: {
          onChanges: (deployChanges) => {
            // Show changes table immediately after deployment is created
            const changes: ResourceChange[] = [];

            for (const name of deployChanges.datasources.created) {
              changes.push({ status: "new", name, type: "datasource" });
            }
            for (const name of deployChanges.datasources.changed) {
              changes.push({ status: "modified", name, type: "datasource" });
            }
            for (const name of deployChanges.datasources.deleted) {
              changes.push({ status: "deleted", name, type: "datasource" });
            }

            for (const name of deployChanges.pipes.created) {
              changes.push({ status: "new", name, type: "pipe" });
            }
            for (const name of deployChanges.pipes.changed) {
              changes.push({ status: "modified", name, type: "pipe" });
            }
            for (const name of deployChanges.pipes.deleted) {
              changes.push({ status: "deleted", name, type: "pipe" });
            }

            for (const name of deployChanges.connections.created) {
              changes.push({ status: "new", name, type: "connection" });
            }
            for (const name of deployChanges.connections.changed) {
              changes.push({ status: "modified", name, type: "connection" });
            }
            for (const name of deployChanges.connections.deleted) {
              changes.push({ status: "deleted", name, type: "connection" });
            }

            output.showChangesTable(changes);
          },
          onWaitingForReady: () => output.showWaitingForDeployment(),
          onDeploymentReady: () => output.showDeploymentReady(),
          onDeploymentLive: (id) => output.showDeploymentLive(id),
          onValidating: () => output.showValidatingDeployment(),
        },
      });

      const { build, deploy } = result;

      if (!result.success) {
        // Show detailed errors if available
        if (deploy?.errors && deploy.errors.length > 0) {
          output.showBuildErrors(deploy.errors);
        } else if (result.error) {
          output.error(result.error);
        }
        output.showDeployFailure();
        process.exit(1);
      }

      if (options.dryRun) {
        console.log("\n[Dry run] Resources not deployed to API");

        // Show generated content
        if (build) {
          console.log("\n--- Generated Datasources ---");
          build.resources.datasources.forEach((ds) => {
            console.log(`\n${ds.name}.datasource:`);
            console.log(ds.content);
          });

          console.log("\n--- Generated Pipes ---");
          build.resources.pipes.forEach((pipe) => {
            console.log(`\n${pipe.name}.pipe:`);
            console.log(pipe.content);
          });
        }
        output.showDeploySuccess(result.durationMs);
      } else if (options.check) {
        console.log("\n[Check] Resources validated with Tinybird API");
        output.showDeploySuccess(result.durationMs);
      } else if (deploy) {
        if (deploy.result === "no_changes") {
          output.showNoChanges();
        } else {
          // Changes table was already shown via onChanges callback
          output.showDeploySuccess(result.durationMs);
        }
      }
    });

  // Preview command
  program
    .command("preview")
    .description("Create a preview branch and deploy resources (for CI/testing)")
    .option("--dry-run", "Generate without creating branch or deploying")
    .option("--check", "Validate deploy with Tinybird API without applying")
    .option("--debug", "Show debug output including API requests/responses")
    .option("--json", "Output JSON instead of human-readable format")
    .option("-n, --name <name>", "Override preview branch name")
    .option("--local", "Use local Tinybird container")
    .action(async (options) => {
      if (options.debug) {
        process.env.TINYBIRD_DEBUG = "1";
      }

      // Determine devMode override
      let devModeOverride: DevMode | undefined;
      if (options.local) {
        devModeOverride = "local";
      }

      if (!options.json) {
        const modeLabel = devModeOverride === "local" ? " (local)" : "";
        console.log(`Creating preview branch${modeLabel}...\n`);
      }

      const result = await runPreview({
        dryRun: options.dryRun,
        check: options.check,
        name: options.name,
        devModeOverride,
      });

      // JSON output mode
      if (options.json) {
        console.log(JSON.stringify(result, null, 2));
        if (!result.success) {
          process.exit(1);
        }
        return;
      }

      // Human-readable output mode
      if (!result.success) {
        // Parse error message for individual errors (one per line)
        const errorLines = result.error?.split("\n") ?? ["Unknown error"];
        for (const line of errorLines) {
          console.log(pc.red(`- ${line}`));
        }
        console.log("");
        console.log(pc.red(`✗ Preview failed`));
        process.exit(1);
      }

      // Success output
      const durationSec = (result.durationMs / 1000).toFixed(1);
      if (result.branch) {
        if (options.dryRun) {
          console.log(pc.green(`✓ Preview branch: ${result.branch.name}`));
          console.log(pc.dim("  (dry run - branch not created)"));
        } else {
          console.log(pc.green(`✓ Preview branch: ${result.branch.name}`));
          console.log(pc.dim(`  ID: ${result.branch.id}`));
          console.log(pc.dim(`  (use --json to get branch token)`));
        }
      }

      if (result.build) {
        console.log(
          pc.green(`✓ Generated ${result.build.datasourceCount} datasource(s), ${result.build.pipeCount} pipe(s)`)
        );
      }

      if (options.dryRun) {
        console.log(pc.dim("\n[Dry run] Resources not deployed to API"));
      } else if (options.check) {
        console.log(pc.green("\n✓ Resources validated with Tinybird API"));
      } else if (result.deploy) {
        if (result.deploy.result === "no_changes") {
          console.log(pc.green("✓ No changes detected - already up to date"));
        } else {
          console.log(pc.green("✓ Deployed to preview branch"));
        }
      }

      console.log("");
      console.log(pc.green(`✓ Preview completed in ${durationSec}s`));
    });

  // Dev command
  program
    .command("dev")
    .description("Watch for changes and sync with Tinybird")
    .option("--local", "Use local Tinybird container")
    .option("--branch", "Use Tinybird cloud with branches")
    .action(async (options) => {
      // Determine devMode override
      let devModeOverride: DevMode | undefined;
      if (options.local) {
        devModeOverride = "local";
      } else if (options.branch) {
        devModeOverride = "branch";
      }

      try {
        const controller = await runDev({
          devModeOverride,
          onLoginComplete: (info) => {
            console.log("\nAuthentication successful!");
            if (info.workspaceName) {
              console.log(`  Workspace: ${info.workspaceName}`);
            }
            if (info.userEmail) {
              console.log(`  User: ${info.userEmail}`);
            }
            console.log("");
          },
          onBranchReady: (info) => {
            if (info.isLocal) {
              output.showBranchInfo({
                gitBranch: info.gitBranch,
                tinybirdBranch: info.localWorkspace?.name ?? null,
                wasCreated: info.wasCreated ?? false,
                dashboardUrl: info.dashboardUrl,
                isLocal: true,
              });
            } else if (info.isMainBranch) {
              console.log("On main branch - deploying to workspace\n");
            } else if (info.gitBranch) {
              output.showBranchInfo({
                gitBranch: info.gitBranch,
                tinybirdBranch: info.tinybirdBranch?.name ?? null,
                wasCreated: info.wasCreated ?? false,
                dashboardUrl: info.dashboardUrl,
                isLocal: false,
              });
            } else {
              console.log("Not in a git repository - deploying to workspace\n");
            }
          },
          onBuildStart: () => {
            output.highlight("Building...");
          },
          onBuildComplete: (result) => {
            if (!result.success) {
              // Show detailed errors if available
              const { deploy } = result;
              if (deploy?.errors && deploy.errors.length > 0) {
                output.showBuildErrors(deploy.errors);
              } else if (result.error) {
                output.error(result.error);
              }
              output.showBuildFailure(true);
              return;
            }

            const { deploy } = result;

            if (deploy) {
              if (deploy.result === "no_changes") {
                output.showNoChanges();
              } else {
                // Show datasource changes
                if (deploy.datasources) {
                  for (const name of deploy.datasources.created) {
                    output.showResourceChange(`${name}.datasource`, "created");
                  }
                  for (const name of deploy.datasources.changed) {
                    output.showResourceChange(`${name}.datasource`, "changed");
                  }
                  for (const name of deploy.datasources.deleted) {
                    output.showResourceChange(`${name}.datasource`, "deleted");
                  }
                }

                // Show pipe changes
                if (deploy.pipes) {
                  for (const name of deploy.pipes.created) {
                    output.showResourceChange(`${name}.pipe`, "created");
                  }
                  for (const name of deploy.pipes.changed) {
                    output.showResourceChange(`${name}.pipe`, "changed");
                  }
                  for (const name of deploy.pipes.deleted) {
                    output.showResourceChange(`${name}.pipe`, "deleted");
                  }
                }

                output.showBuildSuccess(result.durationMs, true);
              }
            }
          },
          onSchemaValidation: (validation) => {
            if (validation.issues.length > 0) {
              output.info("Schema validation:");
              for (const issue of validation.issues) {
                if (issue.type === "error") {
                  output.error(`  ERROR [${issue.pipeName}]: ${issue.message}`);
                } else {
                  output.warning(`  WARN [${issue.pipeName}]: ${issue.message}`);
                }
              }
            }
          },
          onError: (err) => {
            output.error(err.message);
          },
        });

        console.log("Watching for changes... (Ctrl+C to stop)\n");

        // Handle shutdown
        const shutdown = async () => {
          console.log("\nShutting down...");
          await controller.stop();
          process.exit(0);
        };

        process.on("SIGINT", shutdown);
        process.on("SIGTERM", shutdown);

        // Keep process alive
        await new Promise(() => {});
      } catch (error) {
        console.error(`Error: ${(error as Error).message}`);
        process.exit(1);
      }
    });

  // Branch command
  const branchCommand = new Command("branch").description(
    "Manage Tinybird branches"
  );

  branchCommand
    .command("list")
    .description("List all Tinybird branches")
    .action(async () => {
      const result = await runBranchList();

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      if (!result.branches || result.branches.length === 0) {
        console.log("No branches found.");
        return;
      }

      console.log("Branches:");
      result.branches.forEach((branch) => {
        console.log(`  - ${branch.name} (created: ${branch.created_at})`);
      });
    });

  branchCommand
    .command("status")
    .description("Show current branch status")
    .action(async () => {
      const result = await runBranchStatus();

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      console.log("Branch Status:");
      console.log(`  Git branch: ${result.gitBranch ?? "(not in git repo)"}`);
      if (
        result.tinybirdBranchName &&
        result.tinybirdBranchName !== result.gitBranch
      ) {
        console.log(
          `  Tinybird branch name: ${result.tinybirdBranchName} (sanitized)`
        );
      }
      console.log(`  Main branch: ${result.isMainBranch ? "yes" : "no"}`);

      if (result.tinybirdBranch) {
        console.log(`  Tinybird branch: ${result.tinybirdBranch.name}`);
        console.log(`  Branch ID: ${result.tinybirdBranch.id}`);
        console.log(`  Created: ${result.tinybirdBranch.created_at}`);
        if (result.dashboardUrl) {
          console.log(`  Dashboard: ${result.dashboardUrl}`);
        }
      } else if (!result.isMainBranch && result.tinybirdBranchName) {
        console.log("  Tinybird branch: not created yet");
        console.log("  (Run 'npx tinybird dev' to create it)");
      }

      console.log(`  Cached token: ${result.hasCachedToken ? "yes" : "no"}`);
    });

  branchCommand
    .command("delete")
    .description("Delete a Tinybird branch")
    .argument("<name>", "Branch name to delete")
    .action(async (name) => {
      console.log(`Deleting branch '${name}'...`);

      const result = await runBranchDelete(name);

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      console.log(`Branch '${name}' deleted successfully.`);
    });

  program.addCommand(branchCommand);

  // Clear command
  program
    .command("clear")
    .description("Clear the workspace or branch by deleting and recreating it")
    .option("-y, --yes", "Skip confirmation prompt")
    .option("--local", "Use local Tinybird container")
    .option("--branch", "Use Tinybird cloud with branches")
    .action(async (options) => {
      // Determine devMode override
      let devModeOverride: DevMode | undefined;
      if (options.local) {
        devModeOverride = "local";
      } else if (options.branch) {
        devModeOverride = "branch";
      }

      const modeLabel = devModeOverride === "local" ? "local workspace" : "branch";

      // Confirmation prompt unless --yes is passed
      if (!options.yes) {
        const readline = await import("readline");
        const rl = readline.createInterface({
          input: process.stdin,
          output: process.stdout,
        });

        const answer = await new Promise<string>((resolve) => {
          rl.question(
            `Are you sure you want to clear the ${modeLabel}? This will delete all resources. [y/N]: `,
            (ans) => {
              rl.close();
              resolve(ans);
            }
          );
        });

        if (answer.toLowerCase() !== "y" && answer.toLowerCase() !== "yes") {
          console.log("Aborted.");
          return;
        }
      }

      console.log(`Clearing ${modeLabel}...`);

      const result = await runClear({ devModeOverride });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      const typeLabel = result.isLocal ? "Workspace" : "Branch";
      console.log(`${typeLabel} '${result.name}' cleared successfully.`);
    });

  // Logs command
  program
    .command("logs")
    .description("Query Tinybird service logs for observability data")
    .option(
      "-s, --start <time>",
      "Start time (relative: -1h, -30m, -1d, -7d or ISO 8601)",
      "-1h"
    )
    .option(
      "-e, --end <time>",
      "End time (relative or ISO 8601)",
      undefined
    )
    .option(
      "--source <source>",
      `Comma-separated list of sources: ${LOG_SOURCES.join(", ")}`
    )
    .option("-n, --limit <number>", "Maximum rows to return (1-1000)", "100")
    .option(
      "--env <env>",
      "Target environment: cloud (main workspace), local (container), branch (auto-detect), or a specific branch name"
    )
    .option("--json", "Output raw JSON instead of formatted table")
    .action(async (options) => {
      const sources = options.source
        ? (options.source.split(",").map((s: string) => s.trim()) as LogSource[])
        : undefined;

      const limit = parseInt(options.limit, 10);
      if (isNaN(limit) || limit < 1 || limit > 1000) {
        console.error("Error: limit must be between 1 and 1000");
        process.exit(1);
      }

      const environment = options.env as LogsEnvironment | undefined;

      const result = await runLogs({
        startTime: options.start,
        endTime: options.end,
        sources,
        limit,
        environment,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      if (options.json) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }

      // Table rendering helpers (inspired by Vercel CLI)
      interface ColumnDef<T> {
        label: string;
        width?: number | "stretch";
        getValue: (row: T) => string;
        format?: (value: string, row: T) => string;
      }

      function renderTable<T>(columns: ColumnDef<T>[], rows: T[], tableWidth: number): { header: string; rows: string[] } {
        const maxWidths = columns.map((col) => {
          const headerWidth = col.label.length;
          const maxContent = Math.max(headerWidth, ...rows.map((row) => col.getValue(row).length));
          return maxContent;
        });

        const finalWidths: number[] = [];
        let usedWidth = 0;
        let stretchIndex = -1;

        for (let i = 0; i < columns.length; i++) {
          const col = columns[i];
          if (col.width === "stretch") {
            stretchIndex = i;
            finalWidths.push(0);
          } else if (typeof col.width === "number") {
            finalWidths.push(col.width);
            usedWidth += col.width;
          } else {
            finalWidths.push(maxWidths[i]);
            usedWidth += maxWidths[i];
          }
        }

        usedWidth += (columns.length - 1) * 2;

        if (stretchIndex >= 0) {
          finalWidths[stretchIndex] = Math.max(20, tableWidth - usedWidth);
        }

        const pad = (value: string, width: number): string => {
          if (value.length > width) {
            return value.slice(0, width - 1) + "…";
          }
          return value.padEnd(width);
        };

        const headerStr = columns
          .map((col, i) => pad(col.label, finalWidths[i]))
          .join("  ");

        const formattedRows = rows.map((row) => {
          return columns
            .map((col, i) => {
              const value = col.getValue(row);
              const padded = pad(value, finalWidths[i]);
              return col.format ? col.format(padded, row) : padded;
            })
            .join("  ");
        });

        return { header: pc.dim(headerStr), rows: formattedRows };
      }

      // Parse data JSON to extract key info as key=value pairs
      function extractDataSummary(dataStr: string, _source: string): string {
        try {
          const data = JSON.parse(dataStr) as Record<string, unknown>;

          // Fields to exclude (timestamps and metadata already shown elsewhere)
          const excludeFields = new Set([
            "start_datetime", "timestamp", "created_at", "start_time",
            "end_datetime", "end_time", "date", "source",
          ]);

          // Get all meaningful fields, excluding timestamps
          const allKeys = Object.keys(data).filter((key) => !excludeFields.has(key));

          // Build key=value pairs for all available fields
          const pairs: string[] = [];
          for (const key of allKeys) {
            const value = data[key];
            if (value !== undefined && value !== null && value !== "" && value !== 0) {
              // Format the value (truncate strings, format numbers)
              let formatted: string;
              if (typeof value === "string") {
                formatted = value.length > 25 ? value.slice(0, 22) + "..." : value;
              } else if (typeof value === "number") {
                // Format large numbers with K/M suffix
                if (value >= 1000000) {
                  formatted = (value / 1000000).toFixed(1) + "M";
                } else if (value >= 1000) {
                  formatted = (value / 1000).toFixed(1) + "K";
                } else {
                  formatted = String(value);
                }
              } else {
                formatted = String(value);
              }
              pairs.push(`${key}=${formatted}`);
            }
          }

          return pairs.join(" ") || dataStr.slice(0, 80);
        } catch {
          return dataStr.slice(0, 80);
        }
      }

      // Human-readable output
      if (!result.data || result.data.length === 0) {
        const envLabel = result.environment?.isLocal
          ? `local:${result.environment.target}`
          : result.environment?.target ?? "cloud";
        console.log(pc.dim(`No logs found for the specified time range.`));
        console.log(pc.dim(`\nQueried ${result.query?.sources.length} source(s) from ${envLabel} in ${(result.durationMs / 1000).toFixed(1)}s`));
        return;
      }

      const terminalWidth = process.stdout.columns || 120;

      type RowData = {
        time: string;
        source: string;
        summary: string;
      };

      const rowData: RowData[] = result.data.map((entry) => {
        const date = new Date(entry.timestamp);
        const time = date.toISOString().slice(11, 19); // HH:mm:ss
        return {
          time,
          source: entry.source,
          summary: extractDataSummary(entry.data, entry.source),
        };
      });

      const columns: ColumnDef<RowData>[] = [
        {
          label: "TIME",
          width: 8,
          getValue: (row) => row.time,
          format: (v) => pc.dim(v),
        },
        {
          label: "SOURCE",
          width: 20,
          getValue: (row) => row.source,
        },
        {
          label: "DETAILS",
          width: "stretch",
          getValue: (row) => row.summary,
          format: (v) => pc.dim(v),
        },
      ];

      const table = renderTable(columns, rowData, terminalWidth);

      console.log(table.header);
      for (const row of table.rows) {
        console.log(row);
      }

      // Show environment info and fetch summary
      const envLabel = result.environment?.isLocal
        ? `local:${result.environment.target}`
        : result.environment?.target ?? "cloud";
      console.log(pc.dim(`\nFetched ${result.rows} logs from ${envLabel} in ${(result.durationMs / 1000).toFixed(1)}s`));
    });

  return program;
}

// Run CLI
const program = createCli();
program.parse();
