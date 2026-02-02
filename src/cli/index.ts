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
import { dirname, resolve } from "node:path";
import { Command } from "commander";
import { runInit } from "./commands/init.js";
import { runBuild } from "./commands/build.js";
import { runDev } from "./commands/dev.js";
import { runLogin } from "./commands/login.js";
import {
  runBranchList,
  runBranchStatus,
  runBranchDelete,
} from "./commands/branch.js";
import type { DevMode } from "./config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageJson = JSON.parse(
  readFileSync(resolve(__dirname, "../../package.json"), "utf-8")
) as { version: string };
const VERSION = packageJson.version;

/**
 * Format timestamp for console output
 */
function formatTime(): string {
  return new Date().toLocaleTimeString("en-US", { hour12: false });
}

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
    .action(async (options) => {
      // Validate mode if provided
      if (options.mode && !["branch", "local"].includes(options.mode)) {
        console.error(`Error: Invalid mode '${options.mode}'. Use 'branch' or 'local'.`);
        process.exit(1);
      }

      const result = await runInit({
        force: options.force,
        skipLogin: options.skipLogin,
        devMode: options.mode,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      if (result.created.length > 0) {
        console.log("Created:");
        result.created.forEach((file) => {
          console.log(`  - ${file}`);
        });
      }

      if (result.skipped.length > 0) {
        console.log("\nSkipped (already exists):");
        result.skipped.forEach((file) => {
          console.log(`  - ${file}`);
        });
      }

      if (result.loggedIn) {
        console.log(`\nLogged in successfully!`);
        if (result.workspaceName) {
          console.log(`  Workspace: ${result.workspaceName}`);
        }
        if (result.userEmail) {
          console.log(`  User: ${result.userEmail}`);
        }
        console.log("\nDone! Next steps:");
        console.log("  1. Edit src/tinybird/schema.ts with your schema");
        console.log("  2. Run 'npx tinybird dev' to start development");
      } else if (result.loggedIn === false) {
        console.log("\nLogin was skipped or failed.");
        console.log("\nDone! Next steps:");
        console.log("  1. Run 'npx tinybird login' to authenticate");
        console.log("  2. Edit src/tinybird/schema.ts with your schema");
        console.log("  3. Run 'npx tinybird dev' to start development");
      } else {
        console.log("\nDone! Next steps:");
        console.log("  1. Edit src/tinybird/schema.ts with your schema");
        console.log("  2. Run 'npx tinybird dev' to start development");
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

  // Build command
  program
    .command("build")
    .description("Build and push resources to Tinybird")
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

      const modeLabel = devModeOverride === "local" ? " (local)" : "";
      console.log(`[${formatTime()}] Building${modeLabel}...\n`);

      const result = await runBuild({
        dryRun: options.dryRun,
        devModeOverride,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      const { build, deploy } = result;

      if (build) {
        console.log(`Generated ${build.stats.datasourceCount} datasource(s), ${build.stats.pipeCount} pipe(s)`);
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
      } else if (deploy) {
        if (deploy.result === "no_changes") {
          console.log("No changes detected - already up to date");
        } else {
          console.log(`Deployed to Tinybird successfully`);
        }
      }

      console.log(`\n[${formatTime()}] Done in ${result.durationMs}ms`);
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

      console.log(`tinybird dev v${VERSION}`);
      console.log("Loading config from tinybird.json...\n");

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
              // Local mode
              const workspaceName = info.localWorkspace?.name ?? "unknown";
              if (info.wasCreated) {
                console.log(`Using local Tinybird container`);
                console.log(`Creating local workspace '${workspaceName}'...`);
                console.log("Workspace created.\n");
              } else {
                console.log(`Using local Tinybird container`);
                console.log(`Using existing local workspace '${workspaceName}'\n`);
              }
            } else if (info.isMainBranch) {
              console.log("On main branch - deploying to workspace\n");
            } else if (info.gitBranch) {
              const tinybirdName = info.tinybirdBranch?.name ?? info.gitBranch;
              if (info.wasCreated) {
                console.log(`Detected git branch: ${info.gitBranch}`);
                console.log(`Creating Tinybird branch '${tinybirdName}'...`);
                console.log("Branch created and token cached.\n");
              } else {
                console.log(`Detected git branch: ${info.gitBranch}`);
                console.log(`Using existing Tinybird branch '${tinybirdName}'\n`);
              }
            } else {
              console.log("Not in a git repository - deploying to workspace\n");
            }
          },
          onBuildStart: () => {
            console.log(`[${formatTime()}] Building...`);
          },
          onBuildComplete: (result) => {
            if (!result.success) {
              console.error(`[${formatTime()}] Build failed: ${result.error}`);
              return;
            }

            const { deploy } = result;

            if (deploy) {
              if (deploy.result === "no_changes") {
                console.log(`[${formatTime()}] No changes detected`);
              } else {
                console.log(`[${formatTime()}] Built in ${result.durationMs}ms`);

                // Show datasource changes
                if (deploy.datasources) {
                  for (const name of deploy.datasources.created) {
                    console.log(`  + datasource ${name} (created)`);
                  }
                  for (const name of deploy.datasources.changed) {
                    console.log(`  ~ datasource ${name} (changed)`);
                  }
                  for (const name of deploy.datasources.deleted) {
                    console.log(`  - datasource ${name} (deleted)`);
                  }
                }

                // Show pipe changes
                if (deploy.pipes) {
                  for (const name of deploy.pipes.created) {
                    console.log(`  + pipe ${name} (created)`);
                  }
                  for (const name of deploy.pipes.changed) {
                    console.log(`  ~ pipe ${name} (changed)`);
                  }
                  for (const name of deploy.pipes.deleted) {
                    console.log(`  - pipe ${name} (deleted)`);
                  }
                }
              }
            }
          },
          onSchemaValidation: (validation) => {
            if (validation.issues.length > 0) {
              console.log(`[${formatTime()}] Schema validation:`);
              for (const issue of validation.issues) {
                if (issue.type === "error") {
                  console.error(`  ERROR [${issue.pipeName}]: ${issue.message}`);
                } else {
                  console.warn(`  WARN [${issue.pipeName}]: ${issue.message}`);
                }
              }
            }
          },
          onError: (error) => {
            console.error(`[${formatTime()}] Error: ${error.message}`);
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
  const branchCommand = new Command("branch")
    .description("Manage Tinybird branches");

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
      if (result.tinybirdBranchName && result.tinybirdBranchName !== result.gitBranch) {
        console.log(`  Tinybird branch name: ${result.tinybirdBranchName} (sanitized)`);
      }
      console.log(`  Main branch: ${result.isMainBranch ? "yes" : "no"}`);

      if (result.tinybirdBranch) {
        console.log(`  Tinybird branch: ${result.tinybirdBranch.name}`);
        console.log(`  Branch ID: ${result.tinybirdBranch.id}`);
        console.log(`  Created: ${result.tinybirdBranch.created_at}`);
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

  return program;
}

// Run CLI
const program = createCli();
program.parse();
