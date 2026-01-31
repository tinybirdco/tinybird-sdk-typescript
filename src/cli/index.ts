#!/usr/bin/env node
/**
 * Tinybird CLI
 * Commands for building and deploying Tinybird projects
 */

import { Command } from "commander";
import { runInit } from "./commands/init.js";
import { runBuild } from "./commands/build.js";
import { runDev } from "./commands/dev.js";

// Read version from package.json
const VERSION = "0.0.1";

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
    .action(async (options) => {
      console.log("Initializing Tinybird project...\n");

      const result = await runInit({
        force: options.force,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      if (result.created.length > 0) {
        console.log("Created:");
        result.created.forEach((file) => console.log(`  - ${file}`));
      }

      if (result.skipped.length > 0) {
        console.log("\nSkipped (already exists):");
        result.skipped.forEach((file) => console.log(`  - ${file}`));
      }

      console.log("\nDone! Next steps:");
      console.log("  1. Set TINYBIRD_TOKEN environment variable");
      console.log("  2. Edit src/tinybird/schema.ts with your schema");
      console.log("  3. Run 'npx tinybird dev' to start development");
    });

  // Build command
  program
    .command("build")
    .description("Build and push resources to Tinybird")
    .option("--dry-run", "Generate without pushing to API")
    .action(async (options) => {
      console.log(`[${formatTime()}] Building...\n`);

      const result = await runBuild({
        dryRun: options.dryRun,
      });

      if (!result.success) {
        console.error(`Error: ${result.error}`);
        process.exit(1);
      }

      const { build, push } = result;

      if (build) {
        console.log(`Generated ${build.stats.datasourceCount} datasource(s), ${build.stats.pipeCount} pipe(s)`);
      }

      if (options.dryRun) {
        console.log("\n[Dry run] Resources not pushed to API");

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
      } else if (push) {
        if (push.result === "no_changes") {
          console.log("No changes detected - already up to date");
        } else {
          console.log(`Pushed to Tinybird successfully`);
        }
      }

      console.log(`\n[${formatTime()}] Done in ${result.durationMs}ms`);
    });

  // Dev command
  program
    .command("dev")
    .description("Watch for changes and sync with Tinybird")
    .action(async () => {
      console.log(`tinybird dev v${VERSION}`);
      console.log("Loading config from tinybird.json...\n");

      try {
        const controller = await runDev({
          onBuildStart: () => {
            console.log(`[${formatTime()}] Building...`);
          },
          onBuildComplete: (result) => {
            if (!result.success) {
              console.error(`[${formatTime()}] Build failed: ${result.error}`);
              return;
            }

            const { build, push } = result;

            if (build && push) {
              if (push.result === "no_changes") {
                console.log(`[${formatTime()}] No changes detected`);
              } else {
                console.log(
                  `[${formatTime()}] Pushed ${build.stats.datasourceCount} datasource(s), ${build.stats.pipeCount} pipe(s) in ${result.durationMs}ms`
                );
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

  return program;
}

// Run CLI
const program = createCli();
program.parse();
