/**
 * Build Tool
 * Builds and deploys Tinybird resources from TypeScript definitions
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

/**
 * Register the build tool
 */
export function registerBuildTool(
  server: McpServer,
  _config: ResolvedConfig
): void {
  server.tool(
    "build",
    "Build and deploy Tinybird resources (datasources, pipes) from TypeScript definitions. Builds to a development branch, not main. Use dry_run to preview changes without deploying.",
    {
      cwd: z
        .string()
        .optional()
        .describe("Working directory containing tinybird.json (defaults to process.cwd())"),
      dry_run: z
        .boolean()
        .optional()
        .describe("If true, generate resources but don't push to Tinybird API"),
      dev_mode: z
        .enum(["branch", "local"])
        .optional()
        .describe("Override devMode: 'branch' (Tinybird cloud) or 'local' (localhost:7181)"),
    },
    async ({ cwd, dry_run, dev_mode }) => {
      const workingDir = cwd ?? process.cwd();

      try {
        const { runBuild } = await import("@tinybirdco/sdk/cli/commands/build");

        const result = await runBuild({
          cwd: workingDir,
          dryRun: dry_run ?? false,
          devModeOverride: dev_mode,
        });

        if (!result.success) {
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify(
                  {
                    success: false,
                    error: result.error,
                    durationMs: result.durationMs,
                  },
                  null,
                  2
                ),
              },
            ],
            isError: true,
          };
        }

        // Format successful response
        const response: Record<string, unknown> = {
          success: true,
          durationMs: result.durationMs,
        };

        if (result.build) {
          response.resources = {
            datasources: result.build.resources.datasources.length,
            pipes: result.build.resources.pipes.length,
            connections: result.build.resources.connections.length,
          };
        }

        if (result.branchInfo) {
          response.branch = {
            gitBranch: result.branchInfo.gitBranch,
            tinybirdBranch: result.branchInfo.tinybirdBranch,
            wasCreated: result.branchInfo.wasCreated,
            dashboardUrl: result.branchInfo.dashboardUrl,
            isLocal: result.branchInfo.isLocal,
          };
        }

        if (result.deploy) {
          response.deploy = {
            success: result.deploy.success,
            result: result.deploy.result,
            datasourceCount: result.deploy.datasourceCount,
            pipeCount: result.deploy.pipeCount,
            connectionCount: result.deploy.connectionCount,
            buildId: result.deploy.buildId,
            pipes: result.deploy.pipes,
            datasources: result.deploy.datasources,
          };
        }

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(response, null, 2),
            },
          ],
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  success: false,
                  error: (error as Error).message,
                },
                null,
                2
              ),
            },
          ],
          isError: true,
        };
      }
    }
  );
}
