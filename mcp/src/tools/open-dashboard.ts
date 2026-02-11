/**
 * Open Dashboard Tool
 * Opens the Tinybird dashboard in the default browser
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

/**
 * Register the open_dashboard tool
 */
export function registerOpenDashboardTool(
  server: McpServer,
  _config: ResolvedConfig
): void {
  server.tool(
    "open_dashboard",
    "Open the Tinybird dashboard in the default browser. Opens the dashboard for the specified environment (cloud, local, or branch).",
    {
      cwd: z
        .string()
        .optional()
        .describe(
          "Working directory containing tinybird.json (defaults to process.cwd())"
        ),
      env: z
        .enum(["cloud", "local", "branch"])
        .describe(
          "Which environment to open: 'cloud' (main workspace), 'local' (localhost), or 'branch' (development branch)"
        ),
    },
    async ({ cwd, env }) => {
      const workingDir = cwd ?? process.cwd();

      try {
        const { runOpenDashboard } = await import(
          "@tinybirdco/sdk/cli/commands/open-dashboard"
        );

        const result = await runOpenDashboard({
          cwd: workingDir,
          environment: env,
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
                  },
                  null,
                  2
                ),
              },
            ],
            isError: true,
          };
        }

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  success: true,
                  url: result.url,
                  environment: result.environment,
                  browserOpened: result.browserOpened,
                  message: result.browserOpened
                    ? `Opened ${result.environment} dashboard in browser`
                    : `Dashboard URL: ${result.url} (browser may not have opened)`,
                },
                null,
                2
              ),
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
