/**
 * Login Tool
 * Authenticates with Tinybird via browser OAuth flow
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

/**
 * Register the login tool
 */
export function registerLoginTool(
  server: McpServer,
  _config: ResolvedConfig
): void {
  server.tool(
    "login",
    "Authenticate with Tinybird via browser OAuth flow. Opens browser for user to login. Returns workspace info on success.",
    {
      cwd: z
        .string()
        .optional()
        .describe("Working directory containing tinybird.json (defaults to process.cwd())"),
      api_host: z
        .string()
        .optional()
        .describe("API host/region override (e.g., 'https://api.us-east.tinybird.co')"),
    },
    async ({ cwd, api_host }) => {
      const workingDir = cwd ?? process.cwd();

      try {
        const { runLogin } = await import("@tinybirdco/sdk/cli/commands/login");

        const result = await runLogin({
          cwd: workingDir,
          apiHost: api_host,
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
                    suggestion: "Ensure browser is available and complete the OAuth flow",
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
                  workspaceName: result.workspaceName,
                  userEmail: result.userEmail,
                  baseUrl: result.baseUrl,
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
                  suggestion: "Run 'npx tinybird login' in terminal if browser flow fails",
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
