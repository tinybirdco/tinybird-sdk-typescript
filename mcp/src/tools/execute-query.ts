/**
 * Execute Query Tool
 * Executes SQL queries against Tinybird's /v0/sql endpoint
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

/**
 * Register the execute_query tool
 */
export function registerExecuteQueryTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "execute_query",
    "Execute a SQL query against Tinybird. Use this to query datasources, test SQL, or explore data.",
    {
      query: z.string().describe("The SQL query to execute"),
    },
    async ({ query }) => {
      const url = `${config.baseUrl}/v0/sql?q=${encodeURIComponent(query)}`;

      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${config.token}`,
        },
      });

      if (!response.ok) {
        const errorText = await response.text();
        return {
          content: [
            {
              type: "text" as const,
              text: `Error executing query: ${response.status} ${response.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const result = await response.text();
      return {
        content: [{ type: "text" as const, text: result }],
      };
    }
  );
}
