/**
 * List Branches Tool
 * Lists all branches (environments) in the workspace
 */

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

interface Branch {
  id: string;
  name: string;
  created_at: string;
}

interface BranchesResponse {
  environments: Branch[];
}

/**
 * Register the list_branches tool
 */
export function registerListBranchesTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "list_branches",
    "List all branches (environments) in the Tinybird workspace. Use branch names with the 'environment' parameter in other tools.",
    {},
    async () => {
      const url = new URL("/v1/environments", config.baseUrl);

      const response = await fetch(url.toString(), {
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
              text: `Error listing branches: ${response.status} ${response.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const data = (await response.json()) as BranchesResponse;
      const branches = data.environments ?? [];

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(branches, null, 2),
          },
        ],
      };
    }
  );
}
