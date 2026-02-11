/**
 * Info Tool
 * Returns information about the current Tinybird project and workspace
 */

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

interface WorkspaceInfo {
  id: string;
  name: string;
  user_id: string;
  user_email: string;
  scope: string;
  /** If not null, this workspace is a branch of the main workspace */
  main: string | null;
}

interface Branch {
  id: string;
  name: string;
  created_at: string;
}

interface BranchesResponse {
  environments: Branch[];
}

/**
 * Register the get_info tool
 */
export function registerInfoTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "get_info",
    "Get information about the current Tinybird project and workspace. Returns workspace details (name, ID, user), API configuration, and available branches.",
    {},
    async () => {
      // Get workspace info
      const workspaceUrl = new URL("/v1/workspace", config.baseUrl);
      const workspaceResponse = await fetch(workspaceUrl.toString(), {
        method: "GET",
        headers: {
          Authorization: `Bearer ${config.token}`,
        },
      });

      if (!workspaceResponse.ok) {
        const errorText = await workspaceResponse.text();
        return {
          content: [
            {
              type: "text" as const,
              text: `Error getting workspace info: ${workspaceResponse.status} ${workspaceResponse.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const workspace = (await workspaceResponse.json()) as WorkspaceInfo;

      // Get branches
      const branchesUrl = new URL("/v1/environments", config.baseUrl);
      let branches: Branch[] = [];
      try {
        const branchesResponse = await fetch(branchesUrl.toString(), {
          method: "GET",
          headers: {
            Authorization: `Bearer ${config.token}`,
          },
        });

        if (branchesResponse.ok) {
          const data = (await branchesResponse.json()) as BranchesResponse;
          branches = data.environments ?? [];
        }
      } catch {
        // Branches are optional, continue without them
      }

      // Derive UI host from API host
      let uiHost = config.baseUrl;
      try {
        const url = new URL(config.baseUrl);
        url.hostname = url.hostname.replace(/^api\./, "app.");
        uiHost = url.origin;
      } catch {
        uiHost = config.baseUrl.replace(/^api\./, "app.");
      }

      // Check if we're currently in a branch context (workspace.main is not null)
      // and try to get the current branch details
      let currentBranch: { name: string; id: string; createdAt: string } | undefined;
      if (workspace.main !== null) {
        // We're in a branch - find the matching branch info
        const matchingBranch = branches.find((b) => b.id === workspace.id);
        if (matchingBranch) {
          currentBranch = {
            name: matchingBranch.name,
            id: matchingBranch.id,
            createdAt: matchingBranch.created_at,
          };
        }
      }

      // Build result
      const result: Record<string, unknown> = {
        workspace: {
          name: workspace.name,
          id: workspace.id,
          userEmail: workspace.user_email,
        },
        api: {
          host: config.baseUrl,
          uiHost,
        },
        branches: branches.map((b) => ({
          name: b.name,
          id: b.id,
          createdAt: b.created_at,
        })),
      };

      // Add current branch if we're in a branch context
      if (currentBranch) {
        result.currentBranch = currentBranch;
      }

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(result, null, 2),
          },
        ],
      };
    }
  );
}
