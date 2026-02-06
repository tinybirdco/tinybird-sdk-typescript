/**
 * Get Resource Tool
 * Fetches the full datafile content of a resource
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

type ResourceType = "datasource" | "pipe" | "connection";

/**
 * Build the API URL for fetching a resource's datafile content
 */
function buildResourceUrl(
  baseUrl: string,
  type: ResourceType,
  name: string
): string {
  const encodedName = encodeURIComponent(name);
  switch (type) {
    case "datasource":
      return `${baseUrl}/v0/datasources/${encodedName}.datasource`;
    case "pipe":
      return `${baseUrl}/v1/pipes/${encodedName}.pipe`;
    case "connection":
      return `${baseUrl}/v0/connectors/${encodedName}.connection`;
  }
}

/**
 * Register the get_resource tool
 */
export function registerGetResourceTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "get_resource",
    "Get the full datafile content of a resource. Use list_resources first to find resource names.",
    {
      name: z.string().describe("The name of the resource to fetch"),
      type: z
        .enum(["datasource", "pipe", "connection"])
        .describe("The type of resource: 'datasource', 'pipe', or 'connection'"),
    },
    async ({ name, type }) => {
      const url = buildResourceUrl(config.baseUrl, type, name);

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
              text: `Error fetching ${type} '${name}': ${response.status} ${response.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const content = await response.text();
      return {
        content: [
          {
            type: "text" as const,
            text: content,
          },
        ],
      };
    }
  );
}
