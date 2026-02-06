/**
 * List Connections Tool
 * Lists all connections/connectors in the workspace
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

export interface Connection {
  id: string;
  service: string;
  name: string;
  connected_datasources: string;
  [key: string]: unknown;
}

interface ConnectorsResponse {
  connectors: Array<{
    id: string;
    service: string;
    name: string;
    linkers: Array<{ datasource_id: string }>;
    settings: Record<string, unknown>;
  }>;
}

/**
 * Register the list_connections tool
 */
export function registerListConnectionsTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "list_connections",
    "List all connections in the workspace. Returns connection ID, name, service type, and connected datasources.",
    {
      service: z
        .string()
        .optional()
        .describe("Filter by service type (e.g., 'kafka', 's3', 's3_iamrole')"),
    },
    async ({ service }) => {
      const url = `${config.baseUrl}/v0/connectors`;
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
              text: `Error listing connections: ${response.status} ${response.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const data = (await response.json()) as ConnectorsResponse;
      const connections: Connection[] = [];

      for (const c of data.connectors) {
        if (service && c.service !== service) {
          continue;
        }

        connections.push({
          id: c.id,
          service: c.service,
          name: c.name,
          connected_datasources: String(c.linkers?.length ?? 0),
          ...c.settings,
        });
      }

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(connections, null, 2),
          },
        ],
      };
    }
  );
}
