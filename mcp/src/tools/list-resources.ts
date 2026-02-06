/**
 * List Resources Tool
 * Lists all resources (datasources, pipes, connections) in the workspace
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

type ResourceType = "datasource" | "pipe" | "connection";

interface Resource {
  name: string;
  type: ResourceType;
  definition: string;
}

interface DatasourcesResponse {
  datasources: Array<{
    name: string;
    [key: string]: unknown;
  }>;
}

interface PipesResponse {
  pipes: Array<{
    name: string;
    [key: string]: unknown;
  }>;
}

interface ConnectorsResponse {
  connectors: Array<{
    id: string;
    name: string;
    service: string;
    [key: string]: unknown;
  }>;
}

interface DatafileResponse {
  text?: string;
  content?: string;
}

async function fetchDatafile(
  config: ResolvedConfig,
  type: "datasources" | "pipes" | "connections",
  name: string
): Promise<string> {
  const url = `${config.baseUrl}/v0/${type}/${encodeURIComponent(name)}?format=datafile`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${config.token}`,
    },
  });

  if (!response.ok) {
    return `# Error fetching ${type}/${name}: ${response.status} ${response.statusText}`;
  }

  const data = (await response.json()) as DatafileResponse;
  return data.text ?? data.content ?? "";
}

/**
 * Register the list_resources tool
 */
export function registerListResourcesTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "list_resources",
    "List all resources in the workspace. Returns resource names and their full datafile definitions. Filter by type: 'datasource', 'pipe', or 'connection'.",
    {
      type: z
        .enum(["datasource", "pipe", "connection"])
        .optional()
        .describe("Filter by resource type: 'datasource', 'pipe', or 'connection'"),
    },
    async ({ type }) => {
      const resources: Resource[] = [];
      const types: ResourceType[] = type
        ? [type]
        : ["datasource", "pipe", "connection"];

      // Fetch datasources
      if (types.includes("datasource")) {
        const url = `${config.baseUrl}/v0/datasources`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${config.token}`,
          },
        });

        if (response.ok) {
          const data = (await response.json()) as DatasourcesResponse;
          for (const ds of data.datasources) {
            const definition = await fetchDatafile(
              config,
              "datasources",
              ds.name
            );
            resources.push({
              name: ds.name,
              type: "datasource",
              definition,
            });
          }
        }
      }

      // Fetch pipes
      if (types.includes("pipe")) {
        const url = `${config.baseUrl}/v0/pipes`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${config.token}`,
          },
        });

        if (response.ok) {
          const data = (await response.json()) as PipesResponse;
          for (const pipe of data.pipes) {
            const definition = await fetchDatafile(config, "pipes", pipe.name);
            resources.push({
              name: pipe.name,
              type: "pipe",
              definition,
            });
          }
        }
      }

      // Fetch connections
      if (types.includes("connection")) {
        const url = `${config.baseUrl}/v0/connectors`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${config.token}`,
          },
        });

        if (response.ok) {
          const data = (await response.json()) as ConnectorsResponse;
          for (const conn of data.connectors) {
            const definition = await fetchDatafile(
              config,
              "connections",
              conn.name
            );
            resources.push({
              name: conn.name,
              type: "connection",
              definition,
            });
          }
        }
      }

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(resources, null, 2),
          },
        ],
      };
    }
  );
}
