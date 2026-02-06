/**
 * List Resources Tool
 * Lists all resources (datasources, pipes, connections) in the workspace
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";
import { resolveEnvironmentConfig } from "../config.js";

type ResourceType = "datasource" | "pipe" | "connection";

interface Resource {
  id: string;
  name: string;
  type: ResourceType;
  description?: string;
}

interface DatasourcesResponse {
  datasources: Array<{
    id: string;
    name: string;
    description?: string;
    [key: string]: unknown;
  }>;
}

interface PipesResponse {
  pipes: Array<{
    id: string;
    name: string;
    description?: string;
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

/**
 * Register the list_resources tool
 */
export function registerListResourcesTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "list_resources",
    "List all resources in the workspace. Returns resource id, name, description, and type. Filter by type: 'datasource', 'pipe', or 'connection'. Use get_resource to fetch full datafile content.",
    {
      type: z
        .enum(["datasource", "pipe", "connection"])
        .optional()
        .describe("Filter by resource type: 'datasource', 'pipe', or 'connection'"),
      environment: z
        .string()
        .optional()
        .describe("Environment to query: 'cloud' (default), 'local', or a branch name"),
    },
    async ({ type, environment }) => {
      // Resolve the effective config based on environment
      const effectiveConfig = await resolveEnvironmentConfig(config, environment);

      const resources: Resource[] = [];
      const types: ResourceType[] = type
        ? [type]
        : ["datasource", "pipe", "connection"];

      // Fetch datasources
      if (types.includes("datasource")) {
        const url = `${effectiveConfig.baseUrl}/v0/datasources`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${effectiveConfig.token}`,
          },
        });

        if (response.ok) {
          const data = (await response.json()) as DatasourcesResponse;
          for (const ds of data.datasources) {
            resources.push({
              id: ds.id,
              name: ds.name,
              type: "datasource",
              description: ds.description,
            });
          }
        }
      }

      // Fetch pipes
      if (types.includes("pipe")) {
        const url = `${effectiveConfig.baseUrl}/v0/pipes`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${effectiveConfig.token}`,
          },
        });

        if (response.ok) {
          const data = (await response.json()) as PipesResponse;
          for (const pipe of data.pipes) {
            resources.push({
              id: pipe.id,
              name: pipe.name,
              type: "pipe",
              description: pipe.description,
            });
          }
        }
      }

      // Fetch connections
      if (types.includes("connection")) {
        const url = `${effectiveConfig.baseUrl}/v0/connectors`;
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${effectiveConfig.token}`,
          },
        });

        if (response.ok) {
          const data = (await response.json()) as ConnectorsResponse;
          for (const conn of data.connectors) {
            resources.push({
              id: conn.id,
              name: conn.name,
              type: "connection",
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
