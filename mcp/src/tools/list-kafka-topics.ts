/**
 * List Kafka Topics Tool
 * Lists available topics from a Kafka connection
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

interface KafkaTopicsResponse {
  preview: Array<{ topic: string }>;
}

/**
 * Register the list_kafka_topics tool
 */
export function registerListKafkaTopicsTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "list_kafka_topics",
    "List available topics from a Kafka connection. Use list_connections first to get the connection ID.",
    {
      connection_id: z
        .string()
        .describe("The ID of the Kafka connection (get this from list_connections)"),
    },
    async ({ connection_id }) => {
      const url = `${config.baseUrl}/v0/connectors/${connection_id}/preview?preview_activity=false`;
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
              text: `Error listing Kafka topics: ${response.status} ${response.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const data = (await response.json()) as KafkaTopicsResponse;
      const topics = data.preview.map((x) => x.topic);

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify({ topics }, null, 2),
          },
        ],
      };
    }
  );
}
