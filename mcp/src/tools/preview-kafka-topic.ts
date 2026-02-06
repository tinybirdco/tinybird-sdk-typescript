/**
 * Preview Kafka Topic Tool
 * Preview data from a Kafka topic
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

interface KafkaPreviewResponse {
  preview?: {
    data?: Array<Record<string, unknown>>;
    meta?: Array<{ name: string; type: string }>;
  };
  earliestTimestamp?: string;
}

/**
 * Register the preview_kafka_topic tool
 */
export function registerPreviewKafkaTopicTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "preview_kafka_topic",
    "Preview data from a Kafka topic. Shows sample messages and schema. Use list_kafka_topics first to get available topics.",
    {
      connection_id: z
        .string()
        .describe("The ID of the Kafka connection (get this from list_connections)"),
      topic: z.string().describe("The Kafka topic name to preview"),
      group_id: z
        .string()
        .optional()
        .describe(
          "Kafka consumer group ID. If not provided, a unique one will be generated based on the topic name."
        ),
    },
    async ({ connection_id, topic, group_id }) => {
      const effectiveGroupId = group_id ?? `${topic}_${Date.now()}`;

      // Validate the group ID
      const validateUrl = new URL(
        `${config.baseUrl}/v0/connectors/${connection_id}/preview`
      );
      validateUrl.searchParams.set("log", "previewGroup");
      validateUrl.searchParams.set("kafka_group_id", effectiveGroupId);
      validateUrl.searchParams.set("kafka_topic", topic);
      validateUrl.searchParams.set("preview_group", "true");

      const validateResponse = await fetch(validateUrl.toString(), {
        method: "GET",
        headers: {
          Authorization: `Bearer ${config.token}`,
        },
      });

      if (!validateResponse.ok) {
        const errorText = await validateResponse.text();
        return {
          content: [
            {
              type: "text" as const,
              text: `Error validating Kafka group ID: ${validateResponse.status} ${validateResponse.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      // Preview the topic
      const previewUrl = new URL(
        `${config.baseUrl}/v0/connectors/${connection_id}/preview`
      );
      previewUrl.searchParams.set("max_records", "12");
      previewUrl.searchParams.set("preview_activity", "true");
      previewUrl.searchParams.set("preview_earliest_timestamp", "true");
      previewUrl.searchParams.set("kafka_topic", topic);
      previewUrl.searchParams.set("kafka_group_id", effectiveGroupId);
      previewUrl.searchParams.set("log", "previewTopic");

      const previewResponse = await fetch(previewUrl.toString(), {
        method: "GET",
        headers: {
          Authorization: `Bearer ${config.token}`,
        },
      });

      if (!previewResponse.ok) {
        const errorText = await previewResponse.text();
        return {
          content: [
            {
              type: "text" as const,
              text: `Error previewing Kafka topic: ${previewResponse.status} ${previewResponse.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const data = (await previewResponse.json()) as KafkaPreviewResponse;
      const preview = data.preview ?? {};
      const messages = preview.data ?? [];
      const schema = preview.meta ?? [];

      const result = {
        topic,
        group_id: effectiveGroupId,
        earliest_timestamp: data.earliestTimestamp,
        schema: schema.map((col) => ({ name: col.name, type: col.type })),
        message_count: messages.length,
        messages,
      };

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
