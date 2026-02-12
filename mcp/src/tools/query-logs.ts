/**
 * Query Logs Tool
 * Queries Tinybird service datasources for unified observability data
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

/**
 * Available log sources (Tinybird service datasources)
 * Must match the SDK's LOG_SOURCES
 */
const LOG_SOURCES = [
  "tinybird.pipe_stats_rt",
  "tinybird.bi_stats_rt",
  "tinybird.block_log",
  "tinybird.datasources_ops_log",
  "tinybird.endpoint_errors",
  "tinybird.kafka_ops_log",
  "tinybird.sinks_ops_log",
  "tinybird.jobs_log",
  "tinybird.llm_usage",
] as const;

/**
 * Register the query_logs tool
 */
export function registerQueryLogsTool(
  server: McpServer,
  _config: ResolvedConfig
): void {
  server.tool(
    "query_logs",
    `Query Tinybird service logs for observability data. Returns unified logs from multiple sources: ${LOG_SOURCES.join(
      ", "
    )}. Use this to debug API calls, data ingestion, query execution, and errors.`,
    {
      start_time: z
        .string()
        .optional()
        .describe(
          "Start time. Supports relative times like '-1h', '-30m', '-1d', '-7d' or ISO 8601 datetime. Default: -1h"
        ),
      end_time: z
        .string()
        .optional()
        .describe(
          "End time. Supports relative times or ISO 8601 datetime. Default: now"
        ),
      source: z
        .array(z.enum(LOG_SOURCES))
        .optional()
        .describe(
          `Filter by source. Available: ${LOG_SOURCES.join(
            ", "
          )}. Default: all sources`
        ),
      limit: z
        .number()
        .min(1)
        .max(1000)
        .optional()
        .describe("Maximum rows to return (1-1000). Default: 100"),
      environment: z
        .string()
        .optional()
        .describe(
          "Target environment: 'cloud' (main workspace), 'local' (container), 'branch' (auto-detect), or a specific branch name"
        ),
    },
    async ({ start_time, end_time, source, limit, environment }) => {
      try {
        const { runLogs } = await import("@tinybirdco/sdk/cli/commands/logs");

        const result = await runLogs({
          startTime: start_time,
          endTime: end_time,
          sources: source,
          limit,
          environment,
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
                    durationMs: result.durationMs,
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
                  query: result.query,
                  environment: result.environment,
                  statistics: result.statistics,
                  rows: result.rows,
                  data: result.data,
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
