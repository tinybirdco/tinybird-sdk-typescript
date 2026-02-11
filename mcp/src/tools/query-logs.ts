/**
 * Query Logs Tool
 * Queries Tinybird service datasources for unified observability data
 */

import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ResolvedConfig } from "../config.js";

/**
 * Available log sources (Tinybird service datasources)
 */
const LOG_SOURCES = [
  "pipe_stats_rt",
  "bi_stats_rt",
  "block_log",
  "datasources_ops_log",
  "endpoint_errors",
  "kafka_ops_log",
  "sinks_ops_log",
  "jobs_log",
  "llm_usage",
] as const;

type LogSource = (typeof LOG_SOURCES)[number];

/**
 * Mapping of datasource to its timestamp column name
 */
const TIMESTAMP_COLUMNS: Record<LogSource, string> = {
  pipe_stats_rt: "start_datetime",
  bi_stats_rt: "start_datetime",
  block_log: "timestamp",
  datasources_ops_log: "timestamp",
  endpoint_errors: "start_datetime",
  kafka_ops_log: "timestamp",
  sinks_ops_log: "timestamp",
  jobs_log: "created_at",
  llm_usage: "start_time",
};

/**
 * Parse relative time string to ISO datetime
 * Supports: -1h, -30m, -1d, -7d, -1w, etc.
 */
function parseRelativeTime(
  relativeTime: string,
  now: Date = new Date()
): string {
  const match = relativeTime.match(/^-(\d+)([mhdw])$/);
  if (!match) {
    return relativeTime;
  }

  const [, amount, unit] = match;
  const ms = now.getTime();
  const value = parseInt(amount, 10);

  let offsetMs: number;
  switch (unit) {
    case "m":
      offsetMs = value * 60 * 1000;
      break;
    case "h":
      offsetMs = value * 60 * 60 * 1000;
      break;
    case "d":
      offsetMs = value * 24 * 60 * 60 * 1000;
      break;
    case "w":
      offsetMs = value * 7 * 24 * 60 * 60 * 1000;
      break;
    default:
      offsetMs = 0;
  }

  return new Date(ms - offsetMs).toISOString();
}

/**
 * Build SQL query for a single source
 */
function buildSourceQuery(
  source: LogSource,
  startTime: string,
  endTime: string
): string {
  const tsCol = TIMESTAMP_COLUMNS[source];

  return `
    SELECT
      '${source}' AS source,
      ${tsCol} AS timestamp,
      toJSONString(tuple(*)) AS data
    FROM tinybird.${source}
    WHERE ${tsCol} >= parseDateTimeBestEffort('${startTime}')
      AND ${tsCol} < parseDateTimeBestEffort('${endTime}')`;
}

/**
 * Build the complete UNION ALL query
 */
function buildQuery(
  sources: readonly LogSource[],
  startTime: string,
  endTime: string,
  limit: number
): string {
  const sourceQueries = sources.map((source) =>
    buildSourceQuery(source, startTime, endTime)
  );

  return `
SELECT * FROM (
${sourceQueries.join("\n  UNION ALL\n")}
)
ORDER BY timestamp DESC
LIMIT ${limit}
FORMAT JSON`;
}

/**
 * Register the query_logs tool
 */
export function registerQueryLogsTool(
  server: McpServer,
  config: ResolvedConfig
): void {
  server.tool(
    "query_logs",
    `Query Tinybird service logs for observability data. Returns unified logs from multiple sources: ${LOG_SOURCES.join(", ")}. Use this to debug API calls, data ingestion, query execution, and errors.`,
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
      sources: z
        .array(z.enum(LOG_SOURCES))
        .optional()
        .describe(
          `Filter by log sources. Available: ${LOG_SOURCES.join(", ")}. Default: all sources`
        ),
      limit: z
        .number()
        .min(1)
        .max(1000)
        .optional()
        .describe("Maximum rows to return (1-1000). Default: 100"),
    },
    async ({ start_time, end_time, sources, limit }) => {
      const now = new Date();

      const resolvedStartTime = parseRelativeTime(start_time ?? "-1h", now);
      const resolvedEndTime = end_time
        ? parseRelativeTime(end_time, now)
        : now.toISOString();
      const resolvedSources = sources?.length ? sources : LOG_SOURCES;
      const resolvedLimit = limit ?? 100;

      const query = buildQuery(
        resolvedSources,
        resolvedStartTime,
        resolvedEndTime,
        resolvedLimit
      );

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
              text: `Error querying logs: ${response.status} ${response.statusText}\n${errorText}`,
            },
          ],
          isError: true,
        };
      }

      const result = await response.text();

      try {
        const jsonResult = JSON.parse(result);
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  query: {
                    start_time: resolvedStartTime,
                    end_time: resolvedEndTime,
                    sources: resolvedSources,
                    limit: resolvedLimit,
                  },
                  statistics: jsonResult.statistics ?? {},
                  rows: jsonResult.rows ?? jsonResult.data?.length ?? 0,
                  data: jsonResult.data ?? [],
                },
                null,
                2
              ),
            },
          ],
        };
      } catch {
        return {
          content: [{ type: "text" as const, text: result }],
        };
      }
    }
  );
}
