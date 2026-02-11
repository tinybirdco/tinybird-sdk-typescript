/**
 * Logs Command
 * Query Tinybird service datasources for observability data
 */

import { loadConfig, type ResolvedConfig } from "../config.js";

/**
 * Available log sources (Tinybird service datasources)
 */
export const LOG_SOURCES = [
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

export type LogSource = (typeof LOG_SOURCES)[number];

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

export interface LogsOptions {
  cwd?: string;
  startTime?: string;
  endTime?: string;
  sources?: LogSource[];
  limit?: number;
}

export interface LogsResult {
  success: boolean;
  error?: string;
  durationMs: number;
  query?: {
    startTime: string;
    endTime: string;
    sources: readonly LogSource[];
    limit: number;
  };
  statistics?: Record<string, unknown>;
  rows?: number;
  data?: Array<{
    source: string;
    timestamp: string;
    data: string;
  }>;
}

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
 * Run the logs command
 */
export async function runLogs(options: LogsOptions = {}): Promise<LogsResult> {
  const startTime = Date.now();
  const cwd = options.cwd ?? process.cwd();

  let config: ResolvedConfig;
  try {
    config = loadConfig(cwd);
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      durationMs: Date.now() - startTime,
    };
  }

  const now = new Date();

  const resolvedStartTime = parseRelativeTime(options.startTime ?? "-1h", now);
  const resolvedEndTime = options.endTime
    ? parseRelativeTime(options.endTime, now)
    : now.toISOString();
  const resolvedSources = options.sources?.length ? options.sources : LOG_SOURCES;
  const resolvedLimit = options.limit ?? 100;

  const query = buildQuery(
    resolvedSources,
    resolvedStartTime,
    resolvedEndTime,
    resolvedLimit
  );

  const url = `${config.baseUrl}/v0/sql?q=${encodeURIComponent(query)}`;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${config.token}`,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      return {
        success: false,
        error: `${response.status} ${response.statusText}\n${errorText}`,
        durationMs: Date.now() - startTime,
      };
    }

    const result = await response.text();
    const jsonResult = JSON.parse(result);

    return {
      success: true,
      durationMs: Date.now() - startTime,
      query: {
        startTime: resolvedStartTime,
        endTime: resolvedEndTime,
        sources: resolvedSources,
        limit: resolvedLimit,
      },
      statistics: jsonResult.statistics ?? {},
      rows: jsonResult.rows ?? jsonResult.data?.length ?? 0,
      data: jsonResult.data ?? [],
    };
  } catch (error) {
    return {
      success: false,
      error: (error as Error).message,
      durationMs: Date.now() - startTime,
    };
  }
}
