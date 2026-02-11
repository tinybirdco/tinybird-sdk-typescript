/**
 * Logs Command
 * Query Tinybird service datasources for observability data
 */

import { loadConfig, LOCAL_BASE_URL, type ResolvedConfig } from "../config.js";
import { getBranch } from "../../api/branches.js";
import {
  getLocalTokens,
  getOrCreateLocalWorkspace,
  getLocalWorkspaceName,
  LocalNotRunningError,
} from "../../api/local.js";
import { getWorkspace } from "../../api/workspaces.js";

/**
 * Available log sources (Tinybird service datasources)
 */
export const LOG_SOURCES = [
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

export type LogSource = (typeof LOG_SOURCES)[number];

/**
 * Mapping of datasource to its timestamp column name
 */
const TIMESTAMP_COLUMNS: Record<LogSource, string> = {
  "tinybird.pipe_stats_rt": "start_datetime",
  "tinybird.bi_stats_rt": "start_datetime",
  "tinybird.block_log": "timestamp",
  "tinybird.datasources_ops_log": "timestamp",
  "tinybird.endpoint_errors": "start_datetime",
  "tinybird.kafka_ops_log": "timestamp",
  "tinybird.sinks_ops_log": "timestamp",
  "tinybird.jobs_log": "created_at",
  "tinybird.llm_usage": "start_time",
};

/**
 * Environment target for logs query
 * - "cloud": Main workspace (explicit production)
 * - "local": Local Tinybird container
 * - "branch": Auto-detect from git branch
 * - string: Specific branch name
 */
export type LogsEnvironment = "cloud" | "local" | "branch" | string;

export interface LogsOptions {
  cwd?: string;
  startTime?: string;
  endTime?: string;
  sources?: LogSource[];
  limit?: number;
  /** Environment target override */
  environment?: LogsEnvironment;
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
  /** Resolved environment info */
  environment?: {
    /** Target type: cloud, local, or branch name */
    target: string;
    /** Whether this is local mode */
    isLocal: boolean;
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
 * Uses formatRowNoNewline to get JSON with column names
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
      formatRowNoNewline('JSONEachRow', *) AS data
    FROM ${source}
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
 * Resolve the effective token and baseUrl based on environment option
 */
async function resolveEnvironment(
  config: ResolvedConfig,
  environment?: LogsEnvironment
): Promise<{
  token: string;
  baseUrl: string;
  target: string;
  isLocal: boolean;
}> {
  // If explicitly cloud, use main workspace
  if (environment === "cloud") {
    return {
      token: config.token,
      baseUrl: config.baseUrl,
      target: "cloud",
      isLocal: false,
    };
  }

  // If explicitly local or devMode is local (and not overridden)
  if (environment === "local" || (!environment && config.devMode === "local")) {
    const localTokens = await getLocalTokens();

    // Determine workspace name
    let workspaceName: string;
    if (config.isMainBranch || !config.tinybirdBranch) {
      // On main branch: use the authenticated workspace name
      const authenticatedWorkspace = await getWorkspace({
        baseUrl: config.baseUrl,
        token: config.token,
      });
      workspaceName = authenticatedWorkspace.name;
    } else {
      // On feature branch: use branch name
      workspaceName = getLocalWorkspaceName(config.tinybirdBranch, config.cwd);
    }

    const { workspace } = await getOrCreateLocalWorkspace(localTokens, workspaceName);

    return {
      token: workspace.token,
      baseUrl: LOCAL_BASE_URL,
      target: workspaceName,
      isLocal: true,
    };
  }

  // Determine branch name
  let branchName: string | null = null;
  if (environment === "branch" || !environment) {
    // Auto-detect from git branch
    branchName = config.tinybirdBranch;
  } else if (environment) {
    // Explicit branch name provided
    branchName = environment;
  }

  // If we have a branch name and we're not on main, use the branch
  if (branchName && !config.isMainBranch) {
    try {
      const branch = await getBranch(
        { baseUrl: config.baseUrl, token: config.token },
        branchName
      );

      if (branch.token) {
        return {
          token: branch.token,
          baseUrl: config.baseUrl,
          target: branchName,
          isLocal: false,
        };
      }
    } catch (error) {
      // Branch doesn't exist - fall back to main workspace
      // This is acceptable for logs as they might want to query before branch exists
    }
  }

  // Fall back to main workspace
  return {
    token: config.token,
    baseUrl: config.baseUrl,
    target: "cloud",
    isLocal: false,
  };
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

  // Resolve environment (token and baseUrl)
  let envConfig: { token: string; baseUrl: string; target: string; isLocal: boolean };
  try {
    envConfig = await resolveEnvironment(config, options.environment);
  } catch (error) {
    if (error instanceof LocalNotRunningError) {
      return {
        success: false,
        error: error.message,
        durationMs: Date.now() - startTime,
      };
    }
    return {
      success: false,
      error: `Failed to resolve environment: ${(error as Error).message}`,
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

  const url = `${envConfig.baseUrl}/v0/sql?q=${encodeURIComponent(query)}`;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${envConfig.token}`,
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
      environment: {
        target: envConfig.target,
        isLocal: envConfig.isLocal,
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
