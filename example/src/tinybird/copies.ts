import {
  defineDatasource,
  defineCopyPipe,
  node,
  t,
  engine,
} from "@tinybird/sdk";

/**
 * Daily stats snapshot - target datasource for copy pipe
 * Stores daily snapshots of page statistics
 */
export const dailyStatsSnapshot = defineDatasource("daily_stats_snapshot", {
  description: "Daily snapshots of page statistics",
  schema: {
    snapshot_date: t.date(),
    pathname: t.string(),
    total_views: t.uint64(),
    total_unique_sessions: t.uint64(),
  },
  engine: engine.mergeTree({
    sortingKey: ["snapshot_date", "pathname"],
    partitionKey: "toYYYYMM(snapshot_date)",
  }),
});

/**
 * Copy pipe that creates daily snapshots of page stats
 * Runs daily at midnight UTC to capture yesterday's data
 */
export const dailyStatsCopy = defineCopyPipe("daily_stats_copy", {
  description: "Daily snapshot of page statistics",
  target_datasource: dailyStatsSnapshot,
  copy_schedule: "0 0 * * *", // Daily at midnight UTC
  copy_mode: "append",
  nodes: [
    node({
      name: "snapshot",
      sql: `
        SELECT
          today() - 1 AS snapshot_date,
          pathname,
          count() AS total_views,
          uniqExact(session_id) AS total_unique_sessions
        FROM page_views
        WHERE toDate(timestamp) = today() - 1
        GROUP BY pathname
      `,
    }),
  ],
});

/**
 * On-demand copy pipe for creating ad-hoc reports
 * Must be triggered manually via API or CLI
 */
export const topPagesReport = defineCopyPipe("top_pages_report", {
  description: "On-demand report of top pages",
  target_datasource: dailyStatsSnapshot,
  copy_mode: "replace", // Replace all data on each run
  // No copy_schedule means it defaults to @on-demand
  nodes: [
    node({
      name: "report",
      sql: `
        SELECT
          toDate({{DateTime(job_timestamp, now())}}) AS snapshot_date,
          pathname,
          count() AS total_views,
          uniqExact(session_id) AS total_unique_sessions
        FROM page_views
        WHERE timestamp >= {{DateTime(job_timestamp, now())}} - interval 7 day
        GROUP BY pathname
        ORDER BY total_views DESC
        LIMIT 100
      `,
    }),
  ],
});
