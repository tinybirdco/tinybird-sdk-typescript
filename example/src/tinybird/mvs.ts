import { defineDatasource, defineMaterializedView, node, t, engine } from "@tinybird/sdk";

/**
 * Daily page stats - target datasource for materialized view
 * Pre-aggregates page view counts by day and pathname for fast queries
 */
export const dailyPageStats = defineDatasource("daily_page_stats", {
  description: "Pre-aggregated daily page view statistics",
  schema: {
    date: t.date(),
    pathname: t.string(),
    views: t.simpleAggregateFunction("sum", t.uint64()),
    unique_sessions: t.simpleAggregateFunction("uniq", t.uint64()),
  },
  engine: engine.aggregatingMergeTree({
    sortingKey: ["date", "pathname"],
    partitionKey: "toYYYYMM(date)",
  }),
});

/**
 * Materialized view that pre-aggregates daily page stats
 * Data flows: page_views -> daily_page_stats_mv -> daily_page_stats
 */
export const dailyPageStatsMv = defineMaterializedView("daily_page_stats_mv", {
  description: "Materialize daily page view aggregations",
  target_datasource: dailyPageStats,
  nodes: [
    node({
      name: "aggregate",
      sql: `
        SELECT
          toDate(timestamp) AS date,
          pathname,
          count() AS views,
          uniqState(session_id) AS unique_sessions
        FROM page_views
        GROUP BY date, pathname
      `,
    }),
  ],
});
