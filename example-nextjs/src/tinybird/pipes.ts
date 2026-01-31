import { definePipe, node, p, t } from "@tinybird/sdk";

/**
 * Top pages pipe - get most visited pages
 */
export const topPages = definePipe("top_pages", {
  description: "Get the most visited pages",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
    limit: p.int32().optional(10).describe("Number of results"),
  },
  nodes: [
    node({
      name: "aggregated",
      sql: `
        SELECT
          pathname,
          count() AS views,
          uniqExact(session_id) AS unique_sessions
        FROM page_views
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
        GROUP BY pathname
        ORDER BY views DESC
        LIMIT {{Int32(limit, 10)}}
      `,
    }),
  ],
  output: {
    pathname: t.string(),
    views: t.uint64(),
    unique_sessions: t.uint64(),
  },
  endpoint: true,
});

/**
 * Page views over time - get page views aggregated by time period
 */
export const pageViewsOverTime = definePipe("page_views_over_time", {
  description: "Get page views over time",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
    granularity: p.string().optional("hour").describe("Time granularity (hour, day, week)"),
  },
  nodes: [
    node({
      name: "time_series",
      sql: `
        SELECT
          toStartOfHour(timestamp) AS time_bucket,
          count() AS views,
          uniqExact(session_id) AS unique_sessions
        FROM page_views
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
        GROUP BY time_bucket
        ORDER BY time_bucket ASC
      `,
    }),
  ],
  output: {
    time_bucket: t.dateTime(),
    views: t.uint64(),
    unique_sessions: t.uint64(),
  },
  endpoint: true,
});

/**
 * Top events pipe - get most frequent events
 */
export const topEvents = definePipe("top_events", {
  description: "Get the most frequent events",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
    limit: p.int32().optional(10).describe("Number of results"),
  },
  nodes: [
    node({
      name: "aggregated",
      sql: `
        SELECT
          event_name,
          count() AS event_count,
          uniqExact(session_id) AS unique_sessions
        FROM events
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT {{Int32(limit, 10)}}
      `,
    }),
  ],
  output: {
    event_name: t.string(),
    event_count: t.uint64(),
    unique_sessions: t.uint64(),
  },
  endpoint: true,
});
