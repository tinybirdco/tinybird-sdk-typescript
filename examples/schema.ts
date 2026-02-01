/**
 * Example Tinybird project schema
 * Demonstrates the full SDK usage pattern
 */

import {
  defineDatasource,
  defineMaterializedView,
  InferMaterializedTarget,
  definePipe,
  defineProject,
  node,
  t,
  p,
  engine,
  type InferRow,
  type InferParams,
  type InferOutput,
} from "../src/index.js";

// ============ Datasources ============

/**
 * Events datasource - tracks user events
 */
export const events = defineDatasource("events", {
  description: "User event tracking data",
  schema: {
    timestamp: t.dateTime(),
    event_id: t.uuid(),
    user_id: t.string(),
    event_type: t.string().lowCardinality(),
    properties: t.json<{ page?: string; action?: string }>(),
    session_id: t.string().nullable(),
    app_version: t.string().lowCardinality(),
  },
  engine: engine.mergeTree({
    sortingKey: ["user_id", "timestamp"],
    partitionKey: "toYYYYMM(timestamp)",
    ttl: "timestamp + INTERVAL 90 DAY",
  }),
});

/**
 * Users datasource - user profiles
 */
export const users = defineDatasource("users", {
  description: "User profile data",
  schema: {
    user_id: t.string(),
    email: t.string(),
    name: t.string().nullable(),
    created_at: t.dateTime(),
    updated_at: t.dateTime(),
    plan: t.string().lowCardinality(),
  },
  engine: engine.replacingMergeTree({
    sortingKey: ["user_id"],
    ver: "updated_at",
  }),
});

/**
 * Events Daily Stats Target Datasource
 */

export const events_daily_stats = defineDatasource("events_daily_stats", {
  description: "Daily stats for events",
  schema: {
    day: t.date(),
    event_type: t.string(),
    count: t.simpleAggregateFunction('sum', t.uint64()),
  },
  engine: engine.aggregatingMergeTree({
    sortingKey: ["day", "event_type"],
  })
});

// ============ Pipes ============
/**
 * Events Daily Stats Materialized View
 */
export const events_daily_stats_mv = defineMaterializedView("events_daily_stats_mv", {
  description: "Daily stats for events",
  datasource: events_daily_stats,
  nodes: [
    node({
      name: "daily_stats",
      sql: `
        SELECT toStartOfDay(timestamp) as day, event_type, count() as count
        FROM events
        GROUP BY day, event_type
      `,
    }),
  ],
});

/**
 * Top events pipe - get top events by count
 */
export const topEvents = definePipe("top_events", {
  description: "Get top events by count within a date range",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
    limit: p.int32().optional(10).describe("Maximum number of results"),
    event_type_filter: p.string().optional().describe("Filter by event type"),
  },
  nodes: [
    node({
      name: "filtered_events",
      description: "Filter events by date range and optional type",
      sql: `
        SELECT *
        FROM events
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
          {% if defined(event_type_filter) %}
          AND event_type = {{String(event_type_filter)}}
          {% end %}
      `,
    }),
    node({
      name: "aggregated",
      description: "Aggregate by event type",
      sql: `
        SELECT
          event_type,
          count() AS event_count,
          uniqExact(user_id) AS unique_users
        FROM filtered_events
        GROUP BY event_type
        ORDER BY event_count DESC
        LIMIT {{Int32(limit, 10)}}
      `,
    }),
  ],
  output: {
    event_type: t.string(),
    event_count: t.uint64(),
    unique_users: t.uint64(),
  },
  endpoint: true,
});

/**
 * User activity pipe - get activity for a specific user
 */
export const userActivity = definePipe("user_activity", {
  description: "Get recent activity for a specific user",
  params: {
    user_id: p.string().describe("User ID to query"),
    limit: p.int32().optional(50).describe("Maximum events to return"),
  },
  nodes: [
    node({
      name: "user_events",
      sql: `
        SELECT
          timestamp,
          event_type,
          properties
        FROM events
        WHERE user_id = {{String(user_id)}}
        ORDER BY timestamp DESC
        LIMIT {{Int32(limit, 50)}}
      `,
    }),
  ],
  output: {
    timestamp: t.dateTime(),
    event_type: t.string(),
    properties: t.json(),
  },
  endpoint: true,
});

export const events_by_day_and_type = definePipe("events_by_day_and_type", {
  description: "Get events by day and type",
  params: {
    start_date: p.date().optional().describe("From date"),
    end_date: p.date().optional().describe("To date"),
    event_type: p.string().optional().describe("Filter by event type"),
  },
  nodes: [
    node({
      name: "events_by_day_and_type_node",
      sql: `
        SELECT *
        FROM events_daily_stats
        WHERE 1=1 
        {% if defined(start_date) %} 
        AND day >= {{Date(start_date)}} 
        {% end %}
        {% if defined(end_date) %} 
        AND day <= {{Date(end_date)}} 
        {% end %}
        {% if defined(event_type) %} 
        AND event_type = {{String(event_type)}} 
        {% end %}
      `,
    }),
  ],
  output: {
    day: t.date(),
    event_type: t.string(),
    count: t.uint64(),
  },
  endpoint: true,
});

// ============ Project ============

/**
 * Main project definition
 * Aggregates all datasources and pipes
 */
export default defineProject({
  datasources: {
    events,
    users,
    events_daily_stats,
  },
  pipes: {
    topEvents,
    userActivity,
    events_daily_stats_mv,
    events_by_day_and_type,
  },
});

// ============ Type Inference Examples ============

// Infer row types from datasources
export type EventRow = InferRow<typeof events>;
// { timestamp: Date; event_id: string; user_id: string; event_type: string; ... }

export type UserRow = InferRow<typeof users>;
// { user_id: string; email: string; name: string | null; ... }

export type EventsDailyStatsMV = InferMaterializedTarget<typeof events_daily_stats_mv>;
// { day: Date; event_type: string; count: number }

export type EventsDailyStatsRow = InferRow<typeof events_daily_stats>;
// { day: Date; event_type: string; count: number }

// Infer parameter types from pipes
export type TopEventsParams = InferParams<typeof topEvents>;
// { start_date: string; end_date: string; limit?: number; event_type_filter?: string }

// Infer output types from pipes
export type TopEventsOutput = InferOutput<typeof topEvents>;
// { event_type: string; event_count: number; unique_users: number }[]
