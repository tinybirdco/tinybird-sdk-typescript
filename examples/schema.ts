/**
 * Example Tinybird project schema
 * Demonstrates the full SDK usage pattern
 */

import {
  defineDatasource,
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

// ============ Pipes ============

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

// ============ Project ============

/**
 * Main project definition
 * Aggregates all datasources and pipes
 */
export default defineProject({
  datasources: {
    events,
    users,
  },
  pipes: {
    topEvents,
    userActivity,
  },
});

// ============ Type Inference Examples ============

// Infer row types from datasources
export type EventRow = InferRow<typeof events>;
// { timestamp: Date; event_id: string; user_id: string; event_type: string; ... }

export type UserRow = InferRow<typeof users>;
// { user_id: string; email: string; name: string | null; ... }

// Infer parameter types from pipes
export type TopEventsParams = InferParams<typeof topEvents>;
// { start_date: string; end_date: string; limit?: number; event_type_filter?: string }

// Infer output types from pipes
export type TopEventsOutput = InferOutput<typeof topEvents>;
// { event_type: string; event_count: number; unique_users: number }[]
