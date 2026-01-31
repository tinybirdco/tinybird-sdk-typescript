import { defineDatasource, t, engine } from "@tinybird/sdk";

/**
 * Page views datasource - tracks page view events
 */
export const pageViews = defineDatasource("page_views", {
  description: "Page view tracking data",
  schema: {
    timestamp: t.dateTime(),
    session_id: t.string(),
    user_id: t.string().nullable(),
    pathname: t.string(),
    referrer: t.string().nullable(),
    user_agent: t.string(),
    country: t.string().lowCardinality().nullable(),
    device_type: t.string().lowCardinality(),
  },
  engine: engine.mergeTree({
    sortingKey: ["pathname", "timestamp"],
    partitionKey: "toYYYYMM(timestamp)",
    ttl: "timestamp + INTERVAL 90 DAY",
  }),
});

/**
 * Events datasource - tracks custom events
 */
export const events = defineDatasource("events", {
  description: "Custom event tracking data",
  schema: {
    timestamp: t.dateTime(),
    session_id: t.string(),
    user_id: t.string().nullable(),
    event_name: t.string().lowCardinality(),
    properties: t.json<Record<string, unknown>>(),
  },
  engine: engine.mergeTree({
    sortingKey: ["event_name", "timestamp"],
    partitionKey: "toYYYYMM(timestamp)",
  }),
});
