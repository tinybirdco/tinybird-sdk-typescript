/**
 * Example client usage
 * Demonstrates how to use the TinybirdClient
 */

import { createClient, type InferRow, type InferParams, type InferOutput } from "../src/index.js";
import { events, topEvents } from "./schema.js";

// Type aliases for convenience
type EventRow = InferRow<typeof events>;
type TopEventsParams = InferParams<typeof topEvents>;
type TopEventsOutput = InferOutput<typeof topEvents>;

// Create client
const client = createClient({
  baseUrl: process.env.TINYBIRD_URL ?? "https://api.tinybird.co",
  token: process.env.TINYBIRD_TOKEN ?? "",
});

async function main() {
  // ============ Query Example ============

  // Query the top_events pipe
  const result = await client.query<TopEventsOutput[number]>("top_events", {
    start_date: "2024-01-01 00:00:00",
    end_date: "2024-01-31 23:59:59",
    limit: 5,
  } satisfies TopEventsParams);

  console.log("Top Events:");
  for (const row of result.data) {
    console.log(`  ${row.event_type}: ${row.event_count} events, ${row.unique_users} unique users`);
  }

  console.log(`\nQuery stats: ${result.statistics.rows_read} rows read in ${result.statistics.elapsed}s`);

  // ============ Ingest Example ============

  // Ingest a single event (type-safe)
  const event: EventRow = {
    timestamp: new Date(),
    event_id: crypto.randomUUID(),
    user_id: "user_123",
    event_type: "page_view",
    properties: { page: "/home", action: "scroll" },
    session_id: "session_456",
    app_version: "2.0.0",
  };

  await client.ingest("events", event);
  console.log("\nIngested event:", event.event_id);

  // Batch ingest
  const batch: EventRow[] = [
    {
      timestamp: new Date(),
      event_id: crypto.randomUUID(),
      user_id: "user_123",
      event_type: "click",
      properties: { action: "button_click" },
      session_id: "session_456",
      app_version: "2.0.0",
    },
    {
      timestamp: new Date(),
      event_id: crypto.randomUUID(),
      user_id: "user_456",
      event_type: "page_view",
      properties: { page: "/dashboard" },
      session_id: "session_789",
      app_version: "2.0.0",
    },
  ];

  const ingestResult = await client.ingestBatch("events", batch);
  console.log(`Ingested batch: ${ingestResult.successful_rows} successful, ${ingestResult.quarantined_rows} quarantined`);

  // ============ Raw SQL Example ============

  const sqlResult = await client.sql<{ count: number }>(
    "SELECT count() as count FROM events"
  );
  console.log(`\nTotal events: ${sqlResult.data[0]?.count}`);
}

main().catch(console.error);
