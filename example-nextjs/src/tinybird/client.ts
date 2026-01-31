import {
  createClient,
  TinybirdClient,
  type InferRow,
  type InferParams,
  type InferOutput,
} from "@tinybird/sdk";
import { pageViews, events, topPages, pageViewsOverTime, topEvents } from "./schema";

// ============ Inferred Types ============

// Row types for ingestion
export type PageViewRow = InferRow<typeof pageViews>;
export type EventRow = InferRow<typeof events>;

// Param types for queries
export type TopPagesParams = InferParams<typeof topPages>;
export type PageViewsOverTimeParams = InferParams<typeof pageViewsOverTime>;
export type TopEventsParams = InferParams<typeof topEvents>;

// Output types for queries
export type TopPagesOutput = InferOutput<typeof topPages>;
export type PageViewsOverTimeOutput = InferOutput<typeof pageViewsOverTime>;
export type TopEventsOutput = InferOutput<typeof topEvents>;

// ============ Client ============

// Lazy-initialize client to avoid build-time errors when env vars aren't set
let _client: TinybirdClient | null = null;

function getClient(): TinybirdClient {
  if (!_client) {
    _client = createClient({
      baseUrl: process.env.TINYBIRD_URL ?? "https://api.tinybird.co",
      token: process.env.TINYBIRD_TOKEN ?? "",
    });
  }
  return _client;
}

// ============ Type-Safe API ============

/**
 * Typed Tinybird API
 * Provides type-safe methods for querying pipes and ingesting events
 */
export const tinybird = {
  // Queries
  query: {
    topPages: (params: TopPagesParams) =>
      getClient().query<TopPagesOutput[number]>("top_pages", params),

    pageViewsOverTime: (params: PageViewsOverTimeParams) =>
      getClient().query<PageViewsOverTimeOutput[number]>("page_views_over_time", params),

    topEvents: (params: TopEventsParams) =>
      getClient().query<TopEventsOutput[number]>("top_events", params),
  },

  // Ingestion
  ingest: {
    pageView: (event: PageViewRow) =>
      getClient().ingest("page_views", event),

    pageViews: (events: PageViewRow[]) =>
      getClient().ingestBatch("page_views", events),

    event: (event: EventRow) =>
      getClient().ingest("events", event),

    events: (events: EventRow[]) =>
      getClient().ingestBatch("events", events),
  },

  // Raw client access if needed
  get raw() {
    return getClient();
  },
};
