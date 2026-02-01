/**
 * Tinybird Client
 *
 * This file defines the typed Tinybird client for your project.
 * Import your datasources and pipes, create the client, and export types.
 */

import {
  createTinybirdClient,
  type InferRow,
  type InferParams,
  type InferOutputRow,
} from "@tinybirdco/sdk";

// Import datasources
import { pageViews, events } from "./datasources";

// Import materialized views
import { dailyPageStats, dailyPageStatsMv } from "./mvs";

// Import copy pipes
import { dailyStatsSnapshot, dailyStatsCopy, topPagesReport } from "./copies";

// Import pipes
import { filteredEvents, filteredPageViews } from "./pipes";

// Import endpoints
import { dailyStats, pageViewsOverTime, topEvents, topPages } from "./endpoints";

// Create the typed Tinybird client
export const tinybird = createTinybirdClient({
  datasources: {
    pageViews,
    events,
    dailyPageStats,
    dailyStatsSnapshot,
  },
  pipes: {
    filteredEvents,
    filteredPageViews,
    dailyStats,
    pageViewsOverTime,
    topEvents,
    topPages,
    dailyPageStatsMv,
    dailyStatsCopy,
    topPagesReport,
  },
});

// ============================================================================
// Row Types - Inferred from datasource schemas
// ============================================================================

export type PageViewsRow = InferRow<typeof pageViews>;
export type EventsRow = InferRow<typeof events>;
export type DailyPageStatsRow = InferRow<typeof dailyPageStats>;
export type DailyStatsSnapshotRow = InferRow<typeof dailyStatsSnapshot>;

// ============================================================================
// Endpoint Types - Params and Output types for API endpoints
// ============================================================================

// Top Pages endpoint
export type TopPagesParams = InferParams<typeof topPages>;
export type TopPagesOutput = InferOutputRow<typeof topPages>;

// Top Events endpoint
export type TopEventsParams = InferParams<typeof topEvents>;
export type TopEventsOutput = InferOutputRow<typeof topEvents>;

// Page Views Over Time endpoint
export type PageViewsOverTimeParams = InferParams<typeof pageViewsOverTime>;
export type PageViewsOverTimeOutput = InferOutputRow<typeof pageViewsOverTime>;

// Daily Stats endpoint
export type DailyStatsParams = InferParams<typeof dailyStats>;
export type DailyStatsOutput = InferOutputRow<typeof dailyStats>;

// ============================================================================
// Re-exports for convenience
// ============================================================================

export {
  // Datasources
  pageViews,
  events,
  // Materialized views
  dailyPageStats,
  dailyPageStatsMv,
  // Copy pipes
  dailyStatsSnapshot,
  dailyStatsCopy,
  topPagesReport,
  // Pipes
  filteredEvents,
  filteredPageViews,
  // Endpoints
  dailyStats,
  pageViewsOverTime,
  topEvents,
  topPages,
};
