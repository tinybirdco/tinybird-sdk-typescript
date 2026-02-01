import { defineProject, type InferRow, type InferParams, type InferOutputRow } from "@tinybird/sdk";
import { pageViews, events } from "./datasources";
import { topPages, pageViewsOverTime, topEvents } from "./pipes";

/**
 * Main Tinybird project schema
 * This aggregates all datasources and pipes for the analytics project
 */
const project = defineProject({
  datasources: {
    pageViews,
    events,
  },
  pipes: {
    topPages,
    pageViewsOverTime,
    topEvents,
  },
});

export default project;

// Export the typed client from the project
export const { tinybird } = project;

// Re-export for convenience
export { pageViews, events } from "./datasources";
export { topPages, pageViewsOverTime, topEvents } from "./pipes";

// Inferred types from schema definitions
export type PageViewsRow = InferRow<typeof pageViews>;
export type EventsRow = InferRow<typeof events>;
export type TopPagesParams = InferParams<typeof topPages>;
export type TopPagesOutput = InferOutputRow<typeof topPages>;
export type PageViewsOverTimeParams = InferParams<typeof pageViewsOverTime>;
export type PageViewsOverTimeOutput = InferOutputRow<typeof pageViewsOverTime>;
export type TopEventsParams = InferParams<typeof topEvents>;
export type TopEventsOutput = InferOutputRow<typeof topEvents>;
