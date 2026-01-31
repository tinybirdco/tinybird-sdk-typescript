import { defineProject } from "@tinybird/sdk";
import { pageViews, events } from "./datasources";
import { topPages, pageViewsOverTime, topEvents } from "./pipes";

/**
 * Main Tinybird project schema
 * This aggregates all datasources and pipes for the analytics project
 */
export default defineProject({
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

// Re-export for convenience
export { pageViews, events } from "./datasources";
export { topPages, pageViewsOverTime, topEvents } from "./pipes";
