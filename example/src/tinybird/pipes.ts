import { definePipe, node, p } from "@tinybirdco/sdk";

/**
 * Reusable pipe - filter page views by date range
 *
 * This is a reusable pipe that doesn't expose an endpoint.
 * It can be used as a building block for other pipes.
 */
export const filteredPageViews = definePipe("filtered_page_views", {
  description: "Filter page views by date range",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
  },
  nodes: [
    node({
      name: "filtered",
      sql: `
        SELECT *
        FROM page_views
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
      `,
    }),
  ],
});

/**
 * Reusable pipe - filter events by date range
 *
 * This is a reusable pipe that doesn't expose an endpoint.
 * It can be used as a building block for other pipes.
 */
export const filteredEvents = definePipe("filtered_events", {
  description: "Filter events by date range",
  params: {
    start_date: p.dateTime().describe("Start of date range"),
    end_date: p.dateTime().describe("End of date range"),
  },
  nodes: [
    node({
      name: "filtered",
      sql: `
        SELECT *
        FROM events
        WHERE timestamp >= {{DateTime(start_date)}}
          AND timestamp <= {{DateTime(end_date)}}
      `,
    }),
  ],
});
