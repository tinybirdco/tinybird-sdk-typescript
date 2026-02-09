import { describe, it, expect } from "vitest";
import {
  generateDatasourceCode,
  generatePipeCode,
  generateDatasourcesFile,
  generatePipesFile,
  generateClientFile,
  generateAllFiles,
} from "./index.js";
import type { DatasourceInfo, PipeInfo } from "../api/resources.js";

describe("generateDatasourceCode", () => {
  it("generates valid datasource code", () => {
    const ds: DatasourceInfo = {
      name: "page_views",
      description: "Page view tracking data",
      columns: [
        { name: "timestamp", type: "DateTime" },
        { name: "pathname", type: "String" },
        { name: "session_id", type: "String" },
      ],
      engine: {
        type: "MergeTree",
        sorting_key: "pathname, timestamp",
      },
    };

    const code = generateDatasourceCode(ds);

    expect(code).toContain('export const pageViews = defineDatasource("page_views"');
    expect(code).toContain("timestamp: t.dateTime()");
    expect(code).toContain("pathname: t.string()");
    expect(code).toContain("session_id: t.string()");
    expect(code).toContain("engine.mergeTree");
    expect(code).toContain("export type PageViewsRow = InferRow<typeof pageViews>");
  });

  it("handles nullable columns", () => {
    const ds: DatasourceInfo = {
      name: "events",
      columns: [
        { name: "user_id", type: "Nullable(String)" },
      ],
      engine: { type: "MergeTree", sorting_key: "user_id" },
    };

    const code = generateDatasourceCode(ds);
    expect(code).toContain("user_id: t.string().nullable()");
  });

  it("handles LowCardinality columns", () => {
    const ds: DatasourceInfo = {
      name: "events",
      columns: [
        { name: "country", type: "LowCardinality(String)" },
      ],
      engine: { type: "MergeTree", sorting_key: "country" },
    };

    const code = generateDatasourceCode(ds);
    expect(code).toContain("country: t.string().lowCardinality()");
  });

  it("includes description in JSDoc", () => {
    const ds: DatasourceInfo = {
      name: "events",
      description: "Event tracking data",
      columns: [{ name: "id", type: "String" }],
      engine: { type: "MergeTree", sorting_key: "id" },
    };

    const code = generateDatasourceCode(ds);
    expect(code).toContain("/**");
    expect(code).toContain(" * Event tracking data");
    expect(code).toContain(" */");
  });

  it("includes forward query when present", () => {
    const ds: DatasourceInfo = {
      name: "events",
      columns: [{ name: "id", type: "String" }],
      engine: { type: "MergeTree", sorting_key: "id" },
      forward_query: "SELECT id",
    };

    const code = generateDatasourceCode(ds);
    expect(code).toContain("forwardQuery: `SELECT id`");
  });
});

describe("generatePipeCode", () => {
  it("generates endpoint code", () => {
    const pipe: PipeInfo = {
      name: "top_pages",
      description: "Get the most visited pages",
      nodes: [
        {
          name: "aggregated",
          sql: "SELECT pathname, count() AS views FROM page_views GROUP BY pathname",
        },
      ],
      params: [
        { name: "limit", type: "Int32", default: 10, required: false },
      ],
      type: "endpoint",
      endpoint: { enabled: true },
      output_columns: [
        { name: "pathname", type: "String" },
        { name: "views", type: "UInt64" },
      ],
    };

    const code = generatePipeCode(pipe);

    expect(code).toContain('export const topPages = defineEndpoint("top_pages"');
    expect(code).toContain("limit: p.int32().optional(10)");
    expect(code).toContain('name: "aggregated"');
    expect(code).toContain("pathname: t.string()");
    expect(code).toContain("views: t.uint64()");
    expect(code).toContain("export type TopPagesParams = InferParams<typeof topPages>");
    expect(code).toContain("export type TopPagesOutput = InferOutputRow<typeof topPages>");
  });

  it("generates materialized view code", () => {
    const pipe: PipeInfo = {
      name: "daily_stats_mv",
      nodes: [
        { name: "aggregate", sql: "SELECT toDate(timestamp) AS date, count() AS cnt FROM events GROUP BY date" },
      ],
      params: [],
      type: "materialized",
      materialized: { datasource: "daily_stats" },
      output_columns: [],
    };

    const code = generatePipeCode(pipe);

    expect(code).toContain('export const dailyStatsMv = defineMaterializedView("daily_stats_mv"');
    expect(code).toContain("datasource: dailyStats");
  });

  it("generates copy pipe code", () => {
    const pipe: PipeInfo = {
      name: "daily_snapshot",
      nodes: [
        { name: "snapshot", sql: "SELECT * FROM events WHERE date = today()" },
      ],
      params: [],
      type: "copy",
      copy: {
        target_datasource: "snapshots",
        copy_schedule: "0 0 * * *",
        copy_mode: "append",
      },
      output_columns: [],
    };

    const code = generatePipeCode(pipe);

    expect(code).toContain('export const dailySnapshot = defineCopyPipe("daily_snapshot"');
    expect(code).toContain("datasource: snapshots");
    expect(code).toContain('copy_schedule: "0 0 * * *"');
    expect(code).toContain('copy_mode: "append"');
  });

  it("generates regular pipe code", () => {
    const pipe: PipeInfo = {
      name: "filtered_events",
      nodes: [
        { name: "filtered", sql: "SELECT * FROM events WHERE status = 'active'" },
      ],
      params: [],
      type: "pipe",
      output_columns: [],
    };

    const code = generatePipeCode(pipe);

    expect(code).toContain('export const filteredEvents = definePipe("filtered_events"');
  });

  it("handles params with descriptions", () => {
    const pipe: PipeInfo = {
      name: "search",
      nodes: [{ name: "search", sql: "SELECT * FROM events" }],
      params: [
        { name: "query", type: "String", required: true, description: "Search query" },
      ],
      type: "endpoint",
      endpoint: { enabled: true },
      output_columns: [],
    };

    const code = generatePipeCode(pipe);
    expect(code).toContain('query: p.string().describe("Search query")');
  });
});

describe("generateDatasourcesFile", () => {
  it("generates file with imports and all datasources", () => {
    const datasources: DatasourceInfo[] = [
      {
        name: "events",
        columns: [{ name: "id", type: "String" }],
        engine: { type: "MergeTree", sorting_key: "id" },
      },
      {
        name: "users",
        columns: [{ name: "user_id", type: "String" }],
        engine: { type: "MergeTree", sorting_key: "user_id" },
      },
    ];

    const file = generateDatasourcesFile(datasources);

    expect(file).toContain('import { defineDatasource, t, engine, type InferRow } from "@tinybirdco/sdk"');
    expect(file).toContain('export const events = defineDatasource("events"');
    expect(file).toContain('export const users = defineDatasource("users"');
  });

  it("generates placeholder for empty datasources", () => {
    const file = generateDatasourcesFile([]);

    expect(file).toContain("// No datasources found in workspace");
  });
});

describe("generatePipesFile", () => {
  it("generates file with appropriate imports", () => {
    const pipes: PipeInfo[] = [
      {
        name: "top_pages",
        nodes: [{ name: "agg", sql: "SELECT 1" }],
        params: [{ name: "limit", type: "Int32", required: false }],
        type: "endpoint",
        endpoint: { enabled: true },
        output_columns: [{ name: "count", type: "UInt64" }],
      },
    ];

    const file = generatePipesFile(pipes, []);

    expect(file).toContain("defineEndpoint");
    expect(file).toContain("node");
    expect(file).toContain("t");
    expect(file).toContain("p");
    expect(file).toContain("type InferParams");
    expect(file).toContain("type InferOutputRow");
  });

  it("imports datasources for materialized views", () => {
    const datasources: DatasourceInfo[] = [
      {
        name: "daily_stats",
        columns: [{ name: "date", type: "Date" }],
        engine: { type: "MergeTree", sorting_key: "date" },
      },
    ];

    const pipes: PipeInfo[] = [
      {
        name: "daily_stats_mv",
        nodes: [{ name: "agg", sql: "SELECT 1" }],
        params: [],
        type: "materialized",
        materialized: { datasource: "daily_stats" },
        output_columns: [],
      },
    ];

    const file = generatePipesFile(pipes, datasources);

    expect(file).toContain('import { dailyStats } from "./datasources.js"');
    expect(file).toContain("defineMaterializedView");
  });

  it("generates placeholder for empty pipes", () => {
    const file = generatePipesFile([], []);

    expect(file).toContain("// No pipes found in workspace");
  });
});

describe("generateClientFile", () => {
  it("generates client with datasources and pipes", () => {
    const datasources: DatasourceInfo[] = [
      {
        name: "events",
        columns: [{ name: "id", type: "String" }],
        engine: { type: "MergeTree", sorting_key: "id" },
      },
    ];

    const pipes: PipeInfo[] = [
      {
        name: "top_events",
        nodes: [{ name: "agg", sql: "SELECT 1" }],
        params: [],
        type: "endpoint",
        endpoint: { enabled: true },
        output_columns: [],
      },
    ];

    const file = generateClientFile(datasources, pipes);

    expect(file).toContain('import { createTinybirdClient } from "@tinybirdco/sdk"');
    expect(file).toContain('import { events, type EventsRow } from "./datasources.js"');
    expect(file).toContain('import { topEvents, type TopEventsParams, type TopEventsOutput } from "./pipes.js"');
    expect(file).toContain("datasources: { events }");
    expect(file).toContain("pipes: { topEvents }");
    expect(file).toContain("export type { EventsRow, TopEventsParams, TopEventsOutput }");
    expect(file).toContain("export { events, topEvents }");
  });

  it("includes configDir for monorepo support", () => {
    const file = generateClientFile([], []);

    // Should include Node imports for deriving configDir
    expect(file).toContain('import { fileURLToPath } from "url"');
    expect(file).toContain('import { dirname } from "path"');
    // Should derive __configDir from import.meta.url
    expect(file).toContain("const __configDir = dirname(fileURLToPath(import.meta.url))");
    // Should pass configDir to createTinybirdClient
    expect(file).toContain("configDir: __configDir");
  });

  it("handles empty datasources and pipes", () => {
    const file = generateClientFile([], []);

    expect(file).toContain("datasources: {}");
    expect(file).toContain("pipes: {}");
  });

  it("only includes endpoints in client pipes", () => {
    const pipes: PipeInfo[] = [
      {
        name: "endpoint_pipe",
        nodes: [{ name: "n", sql: "SELECT 1" }],
        params: [],
        type: "endpoint",
        endpoint: { enabled: true },
        output_columns: [],
      },
      {
        name: "materialized_pipe",
        nodes: [{ name: "n", sql: "SELECT 1" }],
        params: [],
        type: "materialized",
        materialized: { datasource: "target" },
        output_columns: [],
      },
    ];

    const file = generateClientFile([], pipes);

    expect(file).toContain("endpointPipe");
    expect(file).not.toContain("materializedPipe");
  });
});

describe("generateAllFiles", () => {
  it("returns all generated content with counts", () => {
    const datasources: DatasourceInfo[] = [
      {
        name: "events",
        columns: [{ name: "id", type: "String" }],
        engine: { type: "MergeTree", sorting_key: "id" },
      },
    ];

    const pipes: PipeInfo[] = [
      {
        name: "top_events",
        nodes: [{ name: "agg", sql: "SELECT 1" }],
        params: [],
        type: "endpoint",
        endpoint: { enabled: true },
        output_columns: [],
      },
    ];

    const result = generateAllFiles(datasources, pipes);

    expect(result.datasourceCount).toBe(1);
    expect(result.pipeCount).toBe(1);
    expect(result.datasourcesContent).toContain("defineDatasource");
    expect(result.pipesContent).toContain("defineEndpoint");
    expect(result.clientContent).toContain("createTinybirdClient");
  });
});
