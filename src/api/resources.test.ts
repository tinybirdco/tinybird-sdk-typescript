import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import {
  listDatasources,
  getDatasource,
  listPipes,
  getPipe,
  fetchAllResources,
  hasResources,
  ResourceApiError,
} from "./resources.js";

const BASE_URL = "https://api.tinybird.co";
const TOKEN = "test-token";

const handlers = [
  // List datasources
  http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
    return HttpResponse.json({
      datasources: [
        { name: "events", description: "Event data" },
        { name: "users", description: "User data" },
      ],
    });
  }),

  // Get datasource detail - events
  http.get(`${BASE_URL}/v0/datasources/events?from=ts-sdk`, () => {
    return HttpResponse.json({
      name: "events",
      description: "Event tracking data",
      columns: [
        { name: "timestamp", type: "DateTime" },
        { name: "event_name", type: "LowCardinality(String)" },
        { name: "user_id", type: "Nullable(String)" },
      ],
      engine: "MergeTree",
      sorting_key: "event_name, timestamp",
      partition_key: "toYYYYMM(timestamp)",
    });
  }),

  // Get datasource detail - users
  http.get(`${BASE_URL}/v0/datasources/users?from=ts-sdk`, () => {
    return HttpResponse.json({
      name: "users",
      description: "User data",
      columns: [
        { name: "user_id", type: "String" },
        { name: "email", type: "String" },
        { name: "created_at", type: "DateTime" },
      ],
      engine: "MergeTree",
      sorting_key: "user_id",
    });
  }),

  // List pipes
  http.get(`${BASE_URL}/v0/pipes?from=ts-sdk`, () => {
    return HttpResponse.json({
      pipes: [
        { name: "top_events", type: "endpoint" },
        { name: "daily_stats_mv", type: "materialized" },
      ],
    });
  }),

  // Get pipe detail - endpoint
  http.get(`${BASE_URL}/v0/pipes/top_events?from=ts-sdk`, () => {
    return HttpResponse.json({
      name: "top_events",
      description: "Get top events by count",
      endpoint: "/v0/pipes/top_events.json",
      nodes: [
        {
          name: "aggregated",
          sql: "SELECT event_name, count() AS cnt FROM events GROUP BY event_name ORDER BY cnt DESC LIMIT {{Int32(limit, 10)}}",
          params: [
            { name: "limit", type: "Int32", default: 10, required: false },
          ],
          columns: [
            { name: "event_name", type: "String" },
            { name: "cnt", type: "UInt64" },
          ],
        },
      ],
    });
  }),

  // Get pipe detail - materialized
  http.get(`${BASE_URL}/v0/pipes/daily_stats_mv?from=ts-sdk`, () => {
    return HttpResponse.json({
      name: "daily_stats_mv",
      description: "Daily aggregation",
      materialized_datasource: "daily_stats",
      nodes: [
        {
          name: "aggregate",
          sql: "SELECT toDate(timestamp) AS date, count() AS cnt FROM events GROUP BY date",
        },
      ],
    });
  }),

  // Get pipe detail - copy
  http.get(`${BASE_URL}/v0/pipes/daily_snapshot?from=ts-sdk`, () => {
    return HttpResponse.json({
      name: "daily_snapshot",
      description: "Daily snapshot copy",
      copy_target_datasource: "snapshots",
      copy_schedule: "0 0 * * *",
      copy_mode: "append",
      nodes: [
        {
          name: "snapshot",
          sql: "SELECT * FROM events WHERE date = today()",
        },
      ],
    });
  }),
];

const server = setupServer(...handlers);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterAll(() => server.close());
afterEach(() => server.resetHandlers());

describe("listDatasources", () => {
  it("returns array of datasource names", async () => {
    const result = await listDatasources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toEqual(["events", "users"]);
  });

  it("handles empty workspace", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
        return HttpResponse.json({ datasources: [] });
      })
    );

    const result = await listDatasources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toEqual([]);
  });

  it("throws ResourceApiError on 401", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
        return new HttpResponse(null, { status: 401 });
      })
    );

    await expect(
      listDatasources({ baseUrl: BASE_URL, token: TOKEN })
    ).rejects.toThrow(ResourceApiError);
  });

  it("throws ResourceApiError on 403", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
        return new HttpResponse(null, { status: 403 });
      })
    );

    await expect(
      listDatasources({ baseUrl: BASE_URL, token: TOKEN })
    ).rejects.toThrow("Insufficient permissions");
  });
});

describe("getDatasource", () => {
  it("returns datasource info with columns and engine", async () => {
    const result = await getDatasource({ baseUrl: BASE_URL, token: TOKEN }, "events");

    expect(result.name).toBe("events");
    expect(result.description).toBe("Event tracking data");
    expect(result.columns).toHaveLength(3);
    expect(result.columns[0]).toEqual({ name: "timestamp", type: "DateTime" });
    expect(result.columns[1]).toEqual({ name: "event_name", type: "LowCardinality(String)" });
    expect(result.columns[2]).toEqual({ name: "user_id", type: "Nullable(String)" });
    expect(result.engine.type).toBe("MergeTree");
    expect(result.engine.sorting_key).toBe("event_name, timestamp");
    expect(result.engine.partition_key).toBe("toYYYYMM(timestamp)");
  });

  it("throws ResourceApiError on 404", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources/nonexistent?from=ts-sdk`, () => {
        return new HttpResponse(null, { status: 404 });
      })
    );

    await expect(
      getDatasource({ baseUrl: BASE_URL, token: TOKEN }, "nonexistent")
    ).rejects.toThrow("Resource not found");
  });
});

describe("listPipes", () => {
  it("returns array of pipe names", async () => {
    const result = await listPipes({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toEqual(["top_events", "daily_stats_mv"]);
  });
});

describe("getPipe", () => {
  it("returns endpoint pipe info", async () => {
    const result = await getPipe({ baseUrl: BASE_URL, token: TOKEN }, "top_events");

    expect(result.name).toBe("top_events");
    expect(result.description).toBe("Get top events by count");
    expect(result.type).toBe("endpoint");
    expect(result.nodes).toHaveLength(1);
    expect(result.nodes[0].name).toBe("aggregated");
    expect(result.params).toHaveLength(1);
    expect(result.params[0]).toEqual({
      name: "limit",
      type: "Int32",
      default: 10,
      required: false,
    });
    expect(result.output_columns).toHaveLength(2);
  });

  it("returns materialized pipe info", async () => {
    const result = await getPipe({ baseUrl: BASE_URL, token: TOKEN }, "daily_stats_mv");

    expect(result.name).toBe("daily_stats_mv");
    expect(result.type).toBe("materialized");
    expect(result.materialized?.datasource).toBe("daily_stats");
  });

  it("returns copy pipe info", async () => {
    const result = await getPipe({ baseUrl: BASE_URL, token: TOKEN }, "daily_snapshot");

    expect(result.name).toBe("daily_snapshot");
    expect(result.type).toBe("copy");
    expect(result.copy?.target_datasource).toBe("snapshots");
    expect(result.copy?.copy_schedule).toBe("0 0 * * *");
    expect(result.copy?.copy_mode).toBe("append");
  });
});

describe("fetchAllResources", () => {
  it("fetches all datasources and pipes with details", async () => {
    const result = await fetchAllResources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result.datasources).toHaveLength(2);
    expect(result.pipes).toHaveLength(2);

    // Verify datasource details were fetched
    const events = result.datasources.find((ds) => ds.name === "events");
    expect(events?.columns).toBeDefined();
    expect(events?.engine).toBeDefined();

    // Verify pipe details were fetched
    const topEvents = result.pipes.find((p) => p.name === "top_events");
    expect(topEvents?.nodes).toBeDefined();
    expect(topEvents?.type).toBe("endpoint");
  });
});

describe("hasResources", () => {
  it("returns true when workspace has resources", async () => {
    const result = await hasResources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toBe(true);
  });

  it("returns false for empty workspace", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
        return HttpResponse.json({ datasources: [] });
      }),
      http.get(`${BASE_URL}/v0/pipes?from=ts-sdk`, () => {
        return HttpResponse.json({ pipes: [] });
      })
    );

    const result = await hasResources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toBe(false);
  });

  it("returns true when only datasources exist", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/pipes?from=ts-sdk`, () => {
        return HttpResponse.json({ pipes: [] });
      })
    );

    const result = await hasResources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toBe(true);
  });

  it("returns true when only pipes exist", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
        return HttpResponse.json({ datasources: [] });
      })
    );

    const result = await hasResources({ baseUrl: BASE_URL, token: TOKEN });

    expect(result).toBe(true);
  });
});

describe("ResourceApiError", () => {
  it("includes status and endpoint", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/datasources?from=ts-sdk`, () => {
        return new HttpResponse("Unauthorized", { status: 401 });
      })
    );

    try {
      await listDatasources({ baseUrl: BASE_URL, token: TOKEN });
      expect.fail("Should have thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(ResourceApiError);
      const apiError = error as ResourceApiError;
      expect(apiError.status).toBe(401);
      expect(apiError.endpoint).toBe("/v0/datasources");
    }
  });
});
