import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { createTinybirdApi } from "./api.js";
import { TINYBIRD_FROM_PARAM } from "./fetcher.js";
import { BASE_URL } from "../test/handlers.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("TinybirdApi", () => {
  it("sends authorization header and from=ts-sdk param", async () => {
    let authorizationHeader: string | null = null;
    let fromParam: string | null = null;

    server.use(
      http.get(`${BASE_URL}/v1/workspace`, ({ request }) => {
        authorizationHeader = request.headers.get("Authorization");
        const url = new URL(request.url);
        fromParam = url.searchParams.get("from");
        return HttpResponse.json({ ok: true });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    await api.request("/v1/workspace");

    expect(authorizationHeader).toBe("Bearer p.default-token");
    expect(fromParam).toBe(TINYBIRD_FROM_PARAM);
  });

  it("resolves relative paths and preserves query params", async () => {
    let fooParam: string | null = null;
    let fromParam: string | null = null;

    server.use(
      http.get(`${BASE_URL}/v1/build`, ({ request }) => {
        const url = new URL(request.url);
        fooParam = url.searchParams.get("foo");
        fromParam = url.searchParams.get("from");
        return HttpResponse.json({ ok: true });
      })
    );

    const api = createTinybirdApi({
      baseUrl: `${BASE_URL}/`,
      token: "p.default-token",
    });

    await api.request("v1/build?foo=bar");

    expect(fooParam).toBe("bar");
    expect(fromParam).toBe(TINYBIRD_FROM_PARAM);
  });

  it("allows per-request token override", async () => {
    let authorizationHeader: string | null = null;

    server.use(
      http.get(`${BASE_URL}/v1/workspace`, ({ request }) => {
        authorizationHeader = request.headers.get("Authorization");
        return HttpResponse.json({ ok: true });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    await api.request("/v1/workspace", { token: "p.override-token" });

    expect(authorizationHeader).toBe("Bearer p.override-token");
  });

  it("queries endpoint params via tinybirdApi.query", async () => {
    let fromParam: string | null = null;
    let startDateParam: string | null = null;
    let limitParam: string | null = null;
    let tagsParams: string[] = [];

    server.use(
      http.get(`${BASE_URL}/v0/pipes/top_pages.json`, ({ request }) => {
        const url = new URL(request.url);
        fromParam = url.searchParams.get("from");
        startDateParam = url.searchParams.get("start_date");
        limitParam = url.searchParams.get("limit");
        tagsParams = url.searchParams.getAll("tags");

        return HttpResponse.json({
          data: [{ pathname: "/", views: 1 }],
          meta: [
            { name: "pathname", type: "String" },
            { name: "views", type: "UInt64" },
          ],
          rows: 1,
          statistics: {
            elapsed: 0.001,
            rows_read: 1,
            bytes_read: 10,
          },
        });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    const result = await api.query<{ pathname: string; views: number }>("top_pages", {
      start_date: new Date("2024-01-01T00:00:00.000Z"),
      limit: 5,
      tags: ["a", "b"],
    });

    expect(result.rows).toBe(1);
    expect(result.data[0]).toEqual({ pathname: "/", views: 1 });
    expect(fromParam).toBe(TINYBIRD_FROM_PARAM);
    expect(startDateParam).toBe("2024-01-01T00:00:00.000Z");
    expect(limitParam).toBe("5");
    expect(tagsParams).toEqual(["a", "b"]);
  });

  it("ingests rows via tinybirdApi.ingest", async () => {
    let datasourceName: string | null = null;
    let waitParam: string | null = null;
    let fromParam: string | null = null;
    let contentType: string | null = null;
    let parsedBody: Record<string, unknown> | null = null;

    server.use(
      http.post(`${BASE_URL}/v0/events`, async ({ request }) => {
        const url = new URL(request.url);
        datasourceName = url.searchParams.get("name");
        waitParam = url.searchParams.get("wait");
        fromParam = url.searchParams.get("from");
        contentType = request.headers.get("content-type");

        const rawBody = await request.text();
        parsedBody = JSON.parse(rawBody);

        return HttpResponse.json({
          successful_rows: 1,
          quarantined_rows: 0,
        });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    const result = await api.ingest("events", {
      timestamp: new Date("2024-01-01T00:00:00.000Z"),
      count: 10n,
      payload: new Map([["k", "v"]]),
      nested: {
        when: new Date("2024-01-02T00:00:00.000Z"),
      },
    });

    expect(result).toEqual({ successful_rows: 1, quarantined_rows: 0 });
    expect(datasourceName).toBe("events");
    expect(waitParam).toBe("true");
    expect(fromParam).toBe(TINYBIRD_FROM_PARAM);
    expect(contentType).toBe("application/x-ndjson");
    expect(parsedBody).toEqual({
      timestamp: "2024-01-01T00:00:00.000Z",
      count: "10",
      payload: { k: "v" },
      nested: { when: "2024-01-02T00:00:00.000Z" },
    });
  });

  it("executes raw SQL via tinybirdApi.sql", async () => {
    let rawSql: string | null = null;
    let contentType: string | null = null;

    server.use(
      http.post(`${BASE_URL}/v0/sql`, async ({ request }) => {
        contentType = request.headers.get("content-type");
        rawSql = await request.text();

        return HttpResponse.json({
          data: [{ value: 1 }],
          meta: [{ name: "value", type: "UInt8" }],
          rows: 1,
          statistics: {
            elapsed: 0.001,
            rows_read: 1,
            bytes_read: 1,
          },
        });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    const result = await api.sql<{ value: number }>("SELECT 1 AS value");

    expect(result.data[0]?.value).toBe(1);
    expect(contentType).toBe("text/plain");
    expect(rawSql).toBe("SELECT 1 AS value");
  });

  it("returns zero counts for empty ingest batches", async () => {
    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    const result = await api.ingestBatch("events", []);

    expect(result).toEqual({ successful_rows: 0, quarantined_rows: 0 });
  });

  it("parses JSON responses", async () => {
    server.use(
      http.get(`${BASE_URL}/v1/workspace`, () => {
        return HttpResponse.json({ id: "ws_123", name: "main" });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    const result = await api.requestJson<{ id: string; name: string }>("/v1/workspace");

    expect(result.id).toBe("ws_123");
    expect(result.name).toBe("main");
  });

  it("throws TinybirdApiError for non-OK responses", async () => {
    server.use(
      http.get(`${BASE_URL}/v1/workspace`, () => {
        return new HttpResponse("Unauthorized", { status: 401 });
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    await expect(api.requestJson("/v1/workspace")).rejects.toMatchObject({
      name: "TinybirdApiError",
      statusCode: 401,
      responseBody: "Unauthorized",
    });
  });

  it("throws TinybirdApiError with parsed API details", async () => {
    server.use(
      http.get(`${BASE_URL}/v0/pipes/broken.json`, () => {
        return HttpResponse.json(
          {
            error: "Invalid query",
            status: 400,
            documentation: "https://www.tinybird.co/docs",
          },
          { status: 400 }
        );
      })
    );

    const api = createTinybirdApi({
      baseUrl: BASE_URL,
      token: "p.default-token",
    });

    await expect(api.query("broken")).rejects.toMatchObject({
      name: "TinybirdApiError",
      statusCode: 400,
      message: "Invalid query",
      response: {
        error: "Invalid query",
        status: 400,
      },
    });
  });
});
