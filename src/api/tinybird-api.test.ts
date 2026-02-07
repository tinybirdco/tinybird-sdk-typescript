import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import { createTinybirdApi } from "./tinybird-api.js";
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
});
