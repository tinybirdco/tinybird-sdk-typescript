import { beforeEach, describe, expect, it, vi } from "vitest";
import { TinybirdClient } from "./base.js";
import { TinybirdError } from "./types.js";

const mockFetch = vi.fn();
global.fetch = mockFetch;

describe("TokensNamespace", () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  it("uses client custom fetch for createJWT", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ token: "jwt-token" }),
    });

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as typeof fetch,
    });

    const result = await client.tokens.createJWT({
      name: "user_token",
      expiresAt: 1700000000,
      scopes: [{ type: "PIPES:READ", resource: "analytics_pipe" }],
    });

    expect(result).toEqual({ token: "jwt-token" });
    expect(customFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).not.toHaveBeenCalled();

    const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/v0/tokens/");
    expect(parsed.searchParams.get("expiration_time")).toBe("1700000000");
    expect(parsed.searchParams.get("from")).toBe("ts-sdk");
    expect(init.method).toBe("POST");
    const headers = new Headers(init.headers);
    expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
    expect(headers.get("Content-Type")).toBe("application/json");

    const body = JSON.parse(String(init.body)) as {
      name: string;
      scopes: Array<{ type: string; resource: string }>;
      limits?: { rps?: number };
    };
    expect(body).toEqual({
      name: "user_token",
      scopes: [{ type: "PIPES:READ", resource: "analytics_pipe" }],
    });
  });

  it("sends full JWT payload when limits and scope fields are provided", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ token: "jwt-token" }),
    });

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as typeof fetch,
    });

    await client.tokens.createJWT({
      name: "tenant_token",
      expiresAt: 1700000000,
      scopes: [
        {
          type: "PIPES:READ",
          resource: "analytics_pipe",
          fixed_params: { tenant_id: 123 },
        },
      ],
      limits: { rps: 10 },
    });

    const [, init] = customFetch.mock.calls[0] as [string, RequestInit];
    const body = JSON.parse(String(init.body)) as {
      name: string;
      scopes: Array<{
        type: string;
        resource: string;
        fixed_params?: Record<string, unknown>;
      }>;
      limits?: { rps?: number };
    };

    expect(body).toEqual({
      name: "tenant_token",
      scopes: [
        {
          type: "PIPES:READ",
          resource: "analytics_pipe",
          fixed_params: { tenant_id: 123 },
        },
      ],
      limits: { rps: 10 },
    });
  });

  describe("list", () => {
    it("lists tokens using the client's custom fetch", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            tokens: [{ id: "t1", name: "admin_token", token: "p.admin" }],
          }),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      const tokens = await client.tokens.list();

      expect(tokens).toEqual([
        {
          id: "t1",
          name: "admin_token",
          description: undefined,
          scopes: undefined,
          value: "p.admin",
        },
      ]);

      const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/tokens");
      expect(init.method).toBe("GET");
    });

    it("throws TinybirdError on API failure", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: () => Promise.resolve("Insufficient permissions"),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      await expect(client.tokens.list()).rejects.toThrow(TinybirdError);
    });
  });

  describe("get", () => {
    it("gets a token's details", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            id: "t1",
            name: "user_token",
            token: "p.value",
            scopes: [{ type: "PIPES:READ", resource: "analytics_pipe" }],
          }),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      const result = await client.tokens.get("user_token");

      expect(result.value).toBe("p.value");
      expect(result.scopes).toEqual([
        { type: "PIPES:READ", resource: "analytics_pipe", filter: undefined },
      ]);

      const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token");
      expect(init.method).toBe("GET");
    });
  });

  describe("getValue", () => {
    it("returns the raw token value", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({ name: "user_token", token: "p.raw-value" }),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      const value = await client.tokens.getValue("user_token");
      expect(value).toBe("p.raw-value");
    });

    it("throws TinybirdError when the token has no retrievable value", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ name: "admin_token" }),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      await expect(client.tokens.getValue("admin_token")).rejects.toThrow(
        TinybirdError
      );
    });
  });

  describe("revoke", () => {
    it("deletes a token", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        text: () => Promise.resolve(""),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      await client.tokens.revoke("user_token");

      const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token");
      expect(init.method).toBe("DELETE");
    });
  });

  describe("refresh", () => {
    it("refreshes a token", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        text: () =>
          Promise.resolve(JSON.stringify({ name: "user_token", token: "p.new-value" })),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      const result = await client.tokens.refresh("user_token");
      expect(result.value).toBe("p.new-value");

      const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token/refresh");
      expect(init.method).toBe("POST");
    });
  });

  describe("updateScopes", () => {
    it("updates a token's scopes", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        text: () =>
          Promise.resolve(
            JSON.stringify({
              name: "user_token",
              scopes: [{ type: "DATASOURCES:READ", resource: "events" }],
            })
          ),
      });

      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "p.admin-token",
        fetch: customFetch as typeof fetch,
      });

      const result = await client.tokens.updateScopes("user_token", [
        { type: "DATASOURCES:READ", resource: "events" },
      ]);

      expect(result.scopes).toEqual([
        { type: "DATASOURCES:READ", resource: "events", filter: undefined },
      ]);

      const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token");
      expect(init.method).toBe("PUT");
      const body = JSON.parse(String(init.body)) as { scopes: unknown[] };
      expect(body.scopes).toEqual([{ type: "DATASOURCES:READ", resource: "events" }]);
    });
  });
});
