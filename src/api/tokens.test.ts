import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  createJWT,
  TokenApiError,
  type TokenApiConfig,
} from "./tokens.js";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

function expectFromParam(url: string) {
  const parsed = new URL(url);
  expect(parsed.searchParams.get("from")).toBe("ts-sdk");
  return parsed;
}

describe("Token API client", () => {
  const config: TokenApiConfig = {
    baseUrl: "https://api.tinybird.co",
    token: "p.admin-token",
  };

  beforeEach(() => {
    mockFetch.mockReset();
  });

  describe("createJWT", () => {
    it("creates a JWT token with scopes", async () => {
      const mockResponse = { token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test" };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockResponse),
      });

      const result = await createJWT(config, {
        name: "user_token",
        expiresAt: 1700000000,
        scopes: [
          {
            type: "PIPES:READ",
            resource: "analytics_pipe",
            fixed_params: { user_id: 123 },
          },
        ],
      });

      expect(result.token).toBe(mockResponse.token);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/tokens/");
      expect(parsed.searchParams.get("expiration_time")).toBe("1700000000");
      expect(init.method).toBe("POST");
      const headers = new Headers(init.headers);
      expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
      expect(headers.get("Content-Type")).toBe("application/json");

      const body = JSON.parse(init.body);
      expect(body.name).toBe("user_token");
      expect(body.scopes).toHaveLength(1);
      expect(body.scopes[0].type).toBe("PIPES:READ");
      expect(body.scopes[0].resource).toBe("analytics_pipe");
      expect(body.scopes[0].fixed_params).toEqual({ user_id: 123 });
    });

    it("converts Date to Unix timestamp", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ token: "jwt-token" }),
      });

      const expirationDate = new Date("2024-01-01T00:00:00Z");
      await createJWT(config, {
        name: "test",
        expiresAt: expirationDate,
        scopes: [{ type: "PIPES:READ", resource: "pipe" }],
      });

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.searchParams.get("expiration_time")).toBe("1704067200");
    });

    it("converts ISO string to Unix timestamp", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ token: "jwt-token" }),
      });

      await createJWT(config, {
        name: "test",
        expiresAt: "2024-01-01T00:00:00Z",
        scopes: [{ type: "PIPES:READ", resource: "pipe" }],
      });

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.searchParams.get("expiration_time")).toBe("1704067200");
    });

    it("includes limits when provided", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ token: "jwt-token" }),
      });

      await createJWT(config, {
        name: "rate_limited_token",
        expiresAt: 1700000000,
        scopes: [{ type: "PIPES:READ", resource: "pipe" }],
        limits: { rps: 100 },
      });

      const [, init] = mockFetch.mock.calls[0];
      const body = JSON.parse(init.body);
      expect(body.limits).toEqual({ rps: 100 });
    });

    it("uses custom fetch implementation when provided", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ token: "jwt-token" }),
      });

      await createJWT(
        {
          ...config,
          fetch: customFetch as typeof fetch,
        },
        {
          name: "custom_fetch_token",
          expiresAt: 1700000000,
          scopes: [{ type: "PIPES:READ", resource: "pipe" }],
        }
      );

      expect(customFetch).toHaveBeenCalledTimes(1);
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it("supports datasource scope with filter", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ token: "jwt-token" }),
      });

      await createJWT(config, {
        name: "filtered_token",
        expiresAt: 1700000000,
        scopes: [
          {
            type: "DATASOURCES:READ",
            resource: "events",
            filter: "org_id = 'acme'",
          },
        ],
      });

      const [, init] = mockFetch.mock.calls[0];
      const body = JSON.parse(init.body);
      expect(body.scopes[0].filter).toBe("org_id = 'acme'");
    });

    it("throws TokenApiError on 403 with helpful message", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: () => Promise.resolve("Insufficient permissions"),
      });

      await expect(
        createJWT(config, {
          name: "test",
          expiresAt: 1700000000,
          scopes: [],
        })
      ).rejects.toThrow(TokenApiError);

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: () => Promise.resolve("Insufficient permissions"),
      });

      try {
        await createJWT(config, {
          name: "test",
          expiresAt: 1700000000,
          scopes: [],
        });
      } catch (error) {
        expect((error as TokenApiError).status).toBe(403);
        expect((error as TokenApiError).message).toContain("TOKENS or ADMIN scope");
      }
    });

    it("throws TokenApiError on 400 with validation error", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: "Bad Request",
        text: () => Promise.resolve('{"error": "Invalid scope type"}'),
      });

      await expect(
        createJWT(config, {
          name: "test",
          expiresAt: 1700000000,
          scopes: [],
        })
      ).rejects.toThrow(TokenApiError);

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: "Bad Request",
        text: () => Promise.resolve('{"error": "Invalid scope type"}'),
      });

      try {
        await createJWT(config, {
          name: "test",
          expiresAt: 1700000000,
          scopes: [],
        });
      } catch (error) {
        expect((error as TokenApiError).status).toBe(400);
        expect((error as TokenApiError).message).toContain("Invalid scope type");
      }
    });

    it("does not include limits if not provided", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ token: "jwt-token" }),
      });

      await createJWT(config, {
        name: "simple_token",
        expiresAt: 1700000000,
        scopes: [{ type: "PIPES:READ", resource: "pipe" }],
      });

      const [, init] = mockFetch.mock.calls[0];
      const body = JSON.parse(init.body);
      expect(body.limits).toBeUndefined();
    });
  });
});
