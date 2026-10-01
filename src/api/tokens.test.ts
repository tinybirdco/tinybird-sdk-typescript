import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  createJWT,
  deleteToken,
  getToken,
  getTokenValue,
  listTokens,
  refreshToken,
  TokenApiError,
  updateTokenScopes,
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

  describe("listTokens", () => {
    it("lists tokens in the workspace", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            tokens: [
              { id: "t1", name: "admin_token", token: "p.admin" },
              { id: "t2", name: "read_token", scopes: [{ type: "PIPES:READ" }] },
            ],
          }),
      });

      const tokens = await listTokens(config);

      expect(tokens).toHaveLength(2);
      expect(tokens[0]).toEqual({
        id: "t1",
        name: "admin_token",
        description: undefined,
        scopes: undefined,
        value: "p.admin",
      });
      expect(tokens[1].scopes).toEqual([
        { type: "PIPES:READ", resource: undefined, filter: undefined },
      ]);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/tokens");
      expect(init.method).toBe("GET");
      const headers = new Headers(init.headers);
      expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
    });

    it("returns an empty array when the API returns no tokens field", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({}),
      });

      const tokens = await listTokens(config);
      expect(tokens).toEqual([]);
    });

    it("throws TokenApiError on 403", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: () => Promise.resolve("Insufficient permissions"),
      });

      await expect(listTokens(config)).rejects.toThrow(TokenApiError);
    });
  });

  describe("getToken", () => {
    it("gets a token's details including scopes", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            id: "t1",
            name: "user_token",
            token: "p.user-token-value",
            scopes: [{ type: "DATASOURCES:READ", resource: "events", filter: "org_id = 1" }],
          }),
      });

      const result = await getToken(config, "user_token");

      expect(result.name).toBe("user_token");
      expect(result.value).toBe("p.user-token-value");
      expect(result.scopes).toEqual([
        { type: "DATASOURCES:READ", resource: "events", filter: "org_id = 1" },
      ]);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token");
      expect(init.method).toBe("GET");
    });

    it("URL-encodes the name/id", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ name: "weird token" }),
      });

      await getToken(config, "weird token");

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/tokens/weird%20token");
    });

    it("throws TokenApiError on 404 with helpful message", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve("Token not found"),
      });

      await expect(getToken(config, "missing_token")).rejects.toThrow(TokenApiError);
    });
  });

  describe("getTokenValue", () => {
    it("returns the raw token value", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({ name: "user_token", token: "p.user-token-value" }),
      });

      const value = await getTokenValue(config, "user_token");
      expect(value).toBe("p.user-token-value");
    });

    it("throws TokenApiError when the token has no retrievable value", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ name: "admin_token" }),
      });

      await expect(getTokenValue(config, "admin_token")).rejects.toThrow(TokenApiError);
    });
  });

  describe("deleteToken", () => {
    it("deletes a token", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: () => Promise.resolve(""),
      });

      await deleteToken(config, "user_token");

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token");
      expect(init.method).toBe("DELETE");
    });

    it("throws TokenApiError on 403 with helpful message", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: () => Promise.resolve("Insufficient permissions"),
      });

      await expect(deleteToken(config, "user_token")).rejects.toThrow(TokenApiError);
    });
  });

  describe("refreshToken", () => {
    it("refreshes a token and returns its new value", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: () =>
          Promise.resolve(
            JSON.stringify({ name: "user_token", token: "p.new-value" })
          ),
      });

      const result = await refreshToken(config, "user_token");
      expect(result.value).toBe("p.new-value");

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token/refresh");
      expect(init.method).toBe("POST");
    });

    it("handles an empty response body", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: () => Promise.resolve(""),
      });

      const result = await refreshToken(config, "user_token");
      expect(result).toEqual({
        id: undefined,
        name: undefined,
        description: undefined,
        scopes: undefined,
        value: undefined,
      });
    });

    it("throws TokenApiError on 404", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve("Token not found"),
      });

      await expect(refreshToken(config, "missing_token")).rejects.toThrow(
        TokenApiError
      );
    });
  });

  describe("updateTokenScopes", () => {
    it("updates a token's scopes", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: () =>
          Promise.resolve(
            JSON.stringify({
              name: "user_token",
              scopes: [{ type: "DATASOURCES:READ", resource: "events" }],
            })
          ),
      });

      const result = await updateTokenScopes(config, "user_token", [
        { type: "DATASOURCES:READ", resource: "events" },
      ]);

      expect(result.scopes).toEqual([
        { type: "DATASOURCES:READ", resource: "events", filter: undefined },
      ]);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/tokens/user_token");
      expect(init.method).toBe("PUT");
      const headers = new Headers(init.headers);
      expect(headers.get("Content-Type")).toBe("application/json");
      const body = JSON.parse(init.body);
      expect(body.scopes).toEqual([{ type: "DATASOURCES:READ", resource: "events" }]);
    });

    it("throws TokenApiError on 400 with validation error", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: "Bad Request",
        text: () => Promise.resolve('{"error": "Invalid scope type"}'),
      });

      await expect(
        updateTokenScopes(config, "user_token", [{ type: "INVALID" }])
      ).rejects.toThrow(TokenApiError);
    });
  });
});
