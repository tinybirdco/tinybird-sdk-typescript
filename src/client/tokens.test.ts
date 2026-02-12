import { beforeEach, describe, expect, it, vi } from "vitest";
import { TinybirdClient } from "./base.js";

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
});
