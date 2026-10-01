import { beforeEach, describe, expect, it, vi } from "vitest";
import { TinybirdClient } from "./base.js";
import { TinybirdError } from "./types.js";

const mockFetch = vi.fn();
global.fetch = mockFetch;

function textResponse(body: string, init?: { ok?: boolean; status?: number }) {
  return {
    ok: init?.ok ?? true,
    status: init?.status ?? 200,
    text: () => Promise.resolve(body),
    json: () => Promise.resolve(body ? JSON.parse(body) : {}),
  };
}

describe("SecretsNamespace", () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  it("uses client custom fetch for secrets.list", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce(
      textResponse(
        JSON.stringify({ variables: [{ name: "API_KEY" }] })
      )
    );

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as unknown as typeof fetch,
    });

    const result = await client.secrets.list();

    expect(result).toEqual([{ name: "API_KEY" }]);
    expect(customFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).not.toHaveBeenCalled();

    const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/v0/variables");
    expect(parsed.searchParams.get("from")).toBe("ts-sdk");
    expect(init.method).toBe("GET");
    const headers = new Headers(init.headers);
    expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
  });

  it("creates a new secret via secrets.set when it does not exist", async () => {
    const customFetch = vi.fn();
    // existence check -> 404
    customFetch.mockResolvedValueOnce(textResponse("Not found", { ok: false, status: 404 }));
    // create
    customFetch.mockResolvedValueOnce(textResponse(""));

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as unknown as typeof fetch,
    });

    const result = await client.secrets.set("NEW_SECRET", "super-secret-value");

    expect(result).toEqual({ name: "NEW_SECRET", created: true });
    expect(customFetch).toHaveBeenCalledTimes(2);

    const [, createInit] = customFetch.mock.calls[1] as [string, RequestInit];
    expect(createInit.method).toBe("POST");
    const body = new URLSearchParams(createInit.body as string);
    expect(body.get("name")).toBe("NEW_SECRET");
    expect(body.get("value")).toBe("super-secret-value");
  });

  it("updates an existing secret via secrets.set", async () => {
    const customFetch = vi.fn();
    // existence check -> 200 (exists)
    customFetch.mockResolvedValueOnce(textResponse('{"name":"EXISTING"}'));
    // update
    customFetch.mockResolvedValueOnce(textResponse(""));

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as unknown as typeof fetch,
    });

    const result = await client.secrets.set("EXISTING", "rotated-value");

    expect(result).toEqual({ name: "EXISTING", created: false });

    const [updateUrl, updateInit] = customFetch.mock.calls[1] as [string, RequestInit];
    expect(new URL(updateUrl).pathname).toBe("/v0/variables/EXISTING");
    expect(updateInit.method).toBe("PUT");
    const body = new URLSearchParams(updateInit.body as string);
    expect(body.get("value")).toBe("rotated-value");
  });

  it("removes a secret via secrets.remove", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce(textResponse(""));

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as unknown as typeof fetch,
    });

    const result = await client.secrets.remove("API_KEY");

    expect(result).toEqual({ name: "API_KEY" });
    const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
    expect(new URL(url).pathname).toBe("/v0/variables/API_KEY");
    expect(init.method).toBe("DELETE");
  });

  it("wraps secret API failures in TinybirdError", async () => {
    const customFetch = vi
      .fn()
      .mockResolvedValueOnce(textResponse('{"error": "Forbidden"}', { ok: false, status: 403 }));

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as unknown as typeof fetch,
    });

    await expect(client.secrets.list()).rejects.toThrow(TinybirdError);

    customFetch.mockResolvedValueOnce(
      textResponse('{"error": "Forbidden"}', { ok: false, status: 403 })
    );

    try {
      await client.secrets.list();
      expect.unreachable();
    } catch (error) {
      expect((error as TinybirdError).statusCode).toBe(403);
      expect((error as TinybirdError).message).toBe("Forbidden");
    }
  });

  it("never leaks the secret value in a thrown error message", async () => {
    const customFetch = vi.fn();
    // existence check -> 404 (not found, so goes to create)
    customFetch.mockResolvedValueOnce(textResponse("Not found", { ok: false, status: 404 }));
    // create fails and echoes the value back
    customFetch.mockResolvedValueOnce(
      textResponse('{"error": "value top-secret-value is invalid"}', {
        ok: false,
        status: 400,
      })
    );

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as unknown as typeof fetch,
    });

    try {
      await client.secrets.set("BAD_SECRET", "top-secret-value");
      expect.unreachable();
    } catch (error) {
      expect((error as TinybirdError).message).not.toContain("top-secret-value");
    }
  });
});
