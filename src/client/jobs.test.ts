import { beforeEach, describe, expect, it, vi } from "vitest";
import { TinybirdClient } from "./base.js";
import { TinybirdError } from "./types.js";

const mockFetch = vi.fn();
global.fetch = mockFetch;

describe("JobsNamespace", () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  it("uses client custom fetch for list", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          jobs: [
            {
              id: "job-1",
              kind: "import",
              status: "done",
              created_at: "2024-01-01T00:00:00.000Z",
              updated_at: "2024-01-01T00:01:00.000Z",
            },
          ],
        }),
    });

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as typeof fetch,
    });

    const result = await client.jobs.list({ status: "done" });

    expect(result).toHaveLength(1);
    expect(result[0].id).toBe("job-1");
    expect(customFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch).not.toHaveBeenCalled();

    const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/v0/jobs");
    expect(parsed.searchParams.get("status")).toBe("done");
    expect(parsed.searchParams.get("from")).toBe("ts-sdk");
    expect(init.method).toBe("GET");
    const headers = new Headers(init.headers);
    expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
  });

  it("gets a single job", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          id: "job-123",
          kind: "populateview",
          status: "working",
          created_at: "2024-01-01T00:00:00.000Z",
          updated_at: "2024-01-01T00:01:00.000Z",
        }),
    });

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as typeof fetch,
    });

    const job = await client.jobs.get("job-123");

    expect(job.status).toBe("working");

    const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/v0/jobs/job-123");
    expect(init.method).toBe("GET");
  });

  it("cancels a job", async () => {
    const customFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: () =>
        Promise.resolve({
          id: "job-123",
          kind: "import",
          status: "cancelling",
          created_at: "2024-01-01T00:00:00.000Z",
          updated_at: "2024-01-01T00:01:00.000Z",
        }),
    });

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as typeof fetch,
    });

    const job = await client.jobs.cancel("job-123");

    expect(job.status).toBe("cancelling");

    const [url, init] = customFetch.mock.calls[0] as [string, RequestInit];
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/v0/jobs/job-123/cancel");
    expect(init.method).toBe("POST");
  });

  it("wraps job API errors as TinybirdError", async () => {
    const errorResponse = {
      ok: false,
      status: 404,
      statusText: "Not Found",
      text: () => Promise.resolve(""),
    };
    const customFetch = vi
      .fn()
      .mockResolvedValueOnce(errorResponse)
      .mockResolvedValueOnce(errorResponse);

    const client = new TinybirdClient({
      baseUrl: "https://api.tinybird.co",
      token: "p.admin-token",
      fetch: customFetch as typeof fetch,
    });

    await expect(client.jobs.get("missing-job")).rejects.toThrow(TinybirdError);

    try {
      await client.jobs.get("missing-job");
    } catch (error) {
      expect((error as TinybirdError).statusCode).toBe(404);
    }
  });
});
