import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  listJobs,
  getJob,
  cancelJob,
  JobApiError,
  type JobApiConfig,
  type Job,
} from "./jobs.js";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

function expectFromParam(url: string) {
  const parsed = new URL(url);
  expect(parsed.searchParams.get("from")).toBe("ts-sdk");
  return parsed;
}

describe("Job API client", () => {
  const config: JobApiConfig = {
    baseUrl: "https://api.tinybird.co",
    token: "p.admin-token",
  };

  const sampleJob: Job = {
    id: "job-123",
    kind: "import",
    status: "done",
    created_at: "2024-01-01T00:00:00.000Z",
    updated_at: "2024-01-01T00:01:00.000Z",
    job_url: "https://api.tinybird.co/v0/jobs/job-123",
  };

  beforeEach(() => {
    mockFetch.mockReset();
  });

  describe("listJobs", () => {
    it("lists jobs with no filters", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ jobs: [sampleJob] }),
      });

      const result = await listJobs(config);

      expect(result).toEqual([sampleJob]);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/jobs");
      expect(parsed.searchParams.get("status")).toBeNull();
      expect(parsed.searchParams.get("kind")).toBeNull();
      expect(init.method).toBe("GET");
      const headers = new Headers(init.headers);
      expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
    });

    it("filters by status", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ jobs: [] }),
      });

      await listJobs(config, { status: "error" });

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.searchParams.get("status")).toBe("error");
    });

    it("filters by kind", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ jobs: [] }),
      });

      await listJobs(config, { kind: "populateview" });

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.searchParams.get("kind")).toBe("populateview");
    });

    it("filters by status and kind together", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ jobs: [] }),
      });

      await listJobs(config, { status: "working", kind: "copy" });

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.searchParams.get("status")).toBe("working");
      expect(parsed.searchParams.get("kind")).toBe("copy");
    });

    it("returns an empty array when the API omits the jobs field", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({}),
      });

      const result = await listJobs(config);
      expect(result).toEqual([]);
    });

    it("uses custom fetch implementation when provided", async () => {
      const customFetch = vi.fn().mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ jobs: [] }),
      });

      await listJobs({ ...config, fetch: customFetch as typeof fetch });

      expect(customFetch).toHaveBeenCalledTimes(1);
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it("throws JobApiError on failure", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: "Internal Server Error",
        text: () => Promise.resolve("boom"),
      });

      await expect(listJobs(config)).rejects.toThrow(JobApiError);
    });
  });

  describe("getJob", () => {
    it("gets a job by id", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(sampleJob),
      });

      const result = await getJob(config, "job-123");
      expect(result).toEqual(sampleJob);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/jobs/job-123");
      expect(init.method).toBe("GET");
      const headers = new Headers(init.headers);
      expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
    });

    it("encodes the job id in the path", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(sampleJob),
      });

      await getJob(config, "job/with/slash");

      const [url] = mockFetch.mock.calls[0];
      const parsed = new URL(url);
      expect(parsed.pathname).toBe("/v0/jobs/job%2Fwith%2Fslash");
    });

    it("throws a helpful JobApiError on 404", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve(""),
      });

      await expect(getJob(config, "missing-job")).rejects.toThrow(JobApiError);

      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve(""),
      });

      try {
        await getJob(config, "missing-job");
      } catch (error) {
        expect((error as JobApiError).status).toBe(404);
        expect((error as JobApiError).message).toContain("missing-job");
        expect((error as JobApiError).message).toContain("not found");
      }
    });
  });

  describe("cancelJob", () => {
    it("cancels a job", async () => {
      const cancellingJob: Job = { ...sampleJob, status: "cancelling" };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(cancellingJob),
      });

      const result = await cancelJob(config, "job-123");
      expect(result).toEqual(cancellingJob);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/jobs/job-123/cancel");
      expect(init.method).toBe("POST");
      const headers = new Headers(init.headers);
      expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
    });

    it("throws a helpful JobApiError on 404", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve(""),
      });

      await expect(cancelJob(config, "missing-job")).rejects.toThrow(JobApiError);
    });

    it("throws JobApiError with response body on other failures", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 409,
        statusText: "Conflict",
        text: () => Promise.resolve('{"error": "Job already finished"}'),
      });

      try {
        await cancelJob(config, "job-123");
        expect.unreachable();
      } catch (error) {
        expect((error as JobApiError).status).toBe(409);
        expect((error as JobApiError).message).toContain("Job already finished");
      }
    });
  });
});
